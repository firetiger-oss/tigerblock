package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestLRUBasicOperations(t *testing.T) {
	lru := &LRU[string, string]{Limit: 100}

	// Test Load
	value, err := lru.Load("key1", func() (int64, string, error) {
		return 10, "value1", nil
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected 'value1', got %q", value)
	}

	// Test Peek
	value, found := lru.Peek("key1")
	if !found {
		t.Error("Expected to find key1")
	}
	if value != "value1" {
		t.Errorf("Expected 'value1', got %q", value)
	}

	// Test Peek for non-existent key
	_, found = lru.Peek("nonexistent")
	if found {
		t.Error("Expected not to find nonexistent key")
	}
}

func TestLRULoadError(t *testing.T) {
	lru := &LRU[string, string]{Limit: 100}
	expectedErr := errors.New("test error")

	value, err := lru.Load("key1", func() (int64, string, error) {
		return 0, "", expectedErr
	})
	if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
	if value != "" {
		t.Errorf("Expected empty value on error, got %q", value)
	}

	// Verify key is not cached after error
	_, found := lru.Peek("key1")
	if found {
		t.Error("Key should not be cached after error")
	}
}

func TestLRUSizeLimit(t *testing.T) {
	lru := &LRU[string, string]{Limit: 50}

	// Add items that exceed half the limit - they should be cached
	_, err := lru.Load("small1", func() (int64, string, error) {
		return 20, "value1", nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Add item that's larger than half the limit - should not be cached
	_, err = lru.Load("large", func() (int64, string, error) {
		return 30, "large_value", nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check that small item is still there
	_, found := lru.Peek("small1")
	if !found {
		t.Error("Small item should still be cached")
	}

	// Check that large item is not cached
	_, found = lru.Peek("large")
	if found {
		t.Error("Large item should not be cached (exceeds limit/2)")
	}
}

func TestLRUEviction(t *testing.T) {
	lru := &LRU[string, string]{Limit: 100}

	// Fill cache to capacity
	for i := range 10 {
		key := fmt.Sprintf("key%d", i)
		_, err := lru.Load(key, func() (int64, string, error) {
			return 10, fmt.Sprintf("value%d", i), nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Access key0 to make it most recently used
	lru.Peek("key0")

	// Add one more item to trigger eviction
	_, err := lru.Load("key10", func() (int64, string, error) {
		return 10, "value10", nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// key0 should still be there (was accessed recently)
	_, found := lru.Peek("key0")
	if !found {
		t.Error("key0 should still be cached (recently accessed)")
	}

	// key1 should be evicted (least recently used)
	_, found = lru.Peek("key1")
	if found {
		t.Error("key1 should have been evicted")
	}

	// key10 should be cached
	_, found = lru.Peek("key10")
	if !found {
		t.Error("key10 should be cached")
	}
}

func TestLRUDrop(t *testing.T) {
	lru := &LRU[string, string]{Limit: 100}

	// Add some items
	keys := []string{"key1", "key2", "key3"}
	for _, key := range keys {
		_, err := lru.Load(key, func() (int64, string, error) {
			return 10, "value", nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	// Drop multiple keys
	lru.Drop("key1", "key3")

	// Check that dropped keys are gone
	_, found := lru.Peek("key1")
	if found {
		t.Error("key1 should have been dropped")
	}
	_, found = lru.Peek("key3")
	if found {
		t.Error("key3 should have been dropped")
	}

	// Check that key2 is still there
	_, found = lru.Peek("key2")
	if !found {
		t.Error("key2 should still be cached")
	}
}

func TestLRUConcurrentAccess(t *testing.T) {
	lru := &LRU[int, string]{Limit: 10000} // Increased limit to prevent evictions
	const numGoroutines = 10
	const numOperations = 50 // Reduced operations to stay within limit

	var wg sync.WaitGroup
	var errorCount int32

	// Start multiple goroutines doing concurrent operations
	for i := range numGoroutines {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()

			for j := range numOperations {
				key := (goroutineID * numOperations) + j

				// Load
				value, err := lru.Load(key, func() (int64, string, error) {
					return 10, fmt.Sprintf("value-%d", key), nil
				})
				if err != nil {
					t.Errorf("Load failed: %v", err)
					return
				}

				expectedValue := fmt.Sprintf("value-%d", key)
				if value != expectedValue {
					t.Errorf("Expected %q, got %q", expectedValue, value)
					return
				}

				// Small delay to allow other goroutines to run
				time.Sleep(time.Microsecond)

				// Peek - note that with concurrent access and evictions,
				// the key might have been evicted by other goroutines
				value, found := lru.Peek(key)
				if found && value != expectedValue {
					t.Errorf("Peek: Expected %q, got %q", expectedValue, value)
					return
				}
				// Don't fail if key was evicted - that's normal under load
			}
		}(i)
	}

	wg.Wait()

	// Just verify no race conditions occurred (would show up as errors above)
	if errorCount > 0 {
		t.Errorf("Detected %d errors during concurrent access", errorCount)
	}
}

func TestLRUInflightDeduplication(t *testing.T) {
	lru := &LRU[string, string]{Limit: 100}

	const numGoroutines = 5
	var wg sync.WaitGroup
	var callCount int32

	fetchFunc := func() (int64, string, error) {
		callCount++
		time.Sleep(50 * time.Millisecond) // Simulate slow operation
		return 10, "shared_value", nil
	}

	// Start multiple goroutines trying to load the same key
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := lru.Load("shared_key", fetchFunc)
			if err != nil {
				t.Errorf("Load failed: %v", err)
				return
			}
			if value != "shared_value" {
				t.Errorf("Expected 'shared_value', got %q", value)
			}
		}()
	}

	wg.Wait()

	// The fetch function should only be called once due to inflight deduplication
	if callCount != 1 {
		t.Errorf("Expected fetch to be called once, was called %d times", callCount)
	}
}

func TestLRUPeekInflightKey(t *testing.T) {
	lru := &LRU[string, string]{Limit: 100}

	// Start a slow load operation
	var fetchStarted sync.WaitGroup
	var proceedWithFetch sync.WaitGroup
	fetchStarted.Add(1)
	proceedWithFetch.Add(1)

	go func() {
		lru.Load("slow_key", func() (int64, string, error) {
			fetchStarted.Done()
			proceedWithFetch.Wait()
			return 10, "slow_value", nil
		})
	}()

	// Wait for fetch to start
	fetchStarted.Wait()

	// Try to peek while fetch is in progress
	go func() {
		value, found := lru.Peek("slow_key")
		if !found {
			t.Error("Should find inflight key via Peek")
		}
		if value != "slow_value" {
			t.Errorf("Expected 'slow_value', got %q", value)
		}
		proceedWithFetch.Done()
	}()

	// Let fetch complete
	time.Sleep(100 * time.Millisecond)
}

func TestLRUUpdateExistingKey(t *testing.T) {
	lru := &LRU[string, string]{Limit: 100}

	// First load
	value, err := lru.Load("key1", func() (int64, string, error) {
		return 10, "value1", nil
	})
	if err != nil || value != "value1" {
		t.Fatalf("First load failed: err=%v, value=%q", err, value)
	}

	// Load same key again with different value
	value, err = lru.Load("key1", func() (int64, string, error) {
		t.Error("Fetch should not be called for existing key")
		return 15, "value2", nil
	})
	if err != nil {
		t.Fatalf("Second load failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected cached 'value1', got %q", value)
	}
}

func TestPromiseWait(t *testing.T) {
	// Test immediate promise
	promise := &Promise[string]{
		ready: ready,
		value: "test_value",
		error: nil,
	}

	value, err := promise.Wait()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if value != "test_value" {
		t.Errorf("Expected 'test_value', got %q", value)
	}

	// Test promise with error
	testErr := errors.New("test error")
	promise = &Promise[string]{
		ready: ready,
		value: "",
		error: testErr,
	}

	value, err = promise.Wait()
	if err != testErr {
		t.Errorf("Expected test error, got %v", err)
	}
	if value != "" {
		t.Errorf("Expected empty value on error, got %q", value)
	}
}

func TestLRUZeroLimit(t *testing.T) {
	lru := &LRU[string, string]{Limit: 0}

	value, err := lru.Load("key1", func() (int64, string, error) {
		return 10, "value1", nil
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if value != "value1" {
		t.Errorf("Expected 'value1', got %q", value)
	}

	// With zero limit, nothing should be cached
	_, found := lru.Peek("key1")
	if found {
		t.Error("Nothing should be cached with zero limit")
	}
}

func TestTTLLoadContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &TTL[string, string]{Limit: 100}

		fetchStarted := make(chan struct{})
		fetchBlock := make(chan struct{})

		// Start a load that blocks until we release it.
		go func() {
			c.Load(context.Background(), "key", time.Now(), false, func(context.Context) (int64, string, time.Time, error) {
				close(fetchStarted)
				<-fetchBlock
				return 10, "value", time.Time{}, nil
			})
		}()

		<-fetchStarted

		// A second caller with a canceled context should return promptly
		// without waiting for the fetch to complete.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := c.Load(ctx, "key", time.Now(), false, func(context.Context) (int64, string, time.Time, error) {
			t.Error("fetch should not be called for a co-waiter")
			return 0, "", time.Time{}, nil
		})
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got %v", err)
		}

		close(fetchBlock)
		synctest.Wait()

		v, _, err := c.Load(context.Background(), "key", time.Now(), false, func(context.Context) (int64, string, time.Time, error) {
			t.Error("fetch should not be called for a cached key")
			return 0, "", time.Time{}, nil
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if v != "value" {
			t.Errorf("expected %q, got %q", "value", v)
		}
	})
}

// TestTTLLoadAlreadyCanceledCacheMiss verifies that a Load call with an
// already-canceled context on a cache miss returns immediately without
// invoking fetch or populating the cache.
func TestTTLLoadAlreadyCanceledCacheMiss(t *testing.T) {
	c := &TTL[string, string]{Limit: 100}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := c.Load(ctx, "key", time.Now(), false, func(context.Context) (int64, string, time.Time, error) {
		t.Error("fetch must not be called with an already-canceled context")
		return 0, "", time.Time{}, nil
	})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	// Nothing should have been stored in the cache.
	_, _, err = c.Load(context.Background(), "key", time.Now(), false, func(context.Context) (int64, string, time.Time, error) {
		return 10, "value", time.Time{}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error on follow-up load: %v", err)
	}
}

func TestTTLLoadUsesConfiguredFetchContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := &TTL[string, string]{Limit: 100}

		type contextKey string

		fetchCanceled := false
		cancelCalled := make(chan struct{})
		c.NewFetchContext = func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey("source"), "cache"))
			return ctx, func() {
				cancel()
				close(cancelCalled)
			}
		}

		waitCtx, waitCancel := context.WithCancel(context.Background())
		fetchStarted := make(chan struct{})
		fetchRelease := make(chan struct{})
		loadDone := make(chan struct{})

		go func() {
			defer close(loadDone)
			_, _, _ = c.Load(waitCtx, "key", time.Now(), false, func(fetchCtx context.Context) (int64, string, time.Time, error) {
				if got := fetchCtx.Value(contextKey("source")); got != "cache" {
					t.Errorf("expected configured fetch context value, got %v", got)
				}
				close(fetchStarted)
				waitCancel()
				select {
				case <-fetchCtx.Done():
					fetchCanceled = true
				case <-fetchRelease:
				}
				return 10, "value", time.Time{}, nil
			})
		}()

		<-fetchStarted
		synctest.Wait()
		if fetchCanceled {
			t.Fatal("fetch context should not be canceled when the waiter is canceled")
		}

		close(fetchRelease)
		synctest.Wait()
		<-loadDone
		select {
		case <-cancelCalled:
		default:
			t.Fatal("expected configured fetch context cancel function to be called")
		}
	})
}
