package authn

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCachedLoaderBasic(t *testing.T) {
	loadCount := 0
	loader := LoaderFunc[string](func(ctx context.Context, id string) (string, error) {
		loadCount++
		return "value-" + id, nil
	})

	cached := NewCachedLoader(loader)

	value, err := cached.Load(t.Context(), "key1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if value != "value-key1" {
		t.Errorf("expected value-key1, got %s", value)
	}
	if loadCount != 1 {
		t.Errorf("expected 1 load, got %d", loadCount)
	}

	value, err = cached.Load(t.Context(), "key1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if value != "value-key1" {
		t.Errorf("expected value-key1, got %s", value)
	}
	if loadCount != 1 {
		t.Errorf("expected 1 load (cached), got %d", loadCount)
	}

	value, err = cached.Load(t.Context(), "key2")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if value != "value-key2" {
		t.Errorf("expected value-key2, got %s", value)
	}
	if loadCount != 2 {
		t.Errorf("expected 2 loads, got %d", loadCount)
	}
}

func TestCachedLoaderTTLExpiration(t *testing.T) {
	loadCount := 0
	loader := LoaderFunc[string](func(ctx context.Context, id string) (string, error) {
		loadCount++
		return "value-" + id, nil
	})

	cached := NewCachedLoader(loader, WithLoaderCacheTTL[string](50*time.Millisecond))

	_, err := cached.Load(t.Context(), "key1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loadCount != 1 {
		t.Errorf("expected 1 load, got %d", loadCount)
	}

	_, err = cached.Load(t.Context(), "key1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loadCount != 1 {
		t.Errorf("expected 1 load (cached), got %d", loadCount)
	}

	time.Sleep(60 * time.Millisecond)

	_, err = cached.Load(t.Context(), "key1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loadCount != 2 {
		t.Errorf("expected 2 loads (TTL expired), got %d", loadCount)
	}
}

func TestCachedLoaderError(t *testing.T) {
	expectedErr := errors.New("load failed")
	loadCount := 0
	loader := LoaderFunc[string](func(ctx context.Context, id string) (string, error) {
		loadCount++
		return "", expectedErr
	})

	cached := NewCachedLoader(loader)

	_, err := cached.Load(t.Context(), "key1")
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if loadCount != 1 {
		t.Errorf("expected 1 load, got %d", loadCount)
	}

	_, err = cached.Load(t.Context(), "key1")
	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
	if loadCount != 2 {
		t.Errorf("expected 2 loads (errors not cached), got %d", loadCount)
	}
}

func TestCachedLoaderSizeFunc(t *testing.T) {
	loader := LoaderFunc[string](func(ctx context.Context, id string) (string, error) {
		return "value-" + id, nil
	})

	sizeCalled := false
	cached := NewCachedLoader(loader,
		WithLoaderSizeFunc(func(s string) int64 {
			sizeCalled = true
			return int64(len(s))
		}),
	)

	_, err := cached.Load(t.Context(), "key1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if !sizeCalled {
		t.Error("expected size function to be called")
	}
}

func TestCachedLoaderConcurrency(t *testing.T) {
	var loadCount atomic.Int32
	loader := LoaderFunc[string](func(ctx context.Context, id string) (string, error) {
		loadCount.Add(1)
		time.Sleep(10 * time.Millisecond)
		return "value-" + id, nil
	})

	cached := NewCachedLoader(loader)

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := cached.Load(t.Context(), "key1")
			if err != nil {
				t.Errorf("goroutine %d: Load error: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	if loadCount.Load() != 1 {
		t.Errorf("expected 1 load (deduped), got %d", loadCount.Load())
	}
}

func TestCachedLoaderWithCacheSize(t *testing.T) {
	loadCount := 0
	loader := LoaderFunc[string](func(ctx context.Context, id string) (string, error) {
		loadCount++
		return "value-" + id, nil
	})

	cached := NewCachedLoader(loader,
		WithLoaderCacheSize[string](100),
		WithLoaderSizeFunc(func(s string) int64 {
			return int64(len(s))
		}),
	)

	_, err := cached.Load(t.Context(), "key1")
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if loadCount != 1 {
		t.Errorf("expected 1 load, got %d", loadCount)
	}
}
