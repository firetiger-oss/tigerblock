package backoff_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/firetiger-oss/tigerblock/backoff"
)

func TestExponentialBackoff(t *testing.T) {
	minDelay := time.Second
	maxDelay := 10 * time.Second
	exponential := backoff.Exponential()

	for i := range 10 {
		d := exponential.Backoff(i, minDelay, maxDelay)
		if d < minDelay {
			t.Errorf("backoff %d is less than minDelay: %v < %v", i, d, minDelay)
		}
		if d > maxDelay {
			t.Errorf("backoff %d is greater than maxDelay: %v > %v", i, d, maxDelay)
		}
	}
}

func TestFullJitter(t *testing.T) {
	exponential := backoff.Exponential()
	jittered := backoff.FullJitter(exponential)

	minDelay := time.Second
	maxDelay := 10 * time.Second

	// Test multiple attempts to ensure jitter produces different values
	for attempt := range 5 {
		originalDelay := exponential.Backoff(attempt, minDelay, maxDelay)
		jitteredDelay := jittered.Backoff(attempt, minDelay, maxDelay)

		// Jittered delay should be between 0 and original delay
		if jitteredDelay < 0 {
			t.Errorf("jittered delay %v should not be negative", jitteredDelay)
		}
		if jitteredDelay > originalDelay {
			t.Errorf("jittered delay %v should not exceed original delay %v", jitteredDelay, originalDelay)
		}
	}
}

func TestFullJitterWithZeroDuration(t *testing.T) {
	strategy := backoff.StrategyFunc(func(attempt int, minDelay, maxDelay time.Duration) time.Duration {
		return 0
	})
	jittered := backoff.FullJitter(strategy)

	result := jittered.Backoff(0, time.Second, 10*time.Second)
	if result != time.Second {
		t.Errorf("expected 1s duration for zero backoff, got %v", result)
	}
}

func TestSeq(t *testing.T) {
	minDelay := 10 * time.Millisecond
	maxDelay := 100 * time.Millisecond

	ctx := backoff.ContextWithMaxDelay(
		backoff.ContextWithMinDelay(t.Context(), minDelay),
		maxDelay,
	)

	attempts := 0
	lastTime := time.Time{}

	for tm, err := range backoff.Seq(ctx) {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			break
		}

		attempts++

		// First attempt should be immediate (approximately)
		if attempts == 1 {
			if time.Since(tm) > time.Millisecond {
				t.Errorf("first attempt should be immediate, but was %v ago", time.Since(tm))
			}
		} else {
			// Subsequent attempts should have delays
			if !tm.After(lastTime) {
				t.Errorf("attempt %d should be after previous attempt", attempts)
			}
		}

		lastTime = tm

		// Stop after a few attempts to avoid long test times
		if attempts >= 3 {
			break
		}
	}

	if attempts == 0 {
		t.Error("Seq should yield at least one attempt")
	}
}

func TestSeqWithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	attempts := 0
	for _, err := range backoff.Seq(ctx) {
		attempts++
		if attempts == 1 {
			// First iteration should yield the cancelled context error
			if err == nil {
				t.Error("expected error from cancelled context")
			}
			if err != context.Canceled {
				t.Errorf("expected context.Canceled, got %v", err)
			}
		} else {
			t.Error("should not have more than one iteration with cancelled context")
		}
	}

	if attempts != 1 {
		t.Errorf("expected exactly 1 attempt with cancelled context, got %d", attempts)
	}
}

func TestSeqWithTimeout(t *testing.T) {
	// Create a context that will timeout quickly
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	// Set delays to be longer than the timeout
	minDelay := 100 * time.Millisecond
	ctx = backoff.ContextWithMinDelay(ctx, minDelay)

	attempts := 0
	var lastErr error

	for _, err := range backoff.Seq(ctx) {
		attempts++
		lastErr = err

		// Should eventually get a timeout error
		if err != nil {
			break
		}

		// Prevent infinite loop in case timeout doesn't work
		if attempts > 10 {
			t.Error("too many attempts, timeout not working")
			break
		}
	}

	if attempts == 0 {
		t.Error("should have at least one attempt")
	}

	if lastErr == nil {
		t.Error("expected timeout error")
	}
}

func TestSeqWithCustomStrategy(t *testing.T) {
	// Create a custom strategy that always returns a fixed delay
	fixedDelay := 20 * time.Millisecond
	customStrategy := backoff.StrategyFunc(func(attempt int, minDelay, maxDelay time.Duration) time.Duration {
		return fixedDelay
	})

	ctx := backoff.ContextWithStrategy(t.Context(), customStrategy)

	attempts := 0
	startTime := time.Now()

	for tm, err := range backoff.Seq(ctx) {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			break
		}

		attempts++

		if attempts == 1 {
			// First attempt should be immediate
			if time.Since(startTime) > time.Millisecond {
				t.Error("first attempt should be immediate")
			}
		} else if attempts == 2 {
			// Second attempt should be delayed by approximately the fixed delay
			elapsed := tm.Sub(startTime)
			if elapsed < fixedDelay || elapsed > fixedDelay+10*time.Millisecond {
				t.Errorf("second attempt should be after ~%v, but was after %v", fixedDelay, elapsed)
			}
		}

		// Stop after 2 attempts
		if attempts >= 2 {
			break
		}
	}

	if attempts < 2 {
		t.Errorf("expected at least 2 attempts, got %d", attempts)
	}
}

func TestSeqEarlyTermination(t *testing.T) {
	attempts := 0
	for range backoff.Seq(t.Context()) {
		attempts++
		// Break after first attempt to test early termination
		if attempts == 1 {
			break
		}
	}

	if attempts != 1 {
		t.Errorf("expected exactly 1 attempt with early termination, got %d", attempts)
	}
}

func TestWatch(t *testing.T) {
	ctx := backoff.ContextWithMinDelay(
		backoff.ContextWithMaxDelay(t.Context(), 50*time.Millisecond),
		10*time.Millisecond,
	)

	callCount := 0
	values := []string{"initial", "initial", "changed", "changed", "final"}

	watchFunc := func(context.Context) (string, error) {
		if callCount >= len(values) {
			return "final", nil
		}
		value := values[callCount]
		callCount++
		return value, nil
	}

	iterations := 0
	expectedValues := []string{"initial", "changed", "final"}
	actualValues := []string{}

	for value, err := range backoff.Watch(ctx, watchFunc) {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			break
		}

		actualValues = append(actualValues, value)
		iterations++

		// Stop after getting expected values to avoid infinite loop
		if iterations >= len(expectedValues) {
			break
		}
	}

	if !slices.Equal(actualValues, expectedValues) {
		t.Errorf("expected values %v, got %v", expectedValues, actualValues)
	}
}

func TestWatchWithError(t *testing.T) {
	ctx := backoff.ContextWithMinDelay(t.Context(), 10*time.Millisecond)

	callCount := 0
	watchFunc := func(context.Context) (int, error) {
		callCount++
		if callCount == 3 {
			return 0, errors.New("watch function error")
		}
		return callCount, nil
	}

	iterations := 0
	var lastErr error

	for _, err := range backoff.Watch(ctx, watchFunc) {
		iterations++
		if err != nil {
			lastErr = err
			break
		}

		// Should get values 1, 2 before the error
		if iterations > 2 {
			t.Error("should have received error by now")
			break
		}
	}

	if lastErr == nil {
		t.Error("expected error from watch function")
	}

	if lastErr.Error() != "watch function error" {
		t.Errorf("expected 'watch function error', got %v", lastErr)
	}
}

func TestWatchWithCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())

	watchFunc := func(context.Context) (string, error) {
		return "test", nil
	}

	// Cancel context before starting watch
	cancel()

	iterations := 0
	var lastErr error

	for _, err := range backoff.Watch(ctx, watchFunc) {
		iterations++
		if err != nil {
			lastErr = err
			break
		}

		if iterations > 1 {
			t.Error("should have received cancellation error")
			break
		}
	}

	if lastErr == nil {
		t.Error("expected cancellation error")
	}

	if !errors.Is(lastErr, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", lastErr)
	}
}

func TestWatchWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()

	ctx = backoff.ContextWithMinDelay(ctx, 20*time.Millisecond)

	callCount := 0
	watchFunc := func(context.Context) (int, error) {
		callCount++
		return 1, nil // Always return same value to trigger timeout
	}

	iterations := 0
	var lastErr error

	for _, err := range backoff.Watch(ctx, watchFunc) {
		iterations++
		if err != nil {
			lastErr = err
			break
		}

		// Should eventually timeout
		if iterations > 10 {
			t.Error("timeout not working, too many iterations")
			break
		}
	}

	if lastErr == nil {
		t.Error("expected timeout error")
	}
}

func TestWatchEarlyTermination(t *testing.T) {
	ctx := backoff.ContextWithMinDelay(t.Context(), 10*time.Millisecond)

	callCount := 0
	watchFunc := func(context.Context) (int, error) {
		callCount++
		return callCount, nil
	}

	iterations := 0
	for value, err := range backoff.Watch(ctx, watchFunc) {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
			break
		}

		iterations++
		if iterations == 1 {
			// Verify we got the first value
			if value != 1 {
				t.Errorf("expected first value to be 1, got %d", value)
			}
			break // Early termination
		}
	}

	if iterations != 1 {
		t.Errorf("expected exactly 1 iteration with early termination, got %d", iterations)
	}
}

func TestWatchNoChange(t *testing.T) {
	ctx := backoff.ContextWithMinDelay(t.Context(), 5*time.Millisecond)

	watchFunc := func(context.Context) (string, error) {
		return "constant", nil
	}

	// Use a timeout to prevent infinite waiting
	ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	iterations := 0
	var lastErr error
	for value, err := range backoff.Watch(ctx, watchFunc) {
		iterations++
		if err != nil {
			lastErr = err
			break
		}

		if iterations == 1 {
			if value != "constant" {
				t.Errorf("expected 'constant', got %v", value)
			}
		} else {
			t.Errorf("should not yield same value multiple times, got iteration %d", iterations)
			break
		}
	}

	// Should timeout after yielding the constant value once
	if lastErr == nil {
		t.Error("expected timeout error")
	}

	if iterations == 0 {
		t.Error("expected at least 1 iteration")
	}
}

func TestWatchDifferentTypes(t *testing.T) {
	// Test with different comparable types
	t.Run("int", func(t *testing.T) {
		ctx := backoff.ContextWithMinDelay(t.Context(), 5*time.Millisecond)
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		values := []int{1, 1, 2, 3}
		callCount := 0

		watchFunc := func(context.Context) (int, error) {
			if callCount >= len(values) {
				return values[len(values)-1], nil
			}
			value := values[callCount]
			callCount++
			return value, nil
		}

		expected := []int{1, 2, 3}
		actual := []int{}

		for value, err := range backoff.Watch(ctx, watchFunc) {
			if err != nil {
				// Expected timeout after getting all values
				break
			}
			actual = append(actual, value)
			if len(actual) >= len(expected) {
				break
			}
		}

		if !slices.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})

	t.Run("bool", func(t *testing.T) {
		ctx := backoff.ContextWithMinDelay(t.Context(), 5*time.Millisecond)
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()

		// Start with true so the first false will be different from zero value
		values := []bool{true, true, false, false, true}
		callCount := 0

		watchFunc := func(context.Context) (bool, error) {
			if callCount >= len(values) {
				return values[len(values)-1], nil
			}
			value := values[callCount]
			callCount++
			return value, nil
		}

		expected := []bool{true, false, true}
		actual := []bool{}

		for value, err := range backoff.Watch(ctx, watchFunc) {
			if err != nil {
				// Expected timeout after getting all values
				break
			}
			actual = append(actual, value)
			if len(actual) >= len(expected) {
				break
			}
		}

		if !slices.Equal(actual, expected) {
			t.Errorf("expected %v, got %v", expected, actual)
		}
	})
}
