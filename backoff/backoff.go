// Package backoff provides utilities for implementing retry logic with various
// backoff strategies. It supports exponential backoff with jitter, context-based
// configuration, and iterator-based retry sequences.
//
// The package is designed around the Strategy interface, which defines how delays
// are calculated for each retry attempt. Common strategies include exponential
// backoff and jittered variants to avoid thundering herd problems.
//
// Example usage:
//
//	ctx := context.Background()
//	ctx = backoff.ContextWithMinDelay(ctx, 100*time.Millisecond)
//	ctx = backoff.ContextWithMaxDelay(ctx, 5*time.Second)
//
//	for now, err := range backoff.Seq(ctx) {
//		if err != nil {
//			return err // Context was cancelled or timed out
//		}
//
//		if tryOperation() {
//			break // Success, exit retry loop
//		}
//
//		// Operation failed, continue to next retry attempt
//	}
package backoff

import (
	"context"
	"iter"
	"math"
	"math/rand/v2"
	"time"
)

// Strategy defines how backoff delays are calculated for retry attempts.
// Implementations should return appropriate delays based on the attempt number
// and the configured minimum and maximum delay bounds.
type Strategy interface {
	// Backoff calculates the delay duration for a given retry attempt.
	// The attempt parameter starts at 1 for the first retry (after the
	// initial attempt). The returned delay should be between 0 and maxDelay,
	// inclusive.
	Backoff(attempt int, minDelay, maxDelay time.Duration) time.Duration
}

// StrategyFunc is a function adapter that implements the Strategy interface.
// It allows ordinary functions to be used as backoff strategies.
type StrategyFunc func(attempt int, minDelay, maxDelay time.Duration) time.Duration

// Backoff implements the Strategy interface for StrategyFunc.
func (f StrategyFunc) Backoff(attempt int, minDelay, maxDelay time.Duration) time.Duration {
	return f(attempt, minDelay, maxDelay)
}

// Exponential returns a Strategy that implements exponential backoff.
// The delay doubles with each attempt: minDelay, minDelay*2, minDelay*4, etc.,
// up to maxDelay. This strategy is deterministic and provides predictable
// delays.
//
// Example delays with minDelay=100ms and maxDelay=8s:
//   - Attempt 1: 100ms
//   - Attempt 2: 200ms
//   - Attempt 3: 400ms
//   - Attempt 4: 800ms
//   - Attempt 5+: 8s (capped at maxDelay)
func Exponential() Strategy {
	return StrategyFunc(func(attempt int, minDelay, maxDelay time.Duration) time.Duration {
		maxShift := int(math.Log2(float64(maxDelay))-math.Log2(float64(minDelay))) + 1
		attempt = min(attempt, maxShift)
		d := min(minDelay<<attempt, maxDelay)
		return d
	})
}

// FullJitter wraps a backoff Strategy to add random jitter, returning a delay
// between minDelay and the original strategy's delay. This helps prevent thundering
// herd problems when many clients retry simultaneously.
//
// Example: If the wrapped strategy is invoked with a minDelay of 250ms and returns
// 1000ms, FullJitter returns a random duration between 250ms and 1000ms.
func FullJitter(backoff Strategy) Strategy {
	return StrategyFunc(func(attempt int, minDelay, maxDelay time.Duration) time.Duration {
		delay := backoff.Backoff(attempt, minDelay, maxDelay)
		if delay <= minDelay {
			return minDelay
		}

		// Subtract a random amount to bring it between [0, delay)
		delay = delay - time.Duration(rand.Int64N(int64(delay)))

		// Re-apply lower bound
		return max(delay, minDelay)
	})
}

// Seq returns an iterator that yields retry attempts with appropriate delays.
// The iterator yields (time.Time, error) pairs where:
//   - time.Time is the current time when the attempt should be made
//   - error is nil for normal attempts, or the context error if
//     cancelled/timed out
//
// The first iteration happens immediately (no delay). Subsequent iterations
// are delayed according to the backoff strategy configured in the context.
//
// The iterator continues indefinitely until:
//   - The context is cancelled or times out
//   - The caller breaks from the loop
//
// Example usage:
//
//	for now, err := range backoff.Seq(ctx) {
//		if err != nil {
//			return fmt.Errorf("backoff cancelled: %w", err)
//		}
//
//		if err := tryOperation(); err == nil {
//			return nil // Success
//		}
//		// Continue to next retry attempt
//	}
func Seq(ctx context.Context) iter.Seq2[time.Time, error] {
	return func(yield func(time.Time, error) bool) {
		minDelay := MinDelayFromContext(ctx)
		maxDelay := MaxDelayFromContext(ctx)
		strategy := StrategyFromContext(ctx)

		now := time.Now()

		for attempt := 1; ; attempt++ {
			if ctx.Err() != nil {
				yield(now, context.Cause(ctx))
				return
			}

			if !yield(now, nil) {
				return
			}

			delay := strategy.Backoff(attempt, minDelay, maxDelay)
			if delay > 0 {
				select {
				case now = <-time.After(delay):
				case <-ctx.Done():
				}
			}
		}
	}
}

// Watch continuously polls a function and yields values only when they change
// from the previous value. The function uses backoff delays between polls,
// resetting the delay after each value change. This is useful for monitoring
// resources or waiting for state changes with efficient polling.
//
// The function fn is called repeatedly with backoff delays between calls.
// When fn returns a value that differs from the previously yielded value, the
// new value is yielded. If fn returns an error, the error is yielded and the
// iteration stops.
//
// The backoff configuration (min delay, max delay, strategy) is taken from
// the context using the same context functions as Seq (ContextWithMinDelay,
// ContextWithMaxDelay, ContextWithStrategy).
//
// The iterator yields (T, error) pairs where:
//   - T is the changed value returned by fn
//   - error is nil for successful value changes, or the error from
//     fn/context
//
// The iteration continues indefinitely until:
//   - The context is cancelled or times out
//   - The function fn returns an error
//   - The caller breaks from the loop
//
// Example usage:
//
//	ctx := backoff.ContextWithMinDelay(context.Background(),
//		100*time.Millisecond)
//
//	checkStatus := func(ctx context.Context) (string, error) {
//		// Poll some external resource
//		return getCurrentStatus(), nil
//	}
//
//	for status, err := range backoff.Watch(ctx, checkStatus) {
//		if err != nil {
//			return fmt.Errorf("watch failed: %w", err)
//		}
//
//		log.Printf("Status changed to: %s", status)
//
//		if status == "ready" {
//			break // Stop watching when desired state is reached
//		}
//	}
func Watch[T comparable](ctx context.Context, fn func(context.Context) (T, error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var lastValue T
		var zeroValue T

		for {
			for _, err := range Seq(ctx) {
				if err != nil {
					yield(zeroValue, err)
					return
				}

				v, err := fn(ctx)
				if err != nil {
					yield(zeroValue, err)
					return
				}

				if v != lastValue {
					if !yield(v, nil) {
						return
					}
					lastValue = v
					break // continue to outer loop to reset backoff delay
				}
			}
		}
	}
}
