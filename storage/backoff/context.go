package backoff

import (
	"context"
	"time"
)

// Default backoff configuration values used when no explicit values are set
// in the context.
const (
	// DefaultMinDelay is the minimum delay between retry attempts.
	DefaultMinDelay = 250 * time.Millisecond

	// DefaultMaxDelay is the maximum delay between retry attempts.
	DefaultMaxDelay = 8 * time.Second
)

// Context keys for storing backoff configuration in contexts.
// These are unexported to prevent external packages from directly
// manipulating context values.
type contextKeyMinDelay struct{}
type contextKeyMaxDelay struct{}
type contextKeyStrategy struct{}

// ContextWithMinDelay returns a new context with the specified minimum delay.
// The minimum delay is the shortest time to wait between retry attempts.
// This value is used as the base for exponential backoff calculations.
//
// Example:
//
//	ctx := backoff.ContextWithMinDelay(context.Background(),
//		100*time.Millisecond)
func ContextWithMinDelay(ctx context.Context, minDelay time.Duration) context.Context {
	return context.WithValue(ctx, contextKeyMinDelay{}, minDelay)
}

// ContextWithMaxDelay returns a new context with the specified maximum delay.
// The maximum delay caps the backoff time, preventing excessively long waits.
// Even with exponential backoff, delays will not exceed this value.
//
// Example:
//
//	ctx := backoff.ContextWithMaxDelay(context.Background(),
//		30*time.Second)
func ContextWithMaxDelay(ctx context.Context, maxDelay time.Duration) context.Context {
	return context.WithValue(ctx, contextKeyMaxDelay{}, maxDelay)
}

// ContextWithStrategy returns a new context with the specified backoff strategy.
// The strategy determines how delays are calculated for each retry attempt.
//
// Example:
//
//	// Use exponential backoff without jitter
//	ctx := backoff.ContextWithStrategy(context.Background(),
//		backoff.Exponential())
//
//	// Use exponential backoff with full jitter (default)
//	ctx := backoff.ContextWithStrategy(context.Background(),
//		backoff.FullJitter(backoff.Exponential()))
func ContextWithStrategy(ctx context.Context, strategy Strategy) context.Context {
	return context.WithValue(ctx, contextKeyStrategy{}, strategy)
}

// MinDelayFromContext extracts the minimum delay from the context.
// If no minimum delay is set in the context, returns DefaultMinDelay.
//
// This function is typically used internally by backoff strategies,
// but can be useful for debugging or custom strategy implementations.
func MinDelayFromContext(ctx context.Context) time.Duration {
	delay, ok := ctx.Value(contextKeyMinDelay{}).(time.Duration)
	if ok {
		return delay
	}
	return DefaultMinDelay
}

// MaxDelayFromContext extracts the maximum delay from the context.
// If no maximum delay is set in the context, returns DefaultMaxDelay.
//
// This function is typically used internally by backoff strategies,
// but can be useful for debugging or custom strategy implementations.
func MaxDelayFromContext(ctx context.Context) time.Duration {
	delay, ok := ctx.Value(contextKeyMaxDelay{}).(time.Duration)
	if ok {
		return delay
	}
	return DefaultMaxDelay
}

// StrategyFromContext extracts the backoff strategy from the context.
// If no strategy is set in the context, returns the default strategy:
// FullJitter(Exponential()), which provides exponential backoff with
// random jitter to prevent thundering herd problems.
//
// This function is typically used internally by Seq(),
// but can be useful for custom retry implementations.
func StrategyFromContext(ctx context.Context) Strategy {
	strategy, ok := ctx.Value(contextKeyStrategy{}).(Strategy)
	if ok {
		return strategy
	}
	return FullJitter(Exponential())
}
