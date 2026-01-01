package concurrent

import (
	"context"
)

// DefaultLimit is the default maximum number of concurrent operations
// that are performed by functions that use goroutines to manage concurrency.
const DefaultLimit = 10

// Context keys for storing concurrency state.
type (
	semaphoreKey      struct{}
	effectiveLimitKey struct{}
	activeSlotKey     struct{}
)

// semaphore is a channel-based semaphore for limiting concurrent operations.
// It is stored in context and shared across nested concurrent calls.
type semaphore chan struct{}

// acquire blocks until a slot is available or the context is cancelled.
// Returns true if a slot was acquired, false if the context was cancelled.
func (s semaphore) acquire(ctx context.Context) bool {
	select {
	case s <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

// release returns a slot to the semaphore.
func (s semaphore) release() {
	<-s
}

// getSemaphore retrieves the shared semaphore from the context.
// Returns nil if no semaphore has been set.
func getSemaphore(ctx context.Context) semaphore {
	sem, _ := ctx.Value(semaphoreKey{}).(semaphore)
	return sem
}

// getEffectiveLimit returns the effective concurrency limit for the context.
// This may be lower than the semaphore capacity if a nested call reduced it.
func getEffectiveLimit(ctx context.Context) int {
	if limit, ok := ctx.Value(effectiveLimitKey{}).(int); ok {
		return limit
	}
	if sem := getSemaphore(ctx); sem != nil {
		return cap(sem)
	}
	return DefaultLimit
}

// withActiveSlot marks the context as holding an active semaphore slot.
// This is used to detect nested concurrent calls that need to release
// their parent's slot to avoid deadlock.
func withActiveSlot(ctx context.Context, sem semaphore) context.Context {
	return context.WithValue(ctx, activeSlotKey{}, sem)
}

// releaseParentSlot releases the parent's semaphore slot if the current
// context is executing within a goroutine that holds a slot.
// Returns the semaphore and true if a slot was released, nil and false otherwise.
func releaseParentSlot(ctx context.Context) (semaphore, bool) {
	sem, ok := ctx.Value(activeSlotKey{}).(semaphore)
	if ok && sem != nil {
		sem.release()
		return sem, true
	}
	return nil, false
}

// WithLimit creates a new context with a specified maximum concurrency value.
//
// If no semaphore exists in the context, one is created with the specified capacity.
// If a semaphore already exists, only the effective limit is updated (the semaphore
// is shared). The effective limit can only be decreased, never increased.
func WithLimit(ctx context.Context, maxConcurrency int) context.Context {
	if maxConcurrency <= 0 {
		return ctx // ignore invalid values
	}

	// Apply hard limits
	maxConcurrency = max(maxConcurrency, 1)
	maxConcurrency = min(maxConcurrency, 1000)

	sem := getSemaphore(ctx)
	if sem == nil {
		// No semaphore exists, create one with the requested capacity
		sem = make(semaphore, maxConcurrency)
		ctx = context.WithValue(ctx, semaphoreKey{}, sem)
		return context.WithValue(ctx, effectiveLimitKey{}, maxConcurrency)
	}

	// Semaphore already exists - check if we can decrease the effective limit
	currentLimit := getEffectiveLimit(ctx)
	if maxConcurrency >= currentLimit {
		return ctx // never allow increasing the limit
	}

	// Store the reduced effective limit (semaphore stays the same)
	return context.WithValue(ctx, effectiveLimitKey{}, maxConcurrency)
}

// Limit retrieves the maximum concurrency value from the context.
func Limit(ctx context.Context) int {
	limit := getEffectiveLimit(ctx)
	// Apply hard limits as safeguards
	limit = max(limit, 1)
	limit = min(limit, 1000)
	return limit
}
