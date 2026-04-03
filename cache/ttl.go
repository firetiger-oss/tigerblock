package cache

import (
	"context"
	"time"
)

type TTL[K comparable, V any] struct {
	LRU[K, ttlEntry[V]]
	NewFetchContext NewFetchContext
}

type ttlEntry[V any] struct {
	value  V
	expire time.Time
}

type NewFetchContext func() (context.Context, context.CancelFunc)

func (c *TTL[K, V]) lru() *LRU[K, ttlEntry[V]] {
	return &c.LRU
}

func (c *TTL[K, V]) Stat() Stat {
	return c.lru().Stat()
}

func (c *TTL[K, V]) Clear() {
	c.lru().Clear()
}

func (c *TTL[K, V]) Drop(ks ...K) {
	c.lru().Drop(ks...)
}

func (c *TTL[K, V]) Load(ctx context.Context, key K, now time.Time, update bool, fetch func(context.Context) (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	var promise *Promise[ttlEntry[V]]

	c.mutex.Lock()
	entry, ok := c.cache.Lookup(key)
	if !ok || update || (!entry.expire.IsZero() && now.After(entry.expire)) {
		if err = ctx.Err(); err != nil {
			c.mutex.Unlock()
			return value, expire, context.Cause(ctx)
		}
		newFetchContext := c.NewFetchContext
		promise = c.lru().get(key, func() (int64, ttlEntry[V], error) {
			fetchCtx := context.Background()
			cancel := func() {}
			if newFetchContext != nil {
				fetchCtx, cancel = newFetchContext()
			}
			defer cancel()

			size, value, expire, err := fetch(fetchCtx)
			if err != nil {
				return 0, ttlEntry[V]{}, err
			}
			return size, ttlEntry[V]{value: value, expire: expire}, nil
		})
	}
	c.mutex.Unlock()

	if promise != nil {
		if err := waitForPromise(ctx, promise); err != nil {
			return value, expire, err
		}
		if promise.error != nil {
			return value, expire, promise.error
		}
		entry = promise.value
	}

	return entry.value, entry.expire, nil
}

func waitForPromise[T any](ctx context.Context, promise *Promise[T]) error {
	select {
	case <-promise.ready:
		return nil
	case <-ctx.Done():
		// Prefer a completed result over a simultaneous cancellation to avoid
		// turning near-deadline cache hits into flaky cancellations.
		select {
		case <-promise.ready:
			return nil
		default:
			return context.Cause(ctx)
		}
	}
}
