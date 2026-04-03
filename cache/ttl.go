package cache

import (
	"context"
	"time"
)

type TTL[K comparable, V any] LRU[K, ttlEntry[V]]

type ttlEntry[V any] struct {
	value  V
	expire time.Time
}

func (c *TTL[K, V]) lru() *LRU[K, ttlEntry[V]] {
	return (*LRU[K, ttlEntry[V]])(c)
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

func (c *TTL[K, V]) Load(ctx context.Context, key K, now time.Time, update bool, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	var promise *Promise[ttlEntry[V]]

	c.mutex.Lock()
	entry, ok := c.cache.Lookup(key)
	if !ok || update || (!entry.expire.IsZero() && now.After(entry.expire)) {
		promise = c.lru().get(key, func() (int64, ttlEntry[V], error) {
			size, value, expire, err := fetch()
			if err != nil {
				return 0, ttlEntry[V]{}, err
			}
			return size, ttlEntry[V]{value: value, expire: expire}, nil
		})
	}
	c.mutex.Unlock()

	if promise != nil {
		select {
		case <-promise.ready:
		case <-ctx.Done():
			return value, expire, context.Cause(ctx)
		}
		if promise.error != nil {
			return value, expire, promise.error
		}
		entry = promise.value
	}

	return entry.value, entry.expire, nil
}
