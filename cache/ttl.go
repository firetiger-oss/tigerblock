package cache

import "time"

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

func (c *TTL[K, V]) Load(key K, now time.Time, update bool, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	c.mutex.Lock()
	entry, ok := c.cache.Lookup(key)
	if ok && !update && (entry.expire.IsZero() || !now.After(entry.expire)) {
		c.mutex.Unlock()
		return entry.value, entry.expire, nil
	}

	promise := c.lru().fetchLocked(key, func() (int64, ttlEntry[V], error) {
		size, value, expire, err := fetch()
		if err != nil {
			return 0, ttlEntry[V]{}, err
		}
		return size, ttlEntry[V]{value: value, expire: expire}, nil
	})

	entry, err = promise.Wait()
	if err != nil {
		return value, expire, err
	}
	return entry.value, entry.expire, nil
}
