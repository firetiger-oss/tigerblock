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

func (c *TTL[K, V]) Load(key K, now time.Time, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	return c.LoadCloneKey(key, now, passthrough[K], fetch)
}

func (c *TTL[K, V]) LoadCloneKey(key K, now time.Time, clone func(K) K, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	c.mutex.Lock()
	entry, ok := c.cache.Lookup(key)
	if ok && (entry.expire.IsZero() || !now.After(entry.expire)) {
		c.mutex.Unlock()
		return entry.value, entry.expire, nil
	}
	return c.fetchLocked(key, clone, fetch)
}

func (c *TTL[K, V]) Reload(key K, now time.Time, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	return c.ReloadCloneKey(key, now, passthrough[K], fetch)
}

func (c *TTL[K, V]) ReloadCloneKey(key K, now time.Time, clone func(K) K, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	c.mutex.Lock()
	return c.fetchLocked(key, clone, fetch)
}

func (c *TTL[K, V]) fetchLocked(key K, clone func(K) K, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	promise := c.lru().fetchLocked(key, clone, func() (int64, ttlEntry[V], error) {
		size, value, expire, err := fetch()
		if err != nil {
			return 0, ttlEntry[V]{}, err
		}
		return size, ttlEntry[V]{value: value, expire: expire}, nil
	})

	entry, err := promise.Wait()
	if err != nil {
		return value, expire, err
	}
	return entry.value, entry.expire, nil
}
