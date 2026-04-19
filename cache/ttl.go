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

// LoadCloneKey behaves like Load but invokes clone(key) to produce the
// key retained in the cache. See cache.Cache.LoadCloneKey for the clone
// contract.
func (c *TTL[K, V]) LoadCloneKey(key K, now time.Time, clone func(K) K, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	if p := c.lookup(key, now); p != nil {
		return await(p)
	}
	return c.fulfill(clone(key), fetch)
}

func (c *TTL[K, V]) Reload(key K, now time.Time, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	return c.ReloadCloneKey(key, now, passthrough[K], fetch)
}

// ReloadCloneKey is the clone-key variant of Reload. See
// cache.Cache.LoadCloneKey for the clone contract.
func (c *TTL[K, V]) ReloadCloneKey(key K, now time.Time, clone func(K) K, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	// Join a concurrent refresh (if any) without cloning.
	c.mutex.Lock()
	p := c.inflight[key]
	c.mutex.Unlock()
	if p != nil {
		return await(p)
	}
	return c.fulfill(clone(key), fetch)
}

// lookup returns a resolved promise for a fresh cached entry, the existing
// promise for an inflight fetch, or nil if neither is present.
func (c *TTL[K, V]) lookup(key K, now time.Time) *Promise[ttlEntry[V]] {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	entry, ok := c.cache.Lookup(key)
	if ok && (entry.expire.IsZero() || !now.After(entry.expire)) {
		return &Promise[ttlEntry[V]]{ready: ready, value: entry}
	}
	return c.inflight[key]
}

// fulfill installs an inflight entry for stored (or joins an existing one)
// and waits for the result.
func (c *TTL[K, V]) fulfill(stored K, fetch func() (int64, V, time.Time, error)) (value V, expire time.Time, err error) {
	p, readyCh := c.lru().install(stored)
	if readyCh != nil {
		c.lru().runFetch(stored, p, readyCh, func() (int64, ttlEntry[V], error) {
			size, v, expire, err := fetch()
			if err != nil {
				return 0, ttlEntry[V]{}, err
			}
			return size, ttlEntry[V]{value: v, expire: expire}, nil
		})
	}
	return await(p)
}

func await[V any](p *Promise[ttlEntry[V]]) (value V, expire time.Time, err error) {
	entry, err := p.Wait()
	if err != nil {
		return value, expire, err
	}
	return entry.value, entry.expire, nil
}
