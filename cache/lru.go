package cache

import (
	"sync"

	"github.com/firetiger-oss/tigerblock/cache/lru"
)

var ready = make(chan struct{})

func init() {
	close(ready)
}

type Stat struct {
	Limit     int64
	Entries   int64
	Size      int64
	Hits      int64
	Misses    int64
	Evictions int64
}

type Promise[T any] struct {
	ready <-chan struct{}
	value T
	error error
	panic any
}

func (p *Promise[T]) Wait() (T, error) {
	<-p.ready
	if p.panic != nil {
		panic(p.panic)
	}
	return p.value, p.error
}

type LRU[K comparable, V any] struct {
	Limit    int64
	mutex    sync.Mutex
	cache    lru.LRU[K, V]
	inflight map[K]*Promise[V]
}

func (c *LRU[K, V]) Stat() (stat Stat) {
	c.mutex.Lock()
	stat.Limit = c.Limit
	stat.Entries = c.cache.Entries
	stat.Size = c.cache.Size
	stat.Hits = c.cache.Hits
	stat.Misses = c.cache.Misses
	stat.Evictions = c.cache.Evictions
	c.mutex.Unlock()
	return
}

func (c *LRU[K, V]) Clear() {
	c.mutex.Lock()
	c.cache.Clear()
	c.mutex.Unlock()
}

func (c *LRU[K, V]) Drop(ks ...K) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for _, k := range ks {
		c.cache.Delete(k)
	}
}

// Get returns a Promise resolving to the value for k. If k is neither cached
// nor being fetched, the calling goroutine performs fetch inline; concurrent
// callers for the same key receive the same Promise and wait on it. Panics
// from fetch are stored on the Promise, propagated to all waiters via Wait,
// and re-panicked on the originating goroutine.
func (c *LRU[K, V]) Get(k K, fetch func() (int64, V, error)) *Promise[V] {
	return c.GetCloneKey(k, passthrough[K], fetch)
}

// GetCloneKey behaves like Get but invokes clone(k) to produce the key
// that is retained in the cache and inflight map on a miss. The caller's
// k is only used for value-comparison lookups and may be backed by
// transient memory that is reused after the call returns. clone must be
// a pure copy with no observable side effects and must not panic: under
// contention, a caller that loses the race to install an inflight entry
// may invoke clone before discarding the result to join the existing
// fetch.
func (c *LRU[K, V]) GetCloneKey(k K, clone func(K) K, fetch func() (int64, V, error)) *Promise[V] {
	if p := c.lookup(k); p != nil {
		return p
	}
	stored := clone(k)
	p, readyCh := c.install(stored)
	if readyCh == nil {
		return p
	}
	return c.runFetch(stored, p, readyCh, fetch)
}

func (c *LRU[K, V]) Load(k K, fetch func() (int64, V, error)) (V, error) {
	return c.Get(k, fetch).Wait()
}

// LoadCloneKey is the blocking counterpart of GetCloneKey; see that method
// for the clone contract.
func (c *LRU[K, V]) LoadCloneKey(k K, clone func(K) K, fetch func() (int64, V, error)) (V, error) {
	return c.GetCloneKey(k, clone, fetch).Wait()
}

func (c *LRU[K, V]) Peek(k K) (V, bool) {
	c.mutex.Lock()
	v, ok := c.cache.Lookup(k)
	if !ok {
		if p := c.inflight[k]; p != nil {
			c.mutex.Unlock()
			<-p.ready
			return p.value, p.error == nil && p.panic == nil
		}
	}
	c.mutex.Unlock()
	return v, ok
}

// lookup returns a resolved promise for a cache hit, the existing promise
// for an inflight fetch, or nil if neither is present.
func (c *LRU[K, V]) lookup(k K) *Promise[V] {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if v, ok := c.cache.Lookup(k); ok {
		return &Promise[V]{ready: ready, value: v}
	}
	return c.inflight[k]
}

// install installs a new inflight entry for stored. If another goroutine
// completed a fetch for the same key during clone, a promise resolved
// from the cache entry is returned; if an inflight fetch is currently
// running, the existing promise is returned. In both cases readyCh is
// nil. Otherwise the returned readyCh is non-nil and the caller must
// complete the fetch via runFetch.
func (c *LRU[K, V]) install(stored K) (*Promise[V], chan struct{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if _, ok := c.cache.Peek(stored); ok {
		// Race-path hit: another goroutine completed a fetch for stored
		// during clone. Lookup promotes it to MRU and counts the hit.
		v, _ := c.cache.Lookup(stored)
		return &Promise[V]{ready: ready, value: v}, nil
	}
	if p := c.inflight[stored]; p != nil {
		return p, nil
	}
	readyCh := make(chan struct{})
	p := &Promise[V]{ready: readyCh}
	if c.inflight == nil {
		c.inflight = make(map[K]*Promise[V])
	}
	c.inflight[stored] = p
	return p, readyCh
}

// runFetch executes fetch inline, inserts the value into the cache on
// success, and fulfills the promise regardless of outcome. Must only be
// called by the goroutine that owns the inflight entry for stored.
func (c *LRU[K, V]) runFetch(stored K, p *Promise[V], readyCh chan struct{}, fetch func() (int64, V, error)) *Promise[V] {
	var (
		size int64
		v    V
		err  error
	)
	defer func() {
		r := recover()
		c.mutex.Lock()
		if r == nil && err == nil && size < (c.Limit/2) {
			c.cache.Insert(stored, v, size)
			for c.cache.Size > c.Limit {
				c.cache.Evict()
			}
		}
		delete(c.inflight, stored)
		c.mutex.Unlock()
		if r != nil {
			p.panic = r
		} else {
			p.value, p.error = v, err
		}
		close(readyCh)
		if r != nil {
			panic(r)
		}
	}()
	size, v, err = fetch()
	return p
}
