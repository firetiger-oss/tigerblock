package cache

import (
	"iter"
	"sync"
)

func passthrough[T any](v T) T { return v }

type Cache[K comparable, V any] struct {
	mutex    sync.RWMutex
	queue    chan K
	state    map[K]V
	inflight map[K]*Promise[V]
}

func New[K comparable, V any](size int) *Cache[K, V] {
	cache := makeCache[K, V](size)
	return &cache
}

func makeCache[K comparable, V any](size int) Cache[K, V] {
	return Cache[K, V]{
		queue: make(chan K, size),
		state: make(map[K]V, size),
	}
}

func (c *Cache[K, V]) Load(key K, load func() (V, error)) (V, error) {
	return c.LoadCloneKey(key, passthrough[K], load)
}

// LoadCloneKey behaves like Load but invokes clone(key) on a miss to
// produce the key retained in the cache, so callers can pass transient
// keys (e.g. strings backed by a reused buffer) without the cache
// retaining that backing memory. clone must be a pure copy with no
// observable side effects and must not panic: under contention, a caller
// that loses the race to install an inflight entry may invoke clone
// before discarding the result to join the existing fetch.
func (c *Cache[K, V]) LoadCloneKey(key K, clone func(K) K, load func() (V, error)) (V, error) {
	v, hit, p := c.peek(key)
	if hit {
		return v, nil
	}
	if p != nil {
		return p.Wait()
	}
	stored := clone(key)
	p, readyCh := c.claim(stored)
	if readyCh == nil {
		return p.Wait()
	}
	c.runLoad(stored, p, readyCh, load)
	return p.Wait()
}

// peek checks state and inflight under a read lock. hit is true iff the
// value is currently in state; otherwise p is either a pending inflight
// promise or nil when the caller must clone and install one.
func (c *Cache[K, V]) peek(key K) (value V, hit bool, p *Promise[V]) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	if v, ok := c.state[key]; ok {
		return v, true, nil
	}
	return value, false, c.inflight[key]
}

// claim atomically checks state and inflight for key. If either holds a
// result, the returned readyCh is nil and the caller should simply Wait on
// the promise. Otherwise the caller owns a freshly installed inflight entry
// and must complete it via runLoad.
func (c *Cache[K, V]) claim(key K) (*Promise[V], chan struct{}) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if v, ok := c.state[key]; ok {
		return &Promise[V]{ready: ready, value: v}, nil
	}
	if p := c.inflight[key]; p != nil {
		return p, nil
	}
	readyCh := make(chan struct{})
	p := &Promise[V]{ready: readyCh}
	if c.inflight == nil {
		c.inflight = make(map[K]*Promise[V])
	}
	c.inflight[key] = p
	return p, readyCh
}

// runLoad executes load and fulfills the promise installed for stored. Must
// only be called by the goroutine that owns the inflight entry.
func (c *Cache[K, V]) runLoad(stored K, p *Promise[V], readyCh chan struct{}, load func() (V, error)) {
	var (
		v   V
		err error
	)
	defer func() {
		r := recover()
		c.mutex.Lock()
		if r == nil && err == nil {
			if len(c.queue) == cap(c.queue) {
				oldest := <-c.queue
				delete(c.state, oldest)
			}
			c.state[stored] = v
			c.queue <- stored
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
	v, err = load()
}

type SeqCache[K comparable, V any] struct{ cache Cache[K, []V] }

func Seq[K comparable, V any](size int) *SeqCache[K, V] {
	return &SeqCache[K, V]{
		cache: makeCache[K, []V](size),
	}
}

func (s *SeqCache[K, V]) Load(key K, load iter.Seq2[V, error]) iter.Seq2[V, error] {
	return s.LoadCloneKey(key, passthrough[K], load)
}

// LoadCloneKey behaves like Load but invokes clone(key) to produce the
// key retained in the cache. See Cache.LoadCloneKey for the clone contract.
func (s *SeqCache[K, V]) LoadCloneKey(key K, clone func(K) K, load iter.Seq2[V, error]) iter.Seq2[V, error] {
	return func(yield func(V, error) bool) {
		values, err := s.cache.LoadCloneKey(key, clone, func() ([]V, error) {
			var values []V
			for v, err := range load {
				if err != nil {
					return nil, err
				}
				values = append(values, v)
			}
			return values, nil
		})
		for _, v := range values {
			if !yield(v, nil) {
				return
			}
		}
		if err != nil {
			var zero V
			yield(zero, err)
		}
	}
}
