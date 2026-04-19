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

func (c *Cache[K, V]) LoadCloneKey(key K, clone func(K) K, load func() (V, error)) (V, error) {
	c.mutex.RLock()
	value, ok := c.state[key]
	c.mutex.RUnlock()
	if ok {
		return value, nil
	}
	c.mutex.Lock()
	if value, ok := c.state[key]; ok {
		c.mutex.Unlock()
		return value, nil
	}
	if p := c.inflight[key]; p != nil {
		c.mutex.Unlock()
		return p.Wait()
	}
	return c.fetchLocked(key, clone, load).Wait()
}

// fetchLocked is called with c.mutex held for writing, after the caller has
// confirmed that neither c.state nor c.inflight currently has an entry for
// key. The lock is released before returning.
func (c *Cache[K, V]) fetchLocked(key K, clone func(K) K, load func() (V, error)) *Promise[V] {
	stored := clone(key)
	readyCh := make(chan struct{})
	p := &Promise[V]{ready: readyCh}
	if c.inflight == nil {
		c.inflight = make(map[K]*Promise[V])
	}
	c.inflight[stored] = p
	c.mutex.Unlock()

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
	return p
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
