package cache

import "sync"

type Value[T any] struct {
	mutex  sync.Mutex
	value  T
	cached bool
}

func (v *Value[T]) Load(f func() (T, error)) (T, error) {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if v.cached {
		return v.value, nil
	}

	cached, err := f()
	if err == nil {
		v.value = cached
		v.cached = true
	}
	return cached, err
}
