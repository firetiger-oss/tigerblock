package lru

type LRU[K comparable, V any] struct {
	queue list[item[K, V]]
	cache map[K]*element[item[K, V]]

	Entries   int64
	Size      int64
	Hits      int64
	Misses    int64
	Evictions int64
}

type item[K comparable, V any] struct {
	key   K
	value V
	size  int64
}

func (c *LRU[K, V]) Clear() {
	c.queue = list[item[K, V]]{}
	c.cache = nil
	c.Entries = 0
	c.Size = 0
}

func (c *LRU[K, V]) Lookup(k K) (v V, ok bool) {
	elem, ok := c.cache[k]
	if ok {
		c.queue.moveToFront(elem)
		c.Hits++
		return elem.item.value, ok
	}
	c.Misses++
	return v, false
}

// Peek returns the value for k if present without promoting it to most
// recently used and without updating Hits / Misses.
func (c *LRU[K, V]) Peek(k K) (v V, ok bool) {
	elem, ok := c.cache[k]
	if ok {
		return elem.item.value, true
	}
	return v, false
}

func (c *LRU[K, V]) Insert(k K, v V, size int64) bool {
	if c.cache == nil {
		c.cache = make(map[K]*element[item[K, V]])
	} else if prev := c.cache[k]; prev != nil {
		c.queue.moveToFront(prev)
		c.Size -= prev.item.size
		c.Size += size
		prev.item.value = v
		prev.item.size = size
		return false
	}
	elem := &element[item[K, V]]{
		item: item[K, V]{key: k, value: v, size: size},
	}
	c.Entries++
	c.Size += size
	c.cache[k] = elem
	c.queue.pushFront(elem)
	return true
}

func (c *LRU[K, V]) Delete(k K) {
	if elem := c.cache[k]; elem != nil {
		delete(c.cache, k)
		c.queue.remove(elem)
		c.Entries--
		c.Size -= elem.item.size
		c.Evictions++
	}
}

func (c *LRU[K, V]) Evict() (k K, v V, size int64, ok bool) {
	elem := c.queue.popBack()
	if elem == nil {
		return
	}
	delete(c.cache, elem.item.key)
	c.Entries--
	c.Size -= elem.item.size
	c.Evictions++
	return elem.item.key, elem.item.value, elem.item.size, true
}

type list[T any] struct {
	head *element[T]
	tail *element[T]
}

type element[T any] struct {
	item T
	next *element[T]
	prev *element[T]
}

func (l *list[T]) pushFront(e *element[T]) {
	if l.head == nil {
		l.head = e
		l.tail = e
	} else {
		e.next = l.head
		l.head.prev = e
		l.head = e
	}
}

func (l *list[T]) popBack() *element[T] {
	if l.tail == nil {
		return nil
	}
	e := l.tail
	if l.tail.prev != nil {
		l.tail = l.tail.prev
		l.tail.next = nil
	} else {
		l.head = nil
		l.tail = nil
	}
	return e
}

func (l *list[T]) moveToFront(e *element[T]) {
	if e == l.head {
		return
	}
	if e == l.tail {
		l.tail = e.prev
		l.tail.next = nil
	} else {
		e.prev.next = e.next
		e.next.prev = e.prev
	}
	e.next = l.head
	l.head.prev = e
	l.head = e
	e.prev = nil
}

func (l *list[T]) remove(e *element[T]) {
	if e == l.head {
		l.head = e.next
		if l.head != nil {
			l.head.prev = nil
		} else {
			l.tail = nil
		}
	} else if e == l.tail {
		l.tail = e.prev
		if l.tail != nil {
			l.tail.next = nil
		} else {
			l.head = nil
		}
	} else {
		e.prev.next = e.next
		e.next.prev = e.prev
	}
	e.next = nil
	e.prev = nil
}
