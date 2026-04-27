package cache_test

import (
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/firetiger-oss/tigerblock/storage/cache"
)

func TestCache(t *testing.T) {
	c := cache.New[string, string](100)

	dataset := map[string]string{
		"foo": "bar",
		"baz": "qux",
	}

	for k := range dataset {
		v, err := c.Load(k, func() (string, error) {
			return dataset[k], nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if v != dataset[k] {
			t.Errorf("c.Load(%q)=%q, want %q", k, v, dataset[k])
		}
	}
}

func TestSeqCache(t *testing.T) {
	c := cache.Seq[string, string](100)

	dataset := map[string][]string{
		"foo": {"bar", "baz"},
		"baz": {"A", "B", "C"},
	}

	for k := range dataset {
		var values []string
		for v, err := range c.Load(k, func(yield func(string, error) bool) {
			for _, v := range dataset[k] {
				if !yield(v, nil) {
					return
				}
			}
		}) {
			if err != nil {
				t.Fatal(err)
			}
			values = append(values, v)
		}
		slices.Sort(values)
		if !slices.Equal(values, dataset[k]) {
			t.Errorf("c.Load(%q)=%q, want %q", k, values, dataset[k])
		}
	}

}

func TestCacheLoadCloneKey(t *testing.T) {
	c := cache.New[string, string](10)
	var cloneCount int
	clone := func(k string) string {
		cloneCount++
		return k
	}

	v, err := c.LoadCloneKey("foo", clone, func() (string, error) {
		return "bar", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != "bar" {
		t.Errorf("v=%q want %q", v, "bar")
	}
	if cloneCount != 1 {
		t.Errorf("cloneCount=%d want 1 after miss", cloneCount)
	}

	v, err = c.LoadCloneKey("foo", clone, func() (string, error) {
		t.Error("load should not run on hit")
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != "bar" {
		t.Errorf("v=%q want %q", v, "bar")
	}
	if cloneCount != 1 {
		t.Errorf("cloneCount=%d want 1 after hit", cloneCount)
	}
}

func TestCacheLoadCloneKeyStoresClonedValue(t *testing.T) {
	c := cache.New[string, int](10)

	buf := []byte("foo")
	transient := unsafe.String(&buf[0], len(buf))

	v, err := c.LoadCloneKey(transient, strings.Clone, func() (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Errorf("v=%d want 42", v)
	}

	// Mutating buf would break a cached key that aliased the buffer. With
	// strings.Clone, the stored key is independent of buf.
	copy(buf, "xxx")

	v, err = c.LoadCloneKey("foo", strings.Clone, func() (int, error) {
		t.Error("load should not run; stored key must equal looked-up key")
		return 0, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != 42 {
		t.Errorf("v=%d want 42 (hit)", v)
	}
}

func TestCacheLoadCloneKeyInflightDedup(t *testing.T) {
	c := cache.New[string, string](10)

	const numGoroutines = 5
	var callCount int32
	var start sync.WaitGroup
	var done sync.WaitGroup
	var mu sync.Mutex
	start.Add(numGoroutines)
	done.Add(numGoroutines)

	fetch := func() (string, error) {
		mu.Lock()
		callCount++
		mu.Unlock()
		time.Sleep(30 * time.Millisecond)
		return "shared", nil
	}

	for range numGoroutines {
		go func() {
			defer done.Done()
			start.Done()
			start.Wait()
			v, err := c.LoadCloneKey("k", passthroughString, fetch)
			if err != nil {
				t.Errorf("LoadCloneKey failed: %v", err)
			}
			if v != "shared" {
				t.Errorf("v=%q want shared", v)
			}
		}()
	}
	done.Wait()

	if callCount != 1 {
		t.Errorf("fetch ran %d times, want 1 (dedup)", callCount)
	}
}

func TestCacheLoadCloneKeyErrorNotCachedNorInflightRetained(t *testing.T) {
	c := cache.New[string, string](10)
	boom := errors.New("boom")

	_, err := c.LoadCloneKey("k", passthroughString, func() (string, error) {
		return "", boom
	})
	if err != boom {
		t.Errorf("err=%v want %v", err, boom)
	}

	v, err := c.LoadCloneKey("k", passthroughString, func() (string, error) {
		return "ok", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != "ok" {
		t.Errorf("v=%q want ok", v)
	}
}

func TestCachePanicPropagatesAndClearsInflight(t *testing.T) {
	c := cache.New[string, string](10)

	func() {
		defer func() {
			r := recover()
			if r != "boom" {
				t.Errorf("recovered=%v want boom", r)
			}
		}()
		c.LoadCloneKey("k", passthroughString, func() (string, error) {
			panic("boom")
		})
		t.Error("LoadCloneKey should have panicked")
	}()

	v, err := c.LoadCloneKey("k", passthroughString, func() (string, error) {
		return "after", nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if v != "after" {
		t.Errorf("v=%q want after", v)
	}
}

func TestCacheLoadCloneKeyJoinsInflightWithoutCloning(t *testing.T) {
	c := cache.New[string, string](10)

	fetchStarted := make(chan struct{})
	releaseFetch := make(chan struct{})

	loadDone := make(chan struct{})
	go func() {
		defer close(loadDone)
		c.LoadCloneKey("k", passthroughString, func() (string, error) {
			close(fetchStarted)
			<-releaseFetch
			return "v", nil
		})
	}()
	<-fetchStarted

	var cloneCount int32
	joinDone := make(chan struct{})
	go func() {
		defer close(joinDone)
		v, err := c.LoadCloneKey("k", func(s string) string {
			atomic.AddInt32(&cloneCount, 1)
			return s
		}, func() (string, error) {
			t.Error("load should not run when joining an existing inflight fetch")
			return "", nil
		})
		if err != nil {
			t.Errorf("err=%v", err)
		}
		if v != "v" {
			t.Errorf("v=%q want v", v)
		}
	}()

	// Give the second caller time to reach the inflight check.
	time.Sleep(50 * time.Millisecond)
	close(releaseFetch)
	<-loadDone
	<-joinDone

	if got := atomic.LoadInt32(&cloneCount); got != 0 {
		t.Errorf("clone ran %d times while joining inflight, want 0", got)
	}
}

func TestCacheLoadCloneKeyPanicInCloneReleasesLock(t *testing.T) {
	c := cache.New[string, string](10)

	func() {
		defer func() {
			if r := recover(); r != "boom" {
				t.Errorf("recovered=%v want boom", r)
			}
		}()
		c.LoadCloneKey("k", func(string) string { panic("boom") }, func() (string, error) {
			t.Error("load should not run when clone panics")
			return "", nil
		})
		t.Error("LoadCloneKey should have panicked")
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		v, err := c.LoadCloneKey("k", passthroughString, func() (string, error) {
			return "after", nil
		})
		if err != nil {
			t.Errorf("err=%v", err)
		}
		if v != "after" {
			t.Errorf("v=%q want after", v)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("second LoadCloneKey deadlocked — panic in clone did not release the lock")
	}
}

func TestSeqCacheLoadCloneKey(t *testing.T) {
	c := cache.Seq[string, string](10)
	var cloneCount int
	clone := func(k string) string {
		cloneCount++
		return k
	}

	var out []string
	for v, err := range c.LoadCloneKey("k", clone, func(yield func(string, error) bool) {
		for _, v := range []string{"a", "b", "c"} {
			if !yield(v, nil) {
				return
			}
		}
	}) {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if !slices.Equal(out, []string{"a", "b", "c"}) {
		t.Errorf("out=%q", out)
	}
	if cloneCount != 1 {
		t.Errorf("cloneCount=%d want 1", cloneCount)
	}

	out = out[:0]
	for v, err := range c.LoadCloneKey("k", clone, func(yield func(string, error) bool) {
		t.Error("load should not run on hit")
	}) {
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if !slices.Equal(out, []string{"a", "b", "c"}) {
		t.Errorf("hit out=%q", out)
	}
	if cloneCount != 1 {
		t.Errorf("cloneCount=%d want 1 after hit", cloneCount)
	}
}

func passthroughString(s string) string { return s }
