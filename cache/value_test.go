package cache_test

import (
	"testing"

	"github.com/firetiger-oss/tigerblock/cache"
)

func TestValue(t *testing.T) {
	c := 0
	v := cache.Value[int]{}
	f := func() (int, error) {
		c++
		return c, nil
	}

	a, err := v.Load(f)
	if err != nil {
		t.Fatal(err)
	}
	if a != 1 {
		t.Fatalf("expected 1, got %d", a)
	}

	b, err := v.Load(f)
	if err != nil {
		t.Fatal(err)
	}
	if b != 1 {
		t.Fatalf("expected 1, got %d", b)
	}

	if c != 1 {
		t.Fatalf("expected 1, got %d", c)
	}
}
