package storage

import (
	"testing"
	"time"
)

func TestCacheControlMaxAge(t *testing.T) {
	tests := []struct {
		header string
		want   time.Duration
		ok     bool
	}{
		{header: "", ok: false},
		{header: "no-store", ok: false},
		{header: "public", ok: false},
		{header: "max-age=0", want: 0, ok: true},
		{header: "max-age=60", want: 60 * time.Second, ok: true},
		{header: "public, max-age=300", want: 300 * time.Second, ok: true},
		{header: "max-age=300, public", want: 300 * time.Second, ok: true},
		{header: "  max-age=42 ", want: 42 * time.Second, ok: true},
		{header: "max-age=", ok: false},
		{header: "max-age=abc", ok: false},
		{header: "max-age=-5", ok: false},
		{header: "s-maxage=600", ok: false},
		{header: "no-cache, max-age=10", want: 10 * time.Second, ok: true},
	}

	for _, tc := range tests {
		t.Run(tc.header, func(t *testing.T) {
			got, ok := cacheControlMaxAge(tc.header)
			if ok != tc.ok {
				t.Fatalf("cacheControlMaxAge(%q) ok = %t, want %t", tc.header, ok, tc.ok)
			}
			if got != tc.want {
				t.Errorf("cacheControlMaxAge(%q) = %v, want %v", tc.header, got, tc.want)
			}
		})
	}
}
