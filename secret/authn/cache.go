package authn

import (
	"context"
	"time"

	"github.com/firetiger-oss/tigerblock/storage/cache"
)

const (
	DefaultLoaderCacheSize = 256 * 1024 // 256 KiB
	DefaultLoaderCacheTTL  = time.Minute
)

// CachedLoader wraps a Loader with TTL-based caching.
type CachedLoader[C any] struct {
	loader Loader[C]
	cache  cache.TTL[string, C]
	ttl    time.Duration
	size   func(C) int64
}

type LoaderCacheOption[C any] func(*CachedLoader[C])

func WithLoaderCacheTTL[C any](ttl time.Duration) LoaderCacheOption[C] {
	return func(c *CachedLoader[C]) { c.ttl = ttl }
}

func WithLoaderCacheSize[C any](size int64) LoaderCacheOption[C] {
	return func(c *CachedLoader[C]) { c.cache.Limit = size }
}

func WithLoaderSizeFunc[C any](fn func(C) int64) LoaderCacheOption[C] {
	return func(c *CachedLoader[C]) { c.size = fn }
}

func NewCachedLoader[C any](loader Loader[C], opts ...LoaderCacheOption[C]) *CachedLoader[C] {
	c := &CachedLoader[C]{
		loader: loader,
		cache:  cache.TTL[string, C]{Limit: DefaultLoaderCacheSize},
		ttl:    DefaultLoaderCacheTTL,
		size:   func(C) int64 { return 0 },
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *CachedLoader[C]) Load(ctx context.Context, id string) (C, error) {
	now := time.Now()
	value, _, err := c.cache.Load(id, now, func() (int64, C, time.Time, error) {
		v, err := c.loader.Load(ctx, id)
		if err != nil {
			var zero C
			return 0, zero, time.Time{}, err
		}
		return c.size(v), v, now.Add(c.ttl), nil
	})
	return value, err
}

var _ Loader[any] = (*CachedLoader[any])(nil)
