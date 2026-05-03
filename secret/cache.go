package secret

import (
	"context"
	"time"

	"github.com/firetiger-oss/tigerblock/cache"
)

const (
	// DefaultCacheSize is the default maximum size of the cache in bytes.
	DefaultCacheSize = 512 * 1024 // 512 KiB
	// DefaultCacheTTL is the default time-to-live for cached entries.
	DefaultCacheTTL = time.Minute
)

type cacheKey struct {
	name    string
	version string
}

type cacheEntry struct {
	value   Value
	version string
}

// Cache is a caching wrapper for a Provider that caches the results of
// GetSecretValue calls using a TTL-based LRU cache.
type Cache struct {
	provider Provider
	cache    cache.TTL[cacheKey, cacheEntry]
	ttl      time.Duration
}

// CacheOption is a function type that can be used to configure new Cache
// instances created by calling NewCache.
type CacheOption func(*Cache)

// CacheSize sets the maximum size of the cache in bytes.
func CacheSize(size int64) CacheOption {
	return func(c *Cache) { c.cache.Limit = size }
}

// CacheTTL sets the time-to-live for cached entries. After the TTL expires,
// entries will be re-fetched on the next access. A TTL of 0 disables expiration.
func CacheTTL(ttl time.Duration) CacheOption {
	return func(c *Cache) { c.ttl = ttl }
}

// NewCache creates a new Cache that wraps the given provider.
//
// By default, the cache size is 512 KiB and the TTL is 1 minute.
func NewCache(provider Provider, options ...CacheOption) *Cache {
	c := &Cache{
		provider: provider,
		cache:    cache.TTL[cacheKey, cacheEntry]{Limit: DefaultCacheSize},
		ttl:      DefaultCacheTTL,
	}
	for _, opt := range options {
		opt(c)
	}
	return c
}

// GetSecretValue retrieves a secret value, using the cache when possible.
func (c *Cache) GetSecretValue(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
	opts := NewGetOptions(options...)
	key := cacheKey{name: name, version: opts.Version()}

	entry, _, err := c.cache.Load(key, time.Now(), func() (int64, cacheEntry, time.Time, error) {
		value, version, err := c.provider.GetSecretValue(ctx, name, options...)
		if err != nil {
			return 0, cacheEntry{}, time.Time{}, err
		}
		return int64(len(value)), cacheEntry{value: value, version: version}, c.expireAt(), nil
	})
	if err != nil {
		return nil, "", err
	}
	return entry.value, entry.version, nil
}

func (c *Cache) expireAt() time.Time {
	if c.ttl > 0 {
		return time.Now().Add(c.ttl)
	}
	return time.Time{}
}

var _ Provider = (*Cache)(nil)
