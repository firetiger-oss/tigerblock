package secret

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestCache(t *testing.T) {
	ctx := t.Context()

	t.Run("caches secret value", func(t *testing.T) {
		var callCount atomic.Int32
		provider := ProviderFunc(func(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
			callCount.Add(1)
			return Value("secret-value"), "v1", nil
		})

		cache := NewCache(provider)

		// First call should hit the provider
		value, version, err := cache.GetSecretValue(ctx, "test-secret")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(value) != "secret-value" {
			t.Errorf("expected value 'secret-value', got %q", value)
		}
		if version != "v1" {
			t.Errorf("expected version 'v1', got %q", version)
		}
		if callCount.Load() != 1 {
			t.Errorf("expected 1 call to provider, got %d", callCount.Load())
		}

		// Second call should be cached
		value, version, err = cache.GetSecretValue(ctx, "test-secret")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(value) != "secret-value" {
			t.Errorf("expected value 'secret-value', got %q", value)
		}
		if version != "v1" {
			t.Errorf("expected version 'v1', got %q", version)
		}
		if callCount.Load() != 1 {
			t.Errorf("expected still 1 call to provider (cache hit), got %d", callCount.Load())
		}
	})

	t.Run("caches different secrets separately", func(t *testing.T) {
		var callCount atomic.Int32
		provider := ProviderFunc(func(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
			callCount.Add(1)
			return Value(name + "-value"), "v1", nil
		})

		cache := NewCache(provider)

		// Fetch first secret
		value1, _, err := cache.GetSecretValue(ctx, "secret1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(value1) != "secret1-value" {
			t.Errorf("expected value 'secret1-value', got %q", value1)
		}

		// Fetch second secret
		value2, _, err := cache.GetSecretValue(ctx, "secret2")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(value2) != "secret2-value" {
			t.Errorf("expected value 'secret2-value', got %q", value2)
		}

		if callCount.Load() != 2 {
			t.Errorf("expected 2 calls to provider, got %d", callCount.Load())
		}

		// Both should now be cached
		cache.GetSecretValue(ctx, "secret1")
		cache.GetSecretValue(ctx, "secret2")

		if callCount.Load() != 2 {
			t.Errorf("expected still 2 calls to provider (cache hits), got %d", callCount.Load())
		}
	})

	t.Run("caches different versions separately", func(t *testing.T) {
		var callCount atomic.Int32
		provider := ProviderFunc(func(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
			callCount.Add(1)
			opts := NewGetOptions(options...)
			version := opts.Version()
			if version == "" {
				version = "v2"
			}
			return Value("value-" + version), version, nil
		})

		cache := NewCache(provider)

		// Fetch current version
		value1, version1, err := cache.GetSecretValue(ctx, "secret")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if version1 != "v2" {
			t.Errorf("expected version 'v2', got %q", version1)
		}

		// Fetch specific version
		value2, version2, err := cache.GetSecretValue(ctx, "secret", WithVersion("v1"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if version2 != "v1" {
			t.Errorf("expected version 'v1', got %q", version2)
		}
		if string(value2) != "value-v1" {
			t.Errorf("expected value 'value-v1', got %q", value2)
		}

		if callCount.Load() != 2 {
			t.Errorf("expected 2 calls (different versions), got %d", callCount.Load())
		}

		// Both should be cached
		cache.GetSecretValue(ctx, "secret")
		cache.GetSecretValue(ctx, "secret", WithVersion("v1"))

		if callCount.Load() != 2 {
			t.Errorf("expected still 2 calls (cache hits), got %d", callCount.Load())
		}

		// Verify values are different
		if string(value1) == string(value2) {
			t.Error("expected different cached values for different versions")
		}
	})

	t.Run("propagates errors", func(t *testing.T) {
		expectedErr := errors.New("provider error")
		provider := ProviderFunc(func(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
			return nil, "", expectedErr
		})

		cache := NewCache(provider)

		_, _, err := cache.GetSecretValue(ctx, "secret")
		if err != expectedErr {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	t.Run("respects CacheSize option", func(t *testing.T) {
		cache := NewCache(nil, CacheSize(1024))
		if cache.cache.Limit != 1024 {
			t.Errorf("expected cache limit 1024, got %d", cache.cache.Limit)
		}
	})

	t.Run("respects CacheTTL option", func(t *testing.T) {
		cache := NewCache(nil, CacheTTL(5*time.Minute))
		if cache.ttl != 5*time.Minute {
			t.Errorf("expected TTL 5m, got %v", cache.ttl)
		}
	})

	t.Run("uses default values", func(t *testing.T) {
		cache := NewCache(nil)
		if cache.cache.Limit != DefaultCacheSize {
			t.Errorf("expected default cache size %d, got %d", DefaultCacheSize, cache.cache.Limit)
		}
		if cache.ttl != DefaultCacheTTL {
			t.Errorf("expected default TTL %v, got %v", DefaultCacheTTL, cache.ttl)
		}
	})

	t.Run("zero TTL means no expiration", func(t *testing.T) {
		cache := NewCache(nil, CacheTTL(0))
		expireAt := cache.expireAt()
		if !expireAt.IsZero() {
			t.Errorf("expected zero time for no expiration, got %v", expireAt)
		}
	})

	t.Run("positive TTL sets future expiration", func(t *testing.T) {
		cache := NewCache(nil, CacheTTL(time.Minute))
		before := time.Now()
		expireAt := cache.expireAt()
		after := time.Now()

		if expireAt.Before(before.Add(time.Minute)) {
			t.Error("expiration time is too early")
		}
		if expireAt.After(after.Add(time.Minute + time.Second)) {
			t.Error("expiration time is too late")
		}
	})
}

func TestCacheImplementsProvider(t *testing.T) {
	// Compile-time check that Cache implements Provider
	var _ Provider = (*Cache)(nil)
}
