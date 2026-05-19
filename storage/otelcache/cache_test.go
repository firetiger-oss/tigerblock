package otelcache_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	"github.com/firetiger-oss/tigerblock/storage/otelcache"
)

func TestRegisterCache(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	cache := storage.NewCache(
		storage.ObjectCacheSize(1024),
		storage.ObjectInfoCacheSize(1024),
		storage.ObjectPageCacheSize(1024),
		storage.CachePageSize(4),
	)

	reg, err := otelcache.RegisterCache(provider, cache,
		attribute.String("storage.cache.id", "test"),
	)
	if err != nil {
		t.Fatalf("RegisterCache failed: %v", err)
	}
	t.Cleanup(func() {
		_ = reg.Unregister()
	})

	exerciseBucket(t, cache.AdaptBucket(memory.NewBucket()))

	objectsStat, infosStat, pagesStat := cache.Stat()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	metrics := collectByKind(t, rm)

	for _, c := range []struct {
		name string
		stat storage.CacheStat
	}{
		{"objects", objectsStat},
		{"infos", infosStat},
		{"pages", pagesStat},
	} {
		assertKindValue(t, metrics, "storage.cache.size.bytes", c.name, c.stat.Size)
		assertKindValue(t, metrics, "storage.cache.limit.bytes", c.name, c.stat.Limit)
		assertKindValue(t, metrics, "storage.cache.entries", c.name, c.stat.Entries)
		assertKindValue(t, metrics, "storage.cache.hits", c.name, c.stat.Hits)
		assertKindValue(t, metrics, "storage.cache.misses", c.name, c.stat.Misses)
		assertKindValue(t, metrics, "storage.cache.evictions", c.name, c.stat.Evictions)
	}
}

func exerciseBucket(t *testing.T, bucket storage.Bucket) {
	t.Helper()
	ctx := context.Background()

	data := []byte("hello world")
	if _, err := bucket.PutObject(ctx, "test", bytes.NewReader(data)); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if _, err := bucket.HeadObject(ctx, "test"); err != nil {
		t.Fatalf("HeadObject miss failed: %v", err)
	}
	if _, err := bucket.HeadObject(ctx, "test"); err != nil {
		t.Fatalf("HeadObject hit failed: %v", err)
	}

	full, _, err := bucket.GetObject(ctx, "test")
	if err != nil {
		t.Fatalf("GetObject miss failed: %v", err)
	}
	if _, err := io.ReadAll(full); err != nil {
		t.Fatalf("reading full object miss: %v", err)
	}
	full.Close()

	full, _, err = bucket.GetObject(ctx, "test")
	if err != nil {
		t.Fatalf("GetObject hit failed: %v", err)
	}
	if _, err := io.ReadAll(full); err != nil {
		t.Fatalf("reading full object hit: %v", err)
	}
	full.Close()

	page, _, err := bucket.GetObject(ctx, "test", storage.BytesRange(0, 3))
	if err != nil {
		t.Fatalf("GetObject page miss failed: %v", err)
	}
	if _, err := io.ReadAll(page); err != nil {
		t.Fatalf("reading page miss: %v", err)
	}
	page.Close()

	page, _, err = bucket.GetObject(ctx, "test", storage.BytesRange(0, 3))
	if err != nil {
		t.Fatalf("GetObject page hit failed: %v", err)
	}
	if _, err := io.ReadAll(page); err != nil {
		t.Fatalf("reading page hit: %v", err)
	}
	page.Close()
}

func collectByKind(t *testing.T, rm metricdata.ResourceMetrics) map[string]map[string]int64 {
	t.Helper()

	out := make(map[string]map[string]int64)
	record := func(name string, attrs attribute.Set, value int64) {
		kindVal, ok := attrs.Value("storage.cache.kind")
		if !ok {
			t.Fatalf("metric %q missing storage.cache.kind attribute", name)
		}
		idVal, ok := attrs.Value("storage.cache.id")
		if !ok || idVal.AsString() == "" {
			t.Fatalf("metric %q missing storage.cache.id attribute", name)
		}
		if _, ok := out[name]; !ok {
			out[name] = make(map[string]int64)
		}
		out[name][kindVal.AsString()] = value
	}

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					record(m.Name, p.Attributes, p.Value)
				}
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					record(m.Name, p.Attributes, p.Value)
				}
			}
		}
	}
	return out
}

func assertKindValue(t *testing.T, m map[string]map[string]int64, name, kind string, want int64) {
	t.Helper()
	byKind, ok := m[name]
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	got, ok := byKind[kind]
	if !ok {
		t.Fatalf("metric %q missing kind %q", name, kind)
	}
	if got != want {
		t.Fatalf("metric %q kind %q = %d, want %d", name, kind, got, want)
	}
}
