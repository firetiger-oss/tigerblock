package storage_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestCacheMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	cache := newTestCache(storage.CacheMeterProvider(provider))
	exerciseBucket(t, cache.AdaptBucket(memory.NewBucket()))
	assertCacheMetrics(t, reader, cache)
}

func TestCacheMetricsWithoutProvider(t *testing.T) {
	cache := newTestCache()
	exerciseBucket(t, cache.AdaptBucket(memory.NewBucket()))

	objectsStat, infosStat, pagesStat := cache.Stat()
	if objectsStat == (storage.CacheStat{}) {
		t.Fatal("expected object cache stats without metrics provider")
	}
	if infosStat == (storage.CacheStat{}) {
		t.Fatal("expected info cache stats without metrics provider")
	}
	if pagesStat == (storage.CacheStat{}) {
		t.Fatal("expected page cache stats without metrics provider")
	}
}

func newTestCache(options ...storage.CacheOption) *storage.Cache {
	options = append([]storage.CacheOption{
		storage.ObjectCacheSize(1024),
		storage.ObjectInfoCacheSize(1024),
		storage.ObjectPageCacheSize(1024),
		storage.CachePageSize(4),
	}, options...)
	return storage.NewCache(options...)
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

	fullObject, _, err := bucket.GetObject(ctx, "test")
	if err != nil {
		t.Fatalf("GetObject miss failed: %v", err)
	}
	if _, err := io.ReadAll(fullObject); err != nil {
		t.Fatalf("reading full object miss: %v", err)
	}
	fullObject.Close()

	fullObject, _, err = bucket.GetObject(ctx, "test")
	if err != nil {
		t.Fatalf("GetObject hit failed: %v", err)
	}
	if _, err := io.ReadAll(fullObject); err != nil {
		t.Fatalf("reading full object hit: %v", err)
	}
	fullObject.Close()

	pageObject, _, err := bucket.GetObject(ctx, "test", storage.BytesRange(0, 3))
	if err != nil {
		t.Fatalf("GetObject page miss failed: %v", err)
	}
	if _, err := io.ReadAll(pageObject); err != nil {
		t.Fatalf("reading page miss: %v", err)
	}
	pageObject.Close()

	pageObject, _, err = bucket.GetObject(ctx, "test", storage.BytesRange(0, 3))
	if err != nil {
		t.Fatalf("GetObject page hit failed: %v", err)
	}
	if _, err := io.ReadAll(pageObject); err != nil {
		t.Fatalf("reading page hit: %v", err)
	}
	pageObject.Close()
}

func assertCacheMetrics(t *testing.T, reader *sdkmetric.ManualReader, cache *storage.Cache) {
	t.Helper()
	objectsStat, infosStat, pagesStat := cache.Stat()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}

	metrics := collectInt64Metrics(t, rm)

	assertMetricValue(t, metrics, "storage.cache.size.bytes", "objects", objectsStat.Size)
	assertMetricValue(t, metrics, "storage.cache.size.bytes", "infos", infosStat.Size)
	assertMetricValue(t, metrics, "storage.cache.size.bytes", "pages", pagesStat.Size)

	assertMetricValue(t, metrics, "storage.cache.limit.bytes", "objects", objectsStat.Limit)
	assertMetricValue(t, metrics, "storage.cache.limit.bytes", "infos", infosStat.Limit)
	assertMetricValue(t, metrics, "storage.cache.limit.bytes", "pages", pagesStat.Limit)

	assertMetricValue(t, metrics, "storage.cache.entries", "objects", objectsStat.Entries)
	assertMetricValue(t, metrics, "storage.cache.entries", "infos", infosStat.Entries)
	assertMetricValue(t, metrics, "storage.cache.entries", "pages", pagesStat.Entries)

	assertMetricValue(t, metrics, "storage.cache.hits", "objects", objectsStat.Hits)
	assertMetricValue(t, metrics, "storage.cache.hits", "infos", infosStat.Hits)
	assertMetricValue(t, metrics, "storage.cache.hits", "pages", pagesStat.Hits)

	assertMetricValue(t, metrics, "storage.cache.misses", "objects", objectsStat.Misses)
	assertMetricValue(t, metrics, "storage.cache.misses", "infos", infosStat.Misses)
	assertMetricValue(t, metrics, "storage.cache.misses", "pages", pagesStat.Misses)

	assertMetricValue(t, metrics, "storage.cache.evictions", "objects", objectsStat.Evictions)
	assertMetricValue(t, metrics, "storage.cache.evictions", "infos", infosStat.Evictions)
	assertMetricValue(t, metrics, "storage.cache.evictions", "pages", pagesStat.Evictions)
}

type observedMetric struct {
	value   int64
	cacheID string
}

func collectInt64Metrics(t *testing.T, rm metricdata.ResourceMetrics) map[string]map[string]observedMetric {
	t.Helper()

	metrics := make(map[string]map[string]observedMetric)
	for _, scopeMetrics := range rm.ScopeMetrics {
		for _, m := range scopeMetrics.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					recordMetric(t, metrics, m.Name, point.Attributes, point.Value)
				}
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					recordMetric(t, metrics, m.Name, point.Attributes, point.Value)
				}
			}
		}
	}
	return metrics
}

func recordMetric(t *testing.T, metrics map[string]map[string]observedMetric, name string, attrs attribute.Set, value int64) {
	t.Helper()

	kindValue, ok := attrs.Value(attribute.Key("storage.cache.kind"))
	if !ok {
		t.Fatalf("metric %q missing storage.cache.kind attribute", name)
	}
	cacheIDValue, ok := attrs.Value(attribute.Key("storage.cache.id"))
	if !ok {
		t.Fatalf("metric %q missing storage.cache.id attribute", name)
	}
	kind := kindValue.AsString()
	cacheID := cacheIDValue.AsString()

	if _, ok := metrics[name]; !ok {
		metrics[name] = make(map[string]observedMetric)
	}
	metrics[name][kind] = observedMetric{value: value, cacheID: cacheID}
}

func assertMetricValue(t *testing.T, metrics map[string]map[string]observedMetric, name, kind string, want int64) {
	t.Helper()

	kindMetrics, ok := metrics[name]
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	got, ok := kindMetrics[kind]
	if !ok {
		t.Fatalf("metric %q missing kind %q", name, kind)
	}
	if got.cacheID == "" {
		t.Fatalf("metric %q kind %q missing cache id", name, kind)
	}
	if got.value != want {
		t.Fatalf("metric %q kind %q = %d, want %d", name, kind, got.value, want)
	}
}
