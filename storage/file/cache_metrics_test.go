package file

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/firetiger-oss/tigerblock/storage"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestCacheMetricsExposeFireFix verifies the OTEL counters that exist to monitor
// the FIRE-2419 rollout. We drive concurrent gets through a small cache to
// guarantee the proactive evict_until_fits path runs, then check that the
// matching counter is observed as non-zero.
func TestCacheMetricsExposeFireFix(t *testing.T) {
	const (
		goroutines  = 20
		objectSize  = 1024
		cacheLimit  = 4 * objectSize
		cacheObject = "public, max-age=3600"
	)

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	underlying, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < goroutines; i++ {
		key := strings.Repeat("k", i+1)
		val := strings.Repeat("v", objectSize)
		if _, err := underlying.PutObject(ctx, key, strings.NewReader(val), storage.CacheControl(cacheObject)); err != nil {
			t.Fatalf("put key %q: %v", key, err)
		}
	}

	cache := NewCache(cacheDir, cacheLimit, WithMeterProvider(provider))
	bucket := cache.AdaptBucket(underlying)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := strings.Repeat("k", i+1)
			r, _, err := bucket.GetObject(ctx, key)
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, r)
			r.Close()
		}(i)
	}
	wg.Wait()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	counters := map[string]int64{}
	gauges := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				for _, pt := range d.DataPoints {
					counters[m.Name] += pt.Value
				}
			case metricdata.Gauge[int64]:
				for _, pt := range d.DataPoints {
					gauges[m.Name] = pt.Value
				}
			}
		}
	}

	if got := counters["storage.file_cache.evict_until_fits.invocations"]; got == 0 {
		t.Errorf("evict_until_fits.invocations = 0, want >0 — proactive eviction path did not run")
	}
	if got := gauges["storage.file_cache.limit.bytes"]; got != cacheLimit {
		t.Errorf("limit.bytes = %d, want %d", got, cacheLimit)
	}
	for _, name := range []string{
		"storage.file_cache.size.bytes",
		"storage.file_cache.entries",
		"storage.file_cache.inflight.bytes",
		"storage.file_cache.hits",
		"storage.file_cache.misses",
		"storage.file_cache.evictions",
		"storage.file_cache.evict_for_space.invocations",
		"storage.file_cache.write_errors",
	} {
		if _, gaugeOk := gauges[name]; gaugeOk {
			continue
		}
		if _, counterOk := counters[name]; counterOk {
			continue
		}
		t.Errorf("metric %q not observed", name)
	}
}

// TestCacheWithoutMeterProvider verifies that the cache works (and counters
// still tick internally) when no MeterProvider option is supplied.
func TestCacheWithoutMeterProvider(t *testing.T) {
	cacheDir := t.TempDir()
	cache := NewCache(cacheDir, 4096)
	if cache.metricsRegistration != nil {
		t.Error("metricsRegistration should be nil without WithMeterProvider")
	}
	cache.evictUntilFits()
	if got := cache.evictUntilFitsCount.Load(); got != 1 {
		t.Errorf("evictUntilFitsCount = %d, want 1 — internal counters should tick regardless of metrics registration", got)
	}
}
