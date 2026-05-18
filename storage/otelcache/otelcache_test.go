package otelcache_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/otelcache"
)

type stubCache struct {
	stat  storage.CacheStat
	calls int
}

func (s *stubCache) Stat() storage.CacheStat {
	s.calls++
	return s.stat
}

func TestRegister(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	c := &stubCache{stat: storage.CacheStat{
		Limit:     1 << 20,
		Entries:   3,
		Size:      4096,
		Hits:      11,
		Misses:    5,
		Evictions: 2,
	}}

	reg, err := otelcache.Register(provider, c,
		attribute.String("storage.cache.kind", "file"),
		attribute.String("storage.cache.id", "test-cache"),
	)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	t.Cleanup(func() {
		_ = reg.Unregister()
	})

	got := collect(t, reader)

	assertValue(t, got, "storage.cache.size.bytes", c.stat.Size)
	assertValue(t, got, "storage.cache.limit.bytes", c.stat.Limit)
	assertValue(t, got, "storage.cache.entries", c.stat.Entries)
	assertValue(t, got, "storage.cache.hits", c.stat.Hits)
	assertValue(t, got, "storage.cache.misses", c.stat.Misses)
	assertValue(t, got, "storage.cache.evictions", c.stat.Evictions)

	for name, observed := range got {
		if v, ok := observed.attrs.Value("storage.cache.kind"); !ok || v.AsString() != "file" {
			t.Fatalf("metric %q missing or wrong storage.cache.kind attribute: %v", name, v)
		}
		if v, ok := observed.attrs.Value("storage.cache.id"); !ok || v.AsString() != "test-cache" {
			t.Fatalf("metric %q missing or wrong storage.cache.id attribute: %v", name, v)
		}
	}
}

func TestRegisterReflectsLiveStatChanges(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	c := &stubCache{}
	reg, err := otelcache.Register(provider, c)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	t.Cleanup(func() {
		_ = reg.Unregister()
	})

	c.stat = storage.CacheStat{Hits: 1}
	if got := collect(t, reader)["storage.cache.hits"].value; got != 1 {
		t.Fatalf("first collect: hits = %d, want 1", got)
	}

	c.stat = storage.CacheStat{Hits: 7, Misses: 2}
	got := collect(t, reader)
	if got["storage.cache.hits"].value != 7 {
		t.Fatalf("second collect: hits = %d, want 7", got["storage.cache.hits"].value)
	}
	if got["storage.cache.misses"].value != 2 {
		t.Fatalf("second collect: misses = %d, want 2", got["storage.cache.misses"].value)
	}
}

func TestUnregisterStopsObservation(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	c := &stubCache{stat: storage.CacheStat{Hits: 1}}
	reg, err := otelcache.Register(provider, c)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	collect(t, reader)
	if c.calls != 1 {
		t.Fatalf("Stat called %d times before unregister, want 1", c.calls)
	}

	if err := reg.Unregister(); err != nil {
		t.Fatalf("Unregister failed: %v", err)
	}

	collect(t, reader)
	if c.calls != 1 {
		t.Fatalf("Stat called %d times after unregister, want 1", c.calls)
	}
}

type observed struct {
	value int64
	attrs attribute.Set
}

func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]observed {
	t.Helper()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}

	out := make(map[string]observed)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, p := range d.DataPoints {
					out[m.Name] = observed{value: p.Value, attrs: p.Attributes}
				}
			case metricdata.Sum[int64]:
				for _, p := range d.DataPoints {
					out[m.Name] = observed{value: p.Value, attrs: p.Attributes}
				}
			}
		}
	}
	return out
}

func assertValue(t *testing.T, got map[string]observed, name string, want int64) {
	t.Helper()
	m, ok := got[name]
	if !ok {
		t.Fatalf("metric %q not found", name)
	}
	if m.value != want {
		t.Fatalf("metric %q = %d, want %d", name, m.value, want)
	}
}
