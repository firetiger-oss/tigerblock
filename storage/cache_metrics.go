package storage

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"weak"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/firetiger-oss/tigerblock/storage"

type cacheMetrics struct {
	sizeBytes  metric.Int64ObservableGauge
	limitBytes metric.Int64ObservableGauge
	entries    metric.Int64ObservableGauge
	hits       metric.Int64ObservableCounter
	misses     metric.Int64ObservableCounter
	evictions  metric.Int64ObservableCounter
}

type cacheMetricKind struct {
	name string
	stat func(*Cache) CacheStat
}

func registerCacheMetrics(c *Cache, meterProvider metric.MeterProvider) {
	if meterProvider == nil {
		return
	}

	meter := meterProvider.Meter(instrumentationName)
	metrics, err := newCacheMetrics(meter)
	if err != nil {
		slog.Error("registering cache metrics", "error", err)
		return
	}

	cacheID := fmt.Sprintf("%p", c)
	cacheRef := weak.Make(c)
	kinds := [...]cacheMetricKind{
		{
			name: "objects",
			stat: func(cache *Cache) CacheStat {
				objects, _, _ := cache.Stat()
				return objects
			},
		},
		{
			name: "infos",
			stat: func(cache *Cache) CacheStat {
				_, infos, _ := cache.Stat()
				return infos
			},
		},
		{
			name: "pages",
			stat: func(cache *Cache) CacheStat {
				_, _, pages := cache.Stat()
				return pages
			},
		},
	}

	registration, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		cache := cacheRef.Value()
		if cache == nil {
			return nil
		}
		for _, kind := range kinds {
			stat := kind.stat(cache)
			attrs := metric.WithAttributes(
				attribute.String("storage.cache.id", cacheID),
				attribute.String("storage.cache.kind", kind.name),
			)
			observer.ObserveInt64(metrics.sizeBytes, stat.Size, attrs)
			observer.ObserveInt64(metrics.limitBytes, stat.Limit, attrs)
			observer.ObserveInt64(metrics.entries, stat.Entries, attrs)
			observer.ObserveInt64(metrics.hits, stat.Hits, attrs)
			observer.ObserveInt64(metrics.misses, stat.Misses, attrs)
			observer.ObserveInt64(metrics.evictions, stat.Evictions, attrs)
		}
		return nil
	}, metrics.sizeBytes, metrics.limitBytes, metrics.entries, metrics.hits, metrics.misses, metrics.evictions)
	if err != nil {
		slog.Error("registering cache metrics callback", "error", err)
		return
	}

	c.metricsRegistration = registration
	runtime.AddCleanup(c, unregisterCacheMetrics, registration)
}

func unregisterCacheMetrics(registration metric.Registration) {
	if err := registration.Unregister(); err != nil {
		slog.Error("unregistering cache metrics callback", "error", err)
	}
}

func newCacheMetrics(meter metric.Meter) (cacheMetrics, error) {
	sizeBytes, err := meter.Int64ObservableGauge("storage.cache.size.bytes",
		metric.WithDescription("Current cache size in bytes."),
		metric.WithUnit("By"))
	if err != nil {
		return cacheMetrics{}, err
	}
	limitBytes, err := meter.Int64ObservableGauge("storage.cache.limit.bytes",
		metric.WithDescription("Configured cache size limit in bytes."),
		metric.WithUnit("By"))
	if err != nil {
		return cacheMetrics{}, err
	}
	entries, err := meter.Int64ObservableGauge("storage.cache.entries",
		metric.WithDescription("Current number of cached entries."),
		metric.WithUnit("{entry}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	hits, err := meter.Int64ObservableCounter("storage.cache.hits",
		metric.WithDescription("Total number of cache hits."),
		metric.WithUnit("{hits}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	misses, err := meter.Int64ObservableCounter("storage.cache.misses",
		metric.WithDescription("Total number of cache misses."),
		metric.WithUnit("{misses}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	evictions, err := meter.Int64ObservableCounter("storage.cache.evictions",
		metric.WithDescription("Total number of cache evictions."),
		metric.WithUnit("{evictions}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	return cacheMetrics{
		sizeBytes:  sizeBytes,
		limitBytes: limitBytes,
		entries:    entries,
		hits:       hits,
		misses:     misses,
		evictions:  evictions,
	}, nil
}
