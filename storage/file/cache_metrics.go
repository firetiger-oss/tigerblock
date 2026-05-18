package file

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"weak"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/firetiger-oss/tigerblock/storage/file"

type cacheMetrics struct {
	sizeBytes          metric.Int64ObservableGauge
	limitBytes         metric.Int64ObservableGauge
	entries            metric.Int64ObservableGauge
	inFlightBytes      metric.Int64ObservableGauge
	hits               metric.Int64ObservableCounter
	misses             metric.Int64ObservableCounter
	evictions          metric.Int64ObservableCounter
	evictUntilFitsRuns metric.Int64ObservableCounter
	evictForSpaceRuns  metric.Int64ObservableCounter
	writeErrors        metric.Int64ObservableCounter
}

func registerCacheMetrics(c *Cache, meterProvider metric.MeterProvider) {
	meter := meterProvider.Meter(instrumentationName)
	metrics, err := newCacheMetrics(meter)
	if err != nil {
		slog.Error("registering file cache metrics", "error", err)
		return
	}

	cacheID := fmt.Sprintf("%p", c)
	cacheRef := weak.Make(c)

	registration, err := meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		cache := cacheRef.Value()
		if cache == nil {
			return nil
		}
		stat := cache.Stat()
		attrs := metric.WithAttributes(attribute.String("storage.cache.id", cacheID))
		observer.ObserveInt64(metrics.sizeBytes, stat.Size, attrs)
		observer.ObserveInt64(metrics.limitBytes, stat.Limit, attrs)
		observer.ObserveInt64(metrics.entries, stat.Entries, attrs)
		observer.ObserveInt64(metrics.inFlightBytes, cache.inFlight.Load(), attrs)
		observer.ObserveInt64(metrics.hits, stat.Hits, attrs)
		observer.ObserveInt64(metrics.misses, stat.Misses, attrs)
		observer.ObserveInt64(metrics.evictions, stat.Evictions, attrs)
		observer.ObserveInt64(metrics.evictUntilFitsRuns, cache.evictUntilFitsCount.Load(), attrs)
		observer.ObserveInt64(metrics.evictForSpaceRuns, cache.evictForSpaceCount.Load(), attrs)
		observer.ObserveInt64(metrics.writeErrors, cache.writeErrorsENOSPC.Load(),
			metric.WithAttributes(
				attribute.String("storage.cache.id", cacheID),
				attribute.String("kind", "enospc"),
			))
		observer.ObserveInt64(metrics.writeErrors, cache.writeErrorsOther.Load(),
			metric.WithAttributes(
				attribute.String("storage.cache.id", cacheID),
				attribute.String("kind", "other"),
			))
		return nil
	},
		metrics.sizeBytes, metrics.limitBytes, metrics.entries, metrics.inFlightBytes,
		metrics.hits, metrics.misses, metrics.evictions,
		metrics.evictUntilFitsRuns, metrics.evictForSpaceRuns, metrics.writeErrors,
	)
	if err != nil {
		slog.Error("registering file cache metrics callback", "error", err)
		return
	}

	c.metricsRegistration = registration
	runtime.AddCleanup(c, unregisterCacheMetrics, registration)
}

func unregisterCacheMetrics(registration metric.Registration) {
	if err := registration.Unregister(); err != nil {
		slog.Error("unregistering file cache metrics callback", "error", err)
	}
}

func newCacheMetrics(meter metric.Meter) (cacheMetrics, error) {
	sizeBytes, err := meter.Int64ObservableGauge("storage.file_cache.size.bytes",
		metric.WithDescription("Current on-disk cache size in bytes (committed LRU entries)."),
		metric.WithUnit("By"))
	if err != nil {
		return cacheMetrics{}, err
	}
	limitBytes, err := meter.Int64ObservableGauge("storage.file_cache.limit.bytes",
		metric.WithDescription("Configured cache size limit in bytes."),
		metric.WithUnit("By"))
	if err != nil {
		return cacheMetrics{}, err
	}
	entries, err := meter.Int64ObservableGauge("storage.file_cache.entries",
		metric.WithDescription("Current number of cached entries."),
		metric.WithUnit("{entry}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	inFlightBytes, err := meter.Int64ObservableGauge("storage.file_cache.inflight.bytes",
		metric.WithDescription("Bytes for in-progress writes currently charged against the cache limit."),
		metric.WithUnit("By"))
	if err != nil {
		return cacheMetrics{}, err
	}
	hits, err := meter.Int64ObservableCounter("storage.file_cache.hits",
		metric.WithDescription("Total number of cache hits."),
		metric.WithUnit("{hits}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	misses, err := meter.Int64ObservableCounter("storage.file_cache.misses",
		metric.WithDescription("Total number of cache misses."),
		metric.WithUnit("{misses}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	evictions, err := meter.Int64ObservableCounter("storage.file_cache.evictions",
		metric.WithDescription("Total number of LRU evictions (proactive, reactive, and invalidation)."),
		metric.WithUnit("{evictions}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	evictUntilFitsRuns, err := meter.Int64ObservableCounter("storage.file_cache.evict_until_fits.invocations",
		metric.WithDescription("Times the proactive evict-before-write path ran. Non-zero means the in-flight back-pressure introduced in FIRE-2419 is active."),
		metric.WithUnit("{invocations}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	evictForSpaceRuns, err := meter.Int64ObservableCounter("storage.file_cache.evict_for_space.invocations",
		metric.WithDescription("Times the reactive evict-after-ENOSPC path ran. Falling toward zero indicates the proactive path is preventing ENOSPC."),
		metric.WithUnit("{invocations}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	writeErrors, err := meter.Int64ObservableCounter("storage.file_cache.write_errors",
		metric.WithDescription("Cache write failures, labeled by kind (enospc, other)."),
		metric.WithUnit("{errors}"))
	if err != nil {
		return cacheMetrics{}, err
	}
	return cacheMetrics{
		sizeBytes:          sizeBytes,
		limitBytes:         limitBytes,
		entries:            entries,
		inFlightBytes:      inFlightBytes,
		hits:               hits,
		misses:             misses,
		evictions:          evictions,
		evictUntilFitsRuns: evictUntilFitsRuns,
		evictForSpaceRuns:  evictForSpaceRuns,
		writeErrors:        writeErrors,
	}, nil
}
