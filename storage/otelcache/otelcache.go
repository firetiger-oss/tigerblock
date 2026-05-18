// Package otelcache exposes storage.CacheStat values to OpenTelemetry without
// coupling cache implementations to the OTel metric API. Callers register any
// value satisfying Observable against a MeterProvider; the SDK pulls the
// values via an observable-instrument callback on each collection.
package otelcache

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/firetiger-oss/tigerblock/storage"
)

const instrumentationName = "github.com/firetiger-oss/tigerblock/storage/otelcache"

// Observable is implemented by any cache that can report its current
// statistics. Both storage/file.Cache and ad-hoc callers can satisfy it; for
// composite caches that track multiple sub-caches, callers can wrap each
// sub-cache in a small adapter.
type Observable interface {
	Stat() storage.CacheStat
}

// Register installs OpenTelemetry observable instruments on the provided
// MeterProvider that report the current Stat() of o at each metric
// collection. The supplied attributes are attached to every observation,
// allowing callers to distinguish caches (for example,
// attribute.String("storage.cache.kind", "file")).
//
// The returned Registration must be retained by the caller; its Unregister
// method removes the callback.
func Register(provider metric.MeterProvider, o Observable, attrs ...attribute.KeyValue) (metric.Registration, error) {
	meter := provider.Meter(instrumentationName)

	sizeBytes, err := meter.Int64ObservableGauge("storage.cache.size.bytes",
		metric.WithDescription("Current cache size in bytes."),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	limitBytes, err := meter.Int64ObservableGauge("storage.cache.limit.bytes",
		metric.WithDescription("Configured cache size limit in bytes."),
		metric.WithUnit("By"))
	if err != nil {
		return nil, err
	}
	entries, err := meter.Int64ObservableGauge("storage.cache.entries",
		metric.WithDescription("Current number of cached entries."),
		metric.WithUnit("{entry}"))
	if err != nil {
		return nil, err
	}
	hits, err := meter.Int64ObservableCounter("storage.cache.hits",
		metric.WithDescription("Total number of cache hits."),
		metric.WithUnit("{hits}"))
	if err != nil {
		return nil, err
	}
	misses, err := meter.Int64ObservableCounter("storage.cache.misses",
		metric.WithDescription("Total number of cache misses."),
		metric.WithUnit("{misses}"))
	if err != nil {
		return nil, err
	}
	evictions, err := meter.Int64ObservableCounter("storage.cache.evictions",
		metric.WithDescription("Total number of cache evictions."),
		metric.WithUnit("{evictions}"))
	if err != nil {
		return nil, err
	}

	attrSet := metric.WithAttributes(attrs...)
	return meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		s := o.Stat()
		observer.ObserveInt64(sizeBytes, s.Size, attrSet)
		observer.ObserveInt64(limitBytes, s.Limit, attrSet)
		observer.ObserveInt64(entries, s.Entries, attrSet)
		observer.ObserveInt64(hits, s.Hits, attrSet)
		observer.ObserveInt64(misses, s.Misses, attrSet)
		observer.ObserveInt64(evictions, s.Evictions, attrSet)
		return nil
	}, sizeBytes, limitBytes, entries, hits, misses, evictions)
}
