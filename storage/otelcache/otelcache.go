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
	in, err := newInstruments(meter)
	if err != nil {
		return nil, err
	}

	attrSet := metric.WithAttributes(attrs...)
	return meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		in.observe(observer, o.Stat(), attrSet)
		return nil
	}, in.list()...)
}

type instruments struct {
	sizeBytes  metric.Int64ObservableGauge
	limitBytes metric.Int64ObservableGauge
	entries    metric.Int64ObservableGauge
	hits       metric.Int64ObservableCounter
	misses     metric.Int64ObservableCounter
	evictions  metric.Int64ObservableCounter
}

func newInstruments(meter metric.Meter) (instruments, error) {
	var in instruments
	var err error
	if in.sizeBytes, err = meter.Int64ObservableGauge("storage.cache.size.bytes",
		metric.WithDescription("Current cache size in bytes."),
		metric.WithUnit("By")); err != nil {
		return in, err
	}
	if in.limitBytes, err = meter.Int64ObservableGauge("storage.cache.limit.bytes",
		metric.WithDescription("Configured cache size limit in bytes."),
		metric.WithUnit("By")); err != nil {
		return in, err
	}
	if in.entries, err = meter.Int64ObservableGauge("storage.cache.entries",
		metric.WithDescription("Current number of cached entries."),
		metric.WithUnit("{entry}")); err != nil {
		return in, err
	}
	if in.hits, err = meter.Int64ObservableCounter("storage.cache.hits",
		metric.WithDescription("Total number of cache hits."),
		metric.WithUnit("{hits}")); err != nil {
		return in, err
	}
	if in.misses, err = meter.Int64ObservableCounter("storage.cache.misses",
		metric.WithDescription("Total number of cache misses."),
		metric.WithUnit("{misses}")); err != nil {
		return in, err
	}
	if in.evictions, err = meter.Int64ObservableCounter("storage.cache.evictions",
		metric.WithDescription("Total number of cache evictions."),
		metric.WithUnit("{evictions}")); err != nil {
		return in, err
	}
	return in, nil
}

func (in instruments) observe(o metric.Observer, s storage.CacheStat, attrs metric.ObserveOption) {
	o.ObserveInt64(in.sizeBytes, s.Size, attrs)
	o.ObserveInt64(in.limitBytes, s.Limit, attrs)
	o.ObserveInt64(in.entries, s.Entries, attrs)
	o.ObserveInt64(in.hits, s.Hits, attrs)
	o.ObserveInt64(in.misses, s.Misses, attrs)
	o.ObserveInt64(in.evictions, s.Evictions, attrs)
}

func (in instruments) list() []metric.Observable {
	return []metric.Observable{
		in.sizeBytes,
		in.limitBytes,
		in.entries,
		in.hits,
		in.misses,
		in.evictions,
	}
}
