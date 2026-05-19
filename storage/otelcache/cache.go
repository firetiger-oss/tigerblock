package otelcache

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/firetiger-oss/tigerblock/storage"
)

// RegisterCache wires OpenTelemetry observable instruments for all three
// sub-caches of a storage.Cache (objects, infos, pages) under a single
// callback. Each sub-cache is reported with a `storage.cache.kind` attribute
// set to its name; additional attributes supplied by the caller are added to
// every observation.
//
// The returned Registration must be retained by the caller; its Unregister
// method removes the callback.
func RegisterCache(provider metric.MeterProvider, c *storage.Cache, attrs ...attribute.KeyValue) (metric.Registration, error) {
	meter := provider.Meter(instrumentationName)
	in, err := newInstruments(meter)
	if err != nil {
		return nil, err
	}

	objectsAttrs := kindAttrs(attrs, "objects")
	infosAttrs := kindAttrs(attrs, "infos")
	pagesAttrs := kindAttrs(attrs, "pages")

	return meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		// Snapshot once per callback so the three kinds report a coherent
		// view; calling Stat() per kind would let objects/infos/pages drift
		// within a single collection.
		objects, infos, pages := c.Stat()
		in.observe(observer, objects, objectsAttrs)
		in.observe(observer, infos, infosAttrs)
		in.observe(observer, pages, pagesAttrs)
		return nil
	}, in.list()...)
}

func kindAttrs(extra []attribute.KeyValue, kind string) metric.ObserveOption {
	all := make([]attribute.KeyValue, 0, len(extra)+1)
	all = append(all, extra...)
	all = append(all, attribute.String("storage.cache.kind", kind))
	return metric.WithAttributes(all...)
}
