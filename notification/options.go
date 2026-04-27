package notification

import (
	"iter"
	"slices"

	"github.com/firetiger-oss/tigerblock/storage"
)

// Option is a functional option for configuring an ObjectHandler.
type Option func(*Options)

// Options contains configuration for an ObjectHandler.
type Options struct {
	registry              storage.Registry
	filters               []Filter
	deleteAfterProcessing bool
}

// Registry returns the storage registry to use for loading buckets.
// If not set, returns the default global registry.
func (o *Options) Registry() storage.Registry {
	if o.registry == nil {
		return storage.DefaultRegistry()
	}
	return o.registry
}

// Filters returns the list of filters to apply before processing events.
func (o *Options) Filters() []Filter {
	return o.filters
}

// DeleteAfterProcessing returns whether to delete objects after successful processing.
func (o *Options) DeleteAfterProcessing() bool {
	return o.deleteAfterProcessing
}

// NewOptions creates Options from a variadic list of Option functions.
func NewOptions(options ...Option) *Options {
	return newOptions(slices.Values(options))
}

func newOptions[Option ~func(*Options)](options iter.Seq[Option]) *Options {
	opts := new(Options)
	for option := range options {
		option(opts)
	}
	return opts
}

// WithRegistry sets a custom storage registry for loading buckets.
// If not specified, the default global registry is used.
func WithRegistry(registry storage.Registry) Option {
	return func(o *Options) {
		o.registry = registry
	}
}

// WithFilter adds a filter that runs before GetObject.
// Filters can skip events or return errors before object content is fetched.
// Multiple filters are applied in order; all must pass for event to be processed.
func WithFilter(filter Filter) Option {
	return func(o *Options) {
		o.filters = append(o.filters, filter)
	}
}

// WithDeleteAfterProcessing controls automatic deletion of objects after
// successful processing. Only applies to ObjectCreated events.
// Object deletion failures are logged but do not fail the request.
func WithDeleteAfterProcessing(enabled bool) Option {
	return func(o *Options) {
		o.deleteAfterProcessing = enabled
	}
}
