package notification

import (
	"context"
	"time"
)

// EventType represents the type of storage event.
type EventType string

const (
	// ObjectCreated indicates an object was created or updated.
	ObjectCreated EventType = "ObjectCreated"

	// ObjectDeleted indicates an object was deleted.
	ObjectDeleted EventType = "ObjectDeleted"
)

// Event is a unified storage notification event, abstracting differences
// between S3 EventBridge and GCS Pub/Sub notification formats.
type Event struct {
	// Type indicates whether this is a create or delete event.
	Type EventType

	// Object is the full object URI (e.g., "s3://bucket/key", "gs://bucket/key").
	Object string

	// Size is the object size in bytes (only for create events).
	Size int64

	// ETag is the object's entity tag (only for create events).
	ETag string

	// Time is when the event occurred.
	Time time.Time

	// Region is the cloud region where the event originated.
	Region string
}

// ObjectHandler processes storage notification events.
type ObjectHandler interface {
	// HandleEvents processes one or more storage notification events.
	HandleEvents(ctx context.Context, events ...*Event) error
}

// ObjectHandlerFunc is a function adapter that implements ObjectHandler.
type ObjectHandlerFunc func(context.Context, ...*Event) error

// HandleEvents implements the ObjectHandler interface.
func (f ObjectHandlerFunc) HandleEvents(ctx context.Context, events ...*Event) error {
	return f(ctx, events...)
}
