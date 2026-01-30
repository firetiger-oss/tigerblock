// Package cloudflare provides handlers for Cloudflare R2 event notifications
// delivered via Cloudflare Queues.
//
// R2 bucket event notifications are delivered to Cloudflare Queues. This package
// provides an HTTP handler that receives events forwarded from a Cloudflare Worker
// that consumes from the queue.
//
// Example Worker code (TypeScript):
//
//	export default {
//	  async queue(batch: MessageBatch<R2EventMessage>, env: Env): Promise<void> {
//	    for (const message of batch.messages) {
//	      await fetch(env.WEBHOOK_URL, {
//	        method: "POST",
//	        headers: { "Content-Type": "application/json" },
//	        body: JSON.stringify(message.body),
//	      });
//	      message.ack();
//	    }
//	  },
//	};
package cloudflare

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"net/http"
	"time"

	"github.com/firetiger-oss/storage/concurrent"
	"github.com/firetiger-oss/storage/notification"
)

// R2Event represents an R2 bucket event notification.
// See: https://developers.cloudflare.com/r2/buckets/event-notifications/
type R2Event struct {
	// Account is the Cloudflare account ID.
	Account string `json:"account"`

	// Action is the type of operation that triggered the event.
	// Values: "PutObject", "CopyObject", "CompleteMultipartUpload",
	// "DeleteObject", "LifecycleDeletion"
	Action string `json:"action"`

	// Bucket is the name of the R2 bucket.
	Bucket string `json:"bucket"`

	// Object contains information about the affected object.
	Object R2Object `json:"object"`

	// EventTime is when the event occurred (RFC3339 format).
	EventTime time.Time `json:"eventTime"`

	// CopySource contains the source object info for CopyObject actions.
	CopySource *R2CopySource `json:"copySource,omitempty"`
}

// R2Object contains information about the object in an R2 event.
type R2Object struct {
	// Key is the object key (path).
	Key string `json:"key"`

	// Size is the object size in bytes.
	Size int64 `json:"size,omitempty"`

	// ETag is the object's entity tag.
	ETag string `json:"eTag,omitempty"`
}

// R2CopySource contains information about the source object in a CopyObject event.
type R2CopySource struct {
	// Bucket is the source bucket name.
	Bucket string `json:"bucket"`

	// Object is the source object key.
	Object string `json:"object"`
}

// R2EventHandler handles R2 bucket notifications and converts them to unified events.
type R2EventHandler struct {
	objectHandler notification.ObjectHandler
}

// NewR2EventHandler creates a handler for R2 bucket notifications.
func NewR2EventHandler(objectHandler notification.ObjectHandler) *R2EventHandler {
	return &R2EventHandler{
		objectHandler: objectHandler,
	}
}

// Handle processes an R2 event and forwards it to the object handler.
func (h *R2EventHandler) Handle(ctx context.Context, event R2Event) error {
	// Build unified event
	unified := notification.Event{
		Bucket: event.Bucket,
		Key:    event.Object.Key,
		Size:   event.Object.Size,
		ETag:   event.Object.ETag,
		Time:   event.EventTime,
		Source: "cloudflare:r2",
	}

	// Determine event type from action
	switch event.Action {
	case "PutObject", "CopyObject", "CompleteMultipartUpload":
		unified.Type = notification.ObjectCreated
	case "DeleteObject", "LifecycleDeletion":
		unified.Type = notification.ObjectDeleted
	default:
		return fmt.Errorf("%w: unsupported R2 action %q",
			notification.ErrInvalidEvent, event.Action)
	}

	return h.objectHandler.HandleEvent(ctx, &unified)
}

// NewQueuesHandler creates an http.Handler that receives R2 bucket notifications
// forwarded from a Cloudflare Worker consuming from a Queue.
//
// This handler is suitable for use as a webhook endpoint that a Worker POSTs to.
//
// The expected request body is a single R2Event JSON object:
//
//	{
//	  "account": "account-id",
//	  "action": "PutObject",
//	  "bucket": "my-bucket",
//	  "object": { "key": "path/to/file", "size": 1234, "eTag": "..." },
//	  "eventTime": "2024-05-24T19:36:44.379Z"
//	}
func NewQueuesHandler(objectHandler notification.ObjectHandler) http.Handler {
	handler := NewR2EventHandler(objectHandler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var event R2Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			http.Error(w, "failed to parse event: "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := handler.Handle(r.Context(), event); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
}

// NewBatchQueuesHandler creates an http.Handler that receives batches of R2 events.
//
// This is useful when the Worker forwards multiple events in a single request
// to reduce HTTP overhead. The expected request body is an array of R2Event objects.
//
// Events are processed concurrently using the context's concurrency limit.
// Use concurrent.WithLimit to control parallelism.
func NewBatchQueuesHandler(objectHandler notification.ObjectHandler) http.Handler {
	handler := NewR2EventHandler(objectHandler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		events := decodeEventArray(json.NewDecoder(r.Body))

		for _, err := range concurrent.Pipeline(r.Context(), events,
			func(ctx context.Context, event R2Event) (struct{}, error) {
				return struct{}{}, handler.Handle(ctx, event)
			},
		) {
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	})
}

// decodeEventArray streams R2Event objects from a JSON array using a decoder.
func decodeEventArray(dec *json.Decoder) iter.Seq2[R2Event, error] {
	return func(yield func(R2Event, error) bool) {
		token, err := dec.Token()
		if err != nil {
			yield(R2Event{}, fmt.Errorf("failed to read opening bracket: %w", err))
			return
		}
		if delim, ok := token.(json.Delim); !ok || delim != '[' {
			yield(R2Event{}, fmt.Errorf("expected '[', got %v", token))
			return
		}

		for dec.More() {
			var event R2Event
			if err := dec.Decode(&event); err != nil {
				yield(R2Event{}, fmt.Errorf("failed to decode event: %w", err))
				return
			}
			if !yield(event, nil) {
				return
			}
		}

		token, err = dec.Token()
		if err != nil {
			yield(R2Event{}, fmt.Errorf("failed to read closing bracket: %w", err))
			return
		}
		if delim, ok := token.(json.Delim); !ok || delim != ']' {
			yield(R2Event{}, fmt.Errorf("expected ']', got %v", token))
			return
		}
	}
}
