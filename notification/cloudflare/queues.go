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
	"net/http"
	"time"

	"github.com/firetiger-oss/storage/notification"
	"github.com/firetiger-oss/storage/uri"
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
	unified, err := eventFromR2Event(event)
	if err != nil {
		return err
	}
	return h.objectHandler.HandleEvents(ctx, unified)
}

// eventFromR2Event converts an R2Event to a unified notification.Event.
func eventFromR2Event(event R2Event) (*notification.Event, error) {
	unified := &notification.Event{
		Object: uri.Join("r2", event.Bucket, event.Object.Key),
		Size:   event.Object.Size,
		ETag:   event.Object.ETag,
		Time:   event.EventTime,
	}

	switch event.Action {
	case "PutObject", "CopyObject", "CompleteMultipartUpload":
		unified.Type = notification.ObjectCreated
	case "DeleteObject", "LifecycleDeletion":
		unified.Type = notification.ObjectDeleted
	default:
		return nil, fmt.Errorf("%w: unsupported R2 action %q",
			notification.ErrInvalidEvent, event.Action)
	}

	return unified, nil
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

// NewBatchQueuesHandler creates an http.Handler that receives batches of R2 events
// and forwards them as a single batch to the ObjectHandler.
//
// The expected request body is an array of R2Event objects. All events are converted
// and delivered as a single HandleEvents call.
func NewBatchQueuesHandler(objectHandler notification.ObjectHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var r2Events []R2Event
		if err := json.NewDecoder(r.Body).Decode(&r2Events); err != nil {
			http.Error(w, "failed to parse events: "+err.Error(), http.StatusBadRequest)
			return
		}

		events := make([]*notification.Event, 0, len(r2Events))
		for _, r2Event := range r2Events {
			unified, err := eventFromR2Event(r2Event)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			events = append(events, unified)
		}

		if err := objectHandler.HandleEvents(r.Context(), events...); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})
}
