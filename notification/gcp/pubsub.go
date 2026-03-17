package gcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/firetiger-oss/storage/notification"
	"github.com/firetiger-oss/storage/uri"
)

// PubSubPushRequest represents a Pub/Sub push delivery request.
type PubSubPushRequest struct {
	Message      PubSubMessage `json:"message"`
	Subscription string        `json:"subscription"`
}

// PubSubMessage represents a Pub/Sub message.
type PubSubMessage struct {
	Data        string            `json:"data"`
	Attributes  map[string]string `json:"attributes"`
	MessageID   string            `json:"messageId"`
	PublishTime string            `json:"publishTime"`
}

// ObjectMetadata represents the JSON payload for bucket notification data.
type ObjectMetadata struct {
	Kind         string `json:"kind"`
	ID           string `json:"id"`
	SelfLink     string `json:"selfLink"`
	Name         string `json:"name"`
	Bucket       string `json:"bucket"`
	Generation   string `json:"generation"`
	ContentType  string `json:"contentType,omitempty"`
	Size         string `json:"size,omitempty"`
	ETag         string `json:"etag,omitempty"`
	TimeCreated  string `json:"timeCreated,omitempty"`
	Updated      string `json:"updated,omitempty"`
	StorageClass string `json:"storageClass,omitempty"`
}

// BucketNotificationHandler handles GCS Pub/Sub bucket notifications.
type BucketNotificationHandler struct {
	objectHandler notification.ObjectHandler
}

// NewBucketNotificationHandler creates a handler for GCS bucket notifications via Pub/Sub.
func NewBucketNotificationHandler(objectHandler notification.ObjectHandler) *BucketNotificationHandler {
	return &BucketNotificationHandler{
		objectHandler: objectHandler,
	}
}

// Handle processes a Pub/Sub message containing a GCS bucket notification.
func (h *BucketNotificationHandler) Handle(ctx context.Context, msg PubSubMessage) error {
	// Extract attributes
	bucketID := msg.Attributes["bucketId"]
	objectID := msg.Attributes["objectId"]
	eventType := msg.Attributes["eventType"]

	if bucketID == "" || objectID == "" {
		return fmt.Errorf("%w: missing bucketId or objectId in attributes",
			notification.ErrInvalidEvent)
	}

	// Build unified event
	unified := notification.Event{
		Object: uri.Join("gs", bucketID, objectID),
	}

	// Parse publish time
	if t, err := time.Parse(time.RFC3339, msg.PublishTime); err == nil {
		unified.Time = t
	}

	// Determine event type
	switch eventType {
	case "OBJECT_FINALIZE":
		unified.Type = notification.ObjectCreated
	case "OBJECT_DELETE", "OBJECT_ARCHIVE":
		unified.Type = notification.ObjectDeleted
	default:
		return fmt.Errorf("%w: unsupported GCS event type %q",
			notification.ErrInvalidEvent, eventType)
	}

	// Parse object metadata from data payload if available
	if msg.Data != "" {
		data, err := base64.StdEncoding.DecodeString(msg.Data)
		if err == nil {
			var metadata ObjectMetadata
			if json.Unmarshal(data, &metadata) == nil {
				if metadata.Size != "" {
					if size, err := strconv.ParseInt(metadata.Size, 10, 64); err == nil {
						unified.Size = size
					}
				}
				unified.ETag = metadata.ETag
			}
		}
	}

	return h.objectHandler.HandleEvents(ctx, &unified)
}

// NewPubSubHandler creates an http.Handler that receives GCS bucket notifications
// via Pub/Sub push delivery and forwards them to an ObjectHandler.
//
// This handler is suitable for use as a Pub/Sub push endpoint.
func NewPubSubHandler(objectHandler notification.ObjectHandler) http.Handler {
	handler := NewBucketNotificationHandler(objectHandler)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request body", http.StatusBadRequest)
			return
		}

		var pushReq PubSubPushRequest
		if err := json.Unmarshal(body, &pushReq); err != nil {
			http.Error(w, "failed to parse push request: "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := handler.Handle(r.Context(), pushReq.Message); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Pub/Sub expects 2xx response to acknowledge the message
		w.WriteHeader(http.StatusOK)
	})
}
