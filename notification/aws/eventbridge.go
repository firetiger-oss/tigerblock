package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/firetiger-oss/concurrent"
	"github.com/firetiger-oss/storage/notification"
	"github.com/firetiger-oss/storage/uri"
)

// EventBridgeEvent represents an AWS EventBridge event envelope.
type EventBridgeEvent[T any] struct {
	Version    string   `json:"version"`
	ID         string   `json:"id"`
	DetailType string   `json:"detail-type"`
	Source     string   `json:"source"`
	Account    string   `json:"account"`
	Time       string   `json:"time"`
	Region     string   `json:"region"`
	Resources  []string `json:"resources"`
	Detail     T        `json:"detail"`
}

// S3EventDetail represents the detail payload for S3 bucket notifications.
type S3EventDetail struct {
	Version         string `json:"version"`
	Bucket          Bucket `json:"bucket"`
	Object          Object `json:"object"`
	RequestID       string `json:"request-id"`
	Requester       string `json:"requester"`
	SourceIPAddress string `json:"source-ip-address"`
	Reason          string `json:"reason,omitempty"`
}

// Bucket contains bucket information from an S3 event.
type Bucket struct {
	Name string `json:"name"`
}

// Object contains object information from an S3 event.
type Object struct {
	Key       string `json:"key"`
	Size      int64  `json:"size,omitempty"`
	ETag      string `json:"etag,omitempty"`
	VersionID string `json:"version-id,omitempty"`
	Sequencer string `json:"sequencer,omitempty"`
}

// S3EventHandler handles S3 events from EventBridge and forwards them to an ObjectHandler.
type S3EventHandler struct {
	objectHandler notification.ObjectHandler
}

// NewS3EventHandler creates a handler for S3 EventBridge notifications.
func NewS3EventHandler(objectHandler notification.ObjectHandler) *S3EventHandler {
	return &S3EventHandler{
		objectHandler: objectHandler,
	}
}

// Handle processes an EventBridge S3 event.
func (h *S3EventHandler) Handle(ctx context.Context, event EventBridgeEvent[S3EventDetail]) error {
	// Convert to unified event format
	unified := notification.Event{
		Object: uri.Join("s3", event.Detail.Bucket.Name, event.Detail.Object.Key),
		Size:   event.Detail.Object.Size,
		ETag:   event.Detail.Object.ETag,
		Region: event.Region,
	}

	// Parse event time
	if t, err := time.Parse(time.RFC3339, event.Time); err == nil {
		unified.Time = t
	}

	// Determine event type from detail-type
	switch {
	case strings.Contains(event.DetailType, "Object Created"):
		unified.Type = notification.ObjectCreated
	case strings.Contains(event.DetailType, "Object Deleted"):
		unified.Type = notification.ObjectDeleted
	default:
		return fmt.Errorf("%w: unsupported S3 event type %q",
			notification.ErrInvalidEvent, event.DetailType)
	}

	return h.objectHandler.HandleEvent(ctx, &unified)
}

// NewS3EventBridgeHandler creates an http.Handler that receives S3 EventBridge
// notifications via HTTP POST and forwards them to an ObjectHandler.
//
// This handler is suitable for use as an EventBridge API destination target
// or behind an API Gateway.
func NewS3EventBridgeHandler(objectHandler notification.ObjectHandler) http.Handler {
	handler := NewS3EventHandler(objectHandler)
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

		var event EventBridgeEvent[S3EventDetail]
		if err := json.Unmarshal(body, &event); err != nil {
			http.Error(w, "failed to parse event: "+err.Error(), http.StatusBadRequest)
			return
		}

		if err := handler.Handle(r.Context(), event); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})
}

// S3 Lambda Event Types
// These types represent S3 events received directly by AWS Lambda functions.
// They are minimal definitions containing only the fields used by this package.

// S3Event represents an S3 event from AWS Lambda.
type S3Event struct {
	Records []S3EventRecord `json:"Records"`
}

// S3EventRecord represents a single record in an S3 event.
type S3EventRecord struct {
	AWSRegion string    `json:"awsRegion"`
	EventTime time.Time `json:"eventTime"`
	EventName string    `json:"eventName"`
	S3        S3Entity  `json:"s3"`
}

// S3Entity contains S3-specific information in an event record.
type S3Entity struct {
	Bucket S3Bucket `json:"bucket"`
	Object S3Object `json:"object"`
}

// S3Bucket contains bucket information from an S3 Lambda event.
type S3Bucket struct {
	Name string `json:"name"`
}

// S3Object contains object information from an S3 Lambda event.
type S3Object struct {
	Key  string `json:"key"`
	Size int64  `json:"size,omitempty"`
	ETag string `json:"eTag"`
}

// S3LambdaHandler handles direct S3 Lambda events and converts them to notification.Event.
// This handler is designed for AWS Lambda functions that receive S3 events directly
// (not through EventBridge).
//
// Usage Example:
//
//	package main
//
//	import (
//	    "context"
//	    "log"
//
//	    "github.com/aws/aws-lambda-go/lambda"
//	    "github.com/firetiger-oss/storage/notification"
//	    "github.com/firetiger-oss/storage/notification/aws"
//	)
//
//	func main() {
//	    objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, event *notification.Event) error {
//	        log.Printf("Processing: %s/%s", event.Bucket, event.Key)
//	        return nil
//	    })
//
//	    handler := aws.NewS3LambdaHandler(objectHandler)
//	    lambda.Start(handler.HandleEvent)
//	}
//
// The handler processes all records concurrently. Each record is converted to
// a notification.Event with Type set based on the EventName prefix:
//   - "s3:ObjectCreated:" → ObjectCreated
//   - "s3:ObjectRemoved:" → ObjectDeleted
type S3LambdaHandler struct {
	objectHandler notification.ObjectHandler
}

// NewS3LambdaHandler creates a handler for direct S3 Lambda notifications.
func NewS3LambdaHandler(objectHandler notification.ObjectHandler) *S3LambdaHandler {
	return &S3LambdaHandler{
		objectHandler: objectHandler,
	}
}

// HandleEvent processes all records in an S3Event concurrently.
func (h *S3LambdaHandler) HandleEvent(ctx context.Context, event S3Event) error {
	return concurrent.RunTasks(ctx, event.Records, h.HandleRecord)
}

// HandleRecord processes a single S3EventRecord and converts it to notification.Event.
func (h *S3LambdaHandler) HandleRecord(ctx context.Context, record S3EventRecord) error {
	// Convert to unified event format
	unified := notification.Event{
		Object: uri.Join("s3", record.S3.Bucket.Name, record.S3.Object.Key),
		Size:   record.S3.Object.Size,
		ETag:   record.S3.Object.ETag,
		Region: record.AWSRegion,
		Time:   record.EventTime,
	}

	// Determine event type from event name
	switch {
	case strings.HasPrefix(record.EventName, "s3:ObjectCreated:"):
		unified.Type = notification.ObjectCreated
	case strings.HasPrefix(record.EventName, "s3:ObjectRemoved:"):
		unified.Type = notification.ObjectDeleted
	default:
		return fmt.Errorf("%w: unsupported S3 event name %q",
			notification.ErrInvalidEvent, record.EventName)
	}

	return h.objectHandler.HandleEvent(ctx, &unified)
}
