package aws_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/firetiger-oss/tigerblock/notification"
	"github.com/firetiger-oss/tigerblock/notification/aws"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	"github.com/firetiger-oss/tigerblock/uri"
)

func TestS3EventHandlerObjectCreated(t *testing.T) {
	var receivedEvent notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvent = *events[0]
		return nil
	})

	handler := aws.NewS3EventHandler(objectHandler)

	event := aws.EventBridgeEvent[aws.S3EventDetail]{
		Version:    "0",
		ID:         "test-id",
		DetailType: "Object Created",
		Source:     "aws.s3",
		Account:    "123456789012",
		Time:       "2025-01-15T10:30:00Z",
		Region:     "us-west-2",
		Detail: aws.S3EventDetail{
			Bucket: aws.Bucket{Name: "my-bucket"},
			Object: aws.Object{
				Key:  "path/to/file.json",
				Size: 1234,
				ETag: "abc123",
			},
		},
	}

	err := handler.Handle(t.Context(), event)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	if receivedEvent.Type != notification.ObjectCreated {
		t.Errorf("expected type ObjectCreated, got %s", receivedEvent.Type)
	}
	expectedObject := uri.Join("s3", "my-bucket", "path/to/file.json")
	if receivedEvent.Object != expectedObject {
		t.Errorf("expected object %s, got %s", expectedObject, receivedEvent.Object)
	}
	if receivedEvent.Size != 1234 {
		t.Errorf("expected size 1234, got %d", receivedEvent.Size)
	}
	if receivedEvent.ETag != "abc123" {
		t.Errorf("expected etag abc123, got %s", receivedEvent.ETag)
	}
	if receivedEvent.Region != "us-west-2" {
		t.Errorf("expected region us-west-2, got %s", receivedEvent.Region)
	}
}

func TestS3EventHandlerObjectDeleted(t *testing.T) {
	var receivedEvent notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvent = *events[0]
		return nil
	})

	handler := aws.NewS3EventHandler(objectHandler)

	event := aws.EventBridgeEvent[aws.S3EventDetail]{
		DetailType: "Object Deleted",
		Source:     "aws.s3",
		Time:       "2025-01-15T10:30:00Z",
		Region:     "us-west-2",
		Detail: aws.S3EventDetail{
			Bucket: aws.Bucket{Name: "my-bucket"},
			Object: aws.Object{Key: "deleted.txt"},
		},
	}

	err := handler.Handle(t.Context(), event)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	if receivedEvent.Type != notification.ObjectDeleted {
		t.Errorf("expected type ObjectDeleted, got %s", receivedEvent.Type)
	}
}

func TestS3EventHandlerUnsupportedEventType(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := aws.NewS3EventHandler(objectHandler)

	event := aws.EventBridgeEvent[aws.S3EventDetail]{
		DetailType: "Object Restored",
		Source:     "aws.s3",
	}

	err := handler.Handle(t.Context(), event)
	if err == nil {
		t.Fatal("expected error for unsupported event type")
	}
}

func TestNewS3EventBridgeHandler(t *testing.T) {
	bucket := memory.NewBucket()
	_, err := bucket.PutObject(t.Context(), "test.txt",
		strings.NewReader("hello world"),
		storage.ContentType("text/plain"),
	)
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	var receivedBody []byte
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))
	eventBridgeHandler := aws.NewS3EventBridgeHandler(objectHandler)

	// Create test request with EventBridge payload
	payload := `{
		"version": "0",
		"id": "test-id",
		"detail-type": "Object Created",
		"source": "aws.s3",
		"account": "123456789012",
		"time": "2025-01-15T10:30:00Z",
		"region": "us-west-2",
		"detail": {
			"bucket": {"name": "my-bucket"},
			"object": {"key": "test.txt", "size": 11}
		}
	}`

	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte(payload)))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	eventBridgeHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if string(receivedBody) != "hello world" {
		t.Errorf("expected body 'hello world', got '%s'", string(receivedBody))
	}
}

func TestNewS3EventBridgeHandlerInvalidMethod(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := aws.NewS3EventBridgeHandler(objectHandler)

	req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", rec.Code)
	}
}

func TestNewS3EventBridgeHandlerInvalidJSON(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := aws.NewS3EventBridgeHandler(objectHandler)

	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte("invalid json")))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

// Tests for S3 Lambda Handler

func TestS3LambdaHandlerObjectCreated(t *testing.T) {
	var receivedEvents []*notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvents = events
		return nil
	})

	handler := aws.NewS3LambdaHandler(objectHandler)

	eventTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	s3Event := aws.S3Event{
		Records: []aws.S3EventRecord{{
			EventName: "s3:ObjectCreated:Put",
			AWSRegion: "us-west-2",
			EventTime: eventTime,
			S3: aws.S3Entity{
				Bucket: aws.S3Bucket{Name: "my-bucket"},
				Object: aws.S3Object{
					Key:  "path/to/file.json",
					Size: 1234,
					ETag: "abc123",
				},
			},
		}},
	}

	err := handler.HandleEvent(t.Context(), s3Event)
	if err != nil {
		t.Fatalf("HandleEvent failed: %v", err)
	}

	if len(receivedEvents) != 1 {
		t.Fatalf("expected 1 event, got %d", len(receivedEvents))
	}
	receivedEvent := receivedEvents[0]
	if receivedEvent.Type != notification.ObjectCreated {
		t.Errorf("expected type ObjectCreated, got %s", receivedEvent.Type)
	}
	expectedObject := uri.Join("s3", "my-bucket", "path/to/file.json")
	if receivedEvent.Object != expectedObject {
		t.Errorf("expected object %s, got %s", expectedObject, receivedEvent.Object)
	}
	if receivedEvent.Size != 1234 {
		t.Errorf("expected size 1234, got %d", receivedEvent.Size)
	}
	if receivedEvent.ETag != "abc123" {
		t.Errorf("expected etag abc123, got %s", receivedEvent.ETag)
	}
	if receivedEvent.Region != "us-west-2" {
		t.Errorf("expected region us-west-2, got %s", receivedEvent.Region)
	}
	if !receivedEvent.Time.Equal(eventTime) {
		t.Errorf("expected time %v, got %v", eventTime, receivedEvent.Time)
	}
}

func TestS3LambdaHandlerObjectDeleted(t *testing.T) {
	var receivedEvents []*notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvents = events
		return nil
	})

	handler := aws.NewS3LambdaHandler(objectHandler)

	s3Event := aws.S3Event{
		Records: []aws.S3EventRecord{{
			EventName: "s3:ObjectRemoved:Delete",
			AWSRegion: "us-west-2",
			S3: aws.S3Entity{
				Bucket: aws.S3Bucket{Name: "my-bucket"},
				Object: aws.S3Object{Key: "deleted.txt"},
			},
		}},
	}

	err := handler.HandleEvent(t.Context(), s3Event)
	if err != nil {
		t.Fatalf("HandleEvent failed: %v", err)
	}

	if len(receivedEvents) != 1 {
		t.Fatalf("expected 1 event, got %d", len(receivedEvents))
	}
	if receivedEvents[0].Type != notification.ObjectDeleted {
		t.Errorf("expected type ObjectDeleted, got %s", receivedEvents[0].Type)
	}
}

func TestS3LambdaHandlerMultipleRecords(t *testing.T) {
	var receivedEvents []*notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvents = events
		return nil
	})

	handler := aws.NewS3LambdaHandler(objectHandler)

	s3Event := aws.S3Event{
		Records: []aws.S3EventRecord{
			{
				EventName: "s3:ObjectCreated:Put",
				S3: aws.S3Entity{
					Bucket: aws.S3Bucket{Name: "bucket-1"},
					Object: aws.S3Object{Key: "file1.txt"},
				},
			},
			{
				EventName: "s3:ObjectCreated:Post",
				S3: aws.S3Entity{
					Bucket: aws.S3Bucket{Name: "bucket-2"},
					Object: aws.S3Object{Key: "file2.txt"},
				},
			},
		},
	}

	err := handler.HandleEvent(t.Context(), s3Event)
	if err != nil {
		t.Fatalf("HandleEvent failed: %v", err)
	}

	if len(receivedEvents) != 2 {
		t.Fatalf("expected 2 events, got %d", len(receivedEvents))
	}

	// Events are delivered in order (no longer concurrent)
	if receivedEvents[0].Object != uri.Join("s3", "bucket-1", "file1.txt") {
		t.Errorf("expected first event file1.txt, got %s", receivedEvents[0].Object)
	}
	if receivedEvents[1].Object != uri.Join("s3", "bucket-2", "file2.txt") {
		t.Errorf("expected second event file2.txt, got %s", receivedEvents[1].Object)
	}
}

func TestS3LambdaHandlerUnsupportedEventName(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := aws.NewS3LambdaHandler(objectHandler)

	s3Event := aws.S3Event{
		Records: []aws.S3EventRecord{{
			EventName: "s3:ObjectRestore:Completed",
			S3: aws.S3Entity{
				Bucket: aws.S3Bucket{Name: "my-bucket"},
				Object: aws.S3Object{Key: "restored.txt"},
			},
		}},
	}

	err := handler.HandleEvent(t.Context(), s3Event)
	if err == nil {
		t.Fatal("expected error for unsupported event name")
	}
}

func TestS3LambdaHandlerUnsupportedEventInBatch(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := aws.NewS3LambdaHandler(objectHandler)

	s3Event := aws.S3Event{
		Records: []aws.S3EventRecord{
			{
				EventName: "s3:ObjectCreated:Put",
				S3: aws.S3Entity{
					Bucket: aws.S3Bucket{Name: "bucket"},
					Object: aws.S3Object{Key: "good.txt"},
				},
			},
			{
				EventName: "s3:ObjectRestore:Completed",
				S3: aws.S3Entity{
					Bucket: aws.S3Bucket{Name: "bucket"},
					Object: aws.S3Object{Key: "bad.txt"},
				},
			},
		},
	}

	err := handler.HandleEvent(t.Context(), s3Event)
	if err == nil {
		t.Fatal("expected error for unsupported event name")
	}
}
