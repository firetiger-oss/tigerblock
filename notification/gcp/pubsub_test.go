package gcp_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/memory"
	"github.com/firetiger-oss/storage/notification"
	"github.com/firetiger-oss/storage/notification/gcp"
	"github.com/firetiger-oss/storage/uri"
)

func TestBucketNotificationHandlerObjectFinalize(t *testing.T) {
	var receivedEvent notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvent = *events[0]
		return nil
	})

	handler := gcp.NewBucketNotificationHandler(objectHandler)

	msg := gcp.PubSubMessage{
		Attributes: map[string]string{
			"eventType":        "OBJECT_FINALIZE",
			"bucketId":         "my-gcs-bucket",
			"objectId":         "path/to/file.json",
			"objectGeneration": "1234567890",
		},
		MessageID:   "msg-123",
		PublishTime: "2025-01-15T10:30:00Z",
	}

	err := handler.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	if receivedEvent.Type != notification.ObjectCreated {
		t.Errorf("expected type ObjectCreated, got %s", receivedEvent.Type)
	}
	expectedObject := uri.Join("gs", "my-gcs-bucket", "path/to/file.json")
	if receivedEvent.Object != expectedObject {
		t.Errorf("expected object %s, got %s", expectedObject, receivedEvent.Object)
	}
}

func TestBucketNotificationHandlerObjectDelete(t *testing.T) {
	var receivedEvent notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvent = *events[0]
		return nil
	})

	handler := gcp.NewBucketNotificationHandler(objectHandler)

	msg := gcp.PubSubMessage{
		Attributes: map[string]string{
			"eventType": "OBJECT_DELETE",
			"bucketId":  "my-gcs-bucket",
			"objectId":  "deleted.txt",
		},
		PublishTime: "2025-01-15T10:30:00Z",
	}

	err := handler.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	if receivedEvent.Type != notification.ObjectDeleted {
		t.Errorf("expected type ObjectDeleted, got %s", receivedEvent.Type)
	}
}

func TestBucketNotificationHandlerObjectArchive(t *testing.T) {
	var receivedEvent notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvent = *events[0]
		return nil
	})

	handler := gcp.NewBucketNotificationHandler(objectHandler)

	msg := gcp.PubSubMessage{
		Attributes: map[string]string{
			"eventType": "OBJECT_ARCHIVE",
			"bucketId":  "my-gcs-bucket",
			"objectId":  "archived.txt",
		},
		PublishTime: "2025-01-15T10:30:00Z",
	}

	err := handler.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	if receivedEvent.Type != notification.ObjectDeleted {
		t.Errorf("expected type ObjectDeleted for archive, got %s", receivedEvent.Type)
	}
}

func TestBucketNotificationHandlerWithMetadata(t *testing.T) {
	var receivedEvent notification.Event

	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		receivedEvent = *events[0]
		return nil
	})

	handler := gcp.NewBucketNotificationHandler(objectHandler)

	// Create metadata payload
	metadata := `{"kind":"storage#object","id":"test","name":"test.txt","bucket":"my-gcs-bucket","size":"1024","etag":"abc123"}`
	encodedData := base64.StdEncoding.EncodeToString([]byte(metadata))

	msg := gcp.PubSubMessage{
		Data: encodedData,
		Attributes: map[string]string{
			"eventType":     "OBJECT_FINALIZE",
			"bucketId":      "my-gcs-bucket",
			"objectId":      "test.txt",
			"payloadFormat": "JSON_API_V1",
		},
		PublishTime: "2025-01-15T10:30:00Z",
	}

	err := handler.Handle(context.Background(), msg)
	if err != nil {
		t.Fatalf("Handle failed: %v", err)
	}

	if receivedEvent.Size != 1024 {
		t.Errorf("expected size 1024, got %d", receivedEvent.Size)
	}
	if receivedEvent.ETag != "abc123" {
		t.Errorf("expected etag abc123, got %s", receivedEvent.ETag)
	}
}

func TestBucketNotificationHandlerMissingAttributes(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := gcp.NewBucketNotificationHandler(objectHandler)

	// Missing bucketId
	msg := gcp.PubSubMessage{
		Attributes: map[string]string{
			"eventType": "OBJECT_FINALIZE",
			"objectId":  "test.txt",
		},
	}

	err := handler.Handle(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for missing bucketId")
	}
}

func TestBucketNotificationHandlerUnsupportedEventType(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := gcp.NewBucketNotificationHandler(objectHandler)

	msg := gcp.PubSubMessage{
		Attributes: map[string]string{
			"eventType": "OBJECT_METADATA_UPDATE",
			"bucketId":  "my-gcs-bucket",
			"objectId":  "test.txt",
		},
	}

	err := handler.Handle(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for unsupported event type")
	}
}

func TestNewPubSubHandler(t *testing.T) {
	bucket := memory.NewBucket()
	_, err := bucket.PutObject(context.Background(), "test.txt",
		strings.NewReader("hello gcs"),
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
	pubsubHandler := gcp.NewPubSubHandler(objectHandler)

	// Create test request with Pub/Sub push payload
	payload := `{
		"message": {
			"attributes": {
				"eventType": "OBJECT_FINALIZE",
				"bucketId": "my-gcs-bucket",
				"objectId": "test.txt"
			},
			"messageId": "msg-123",
			"publishTime": "2025-01-15T10:30:00Z"
		},
		"subscription": "projects/my-project/subscriptions/my-sub"
	}`

	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte(payload)))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	pubsubHandler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if string(receivedBody) != "hello gcs" {
		t.Errorf("expected body 'hello gcs', got '%s'", string(receivedBody))
	}
}

func TestNewPubSubHandlerInvalidMethod(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := gcp.NewPubSubHandler(objectHandler)

	req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected status 405, got %d", rec.Code)
	}
}

func TestNewPubSubHandlerInvalidJSON(t *testing.T) {
	objectHandler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	handler := gcp.NewPubSubHandler(objectHandler)

	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte("invalid json")))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}
