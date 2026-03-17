package notification_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/memory"
	"github.com/firetiger-oss/storage/notification"
	"github.com/firetiger-oss/storage/uri"
)

func TestObjectHandlerCreateEvent(t *testing.T) {
	// Create a memory bucket with test data
	bucket := memory.NewBucket(&memory.Entry{
		Key:   "path/to/object.json",
		Value: []byte(`{"hello":"world"}`),
	})

	// Add content type via PutObject
	_, err := bucket.PutObject(context.Background(), "path/to/object.json",
		strings.NewReader(`{"hello":"world"}`),
		storage.ContentType("application/json"),
	)
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	// Create a registry that returns our memory bucket for s3://test-bucket
	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	// Track what the handler receives
	var receivedMethod string
	var receivedHost string
	var receivedPath string
	var receivedBody []byte
	var receivedContentType string

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedHost = r.Host
		receivedPath = r.URL.Path
		receivedBody, _ = io.ReadAll(r.Body)
		receivedContentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))

	event := notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "path/to/object.json"),
		Size:   17,
		Time:   time.Now(),
	}

	err = objectHandler.HandleEvents(context.Background(), &event)
	if err != nil {
		t.Fatalf("HandleEvents failed: %v", err)
	}

	// Verify the HTTP request
	if receivedMethod != http.MethodPost {
		t.Errorf("expected method POST, got %s", receivedMethod)
	}
	if receivedHost != "test-bucket" {
		t.Errorf("expected host test-bucket, got %s", receivedHost)
	}
	if receivedPath != "/path/to/object.json" {
		t.Errorf("expected path /path/to/object.json, got %s", receivedPath)
	}
	if string(receivedBody) != `{"hello":"world"}` {
		t.Errorf("expected body {\"hello\":\"world\"}, got %s", string(receivedBody))
	}
	if receivedContentType != "application/json" {
		t.Errorf("expected content-type application/json, got %s", receivedContentType)
	}
}

func TestObjectHandlerDeleteEvent(t *testing.T) {
	// For delete events, we don't need the object in the bucket
	bucket := memory.NewBucket()

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	var receivedMethod string
	var receivedHost string
	var receivedPath string

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedMethod = r.Method
		receivedHost = r.Host
		receivedPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))

	event := notification.Event{
		Type:   notification.ObjectDeleted,
		Object: uri.Join("s3", "test-bucket", "path/to/deleted.json"),
		Time:   time.Now(),
	}

	err := objectHandler.HandleEvents(context.Background(), &event)
	if err != nil {
		t.Fatalf("HandleEvents failed: %v", err)
	}

	// Verify the HTTP request
	if receivedMethod != http.MethodDelete {
		t.Errorf("expected method DELETE, got %s", receivedMethod)
	}
	if receivedHost != "test-bucket" {
		t.Errorf("expected host test-bucket, got %s", receivedHost)
	}
	if receivedPath != "/path/to/deleted.json" {
		t.Errorf("expected path /path/to/deleted.json, got %s", receivedPath)
	}
}

func TestObjectHandlerHandlerError(t *testing.T) {
	bucket := memory.NewBucket(&memory.Entry{
		Key:   "test.txt",
		Value: []byte("test content"),
	})

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))

	event := notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "test.txt"),
	}

	err := objectHandler.HandleEvents(context.Background(), &event)
	if err == nil {
		t.Fatal("expected error for handler failure")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error to contain status 500, got: %v", err)
	}
}

func TestObjectHandlerObjectNotFound(t *testing.T) {
	bucket := memory.NewBucket() // Empty bucket

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))

	event := notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "nonexistent.txt"),
	}

	err := objectHandler.HandleEvents(context.Background(), &event)
	if err == nil {
		t.Fatal("expected error for object not found")
	}
	if !strings.Contains(err.Error(), "object not found") {
		t.Errorf("expected object not found error, got: %v", err)
	}
}

func TestObjectHandlerEventHeaders(t *testing.T) {
	bucket := memory.NewBucket()
	_, err := bucket.PutObject(context.Background(), "test.txt",
		strings.NewReader("content"),
		storage.ContentType("text/plain"),
		storage.Metadata("custom-key", "custom-value"),
	)
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	var headers http.Header

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		headers = r.Header.Clone()
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))

	eventTime := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	event := notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "test.txt"),
		Time:   eventTime,
	}

	err = objectHandler.HandleEvents(context.Background(), &event)
	if err != nil {
		t.Fatalf("HandleEvents failed: %v", err)
	}

	if headers.Get("X-Event-Source") != "s3" {
		t.Errorf("expected X-Event-Source s3, got %s", headers.Get("X-Event-Source"))
	}
	if headers.Get("X-Event-Time") != eventTime.Format(time.RFC3339) {
		t.Errorf("expected X-Event-Time %s, got %s",
			eventTime.Format(time.RFC3339), headers.Get("X-Event-Time"))
	}
	if headers.Get("X-Amz-Meta-custom-key") != "custom-value" {
		t.Errorf("expected X-Amz-Meta-custom-key custom-value, got %s",
			headers.Get("X-Amz-Meta-custom-key"))
	}
}

func TestObjectHandlerDeleteAfterProcessing(t *testing.T) {
	bucket := memory.NewBucket(&memory.Entry{
		Key:   "test/file.txt",
		Value: []byte("test content"),
	})
	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	handlerCalled := false
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	handler := notification.NewObjectHandler(httpHandler,
		notification.WithRegistry(registry),
		notification.WithDeleteAfterProcessing(true),
	)

	event := &notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "test/file.txt"),
	}

	err := handler.HandleEvents(t.Context(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !handlerCalled {
		t.Error("handler was not called")
	}

	_, _, err = bucket.GetObject(t.Context(), "test/file.txt")
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Errorf("expected object to be deleted, got error: %v", err)
	}
}

func TestObjectConsumer(t *testing.T) {
	bucket := memory.NewBucket(&memory.Entry{
		Key:   "consume/data.json",
		Value: []byte(`{"data":"value"}`),
	})
	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	var receivedBody []byte
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	})

	handler := notification.NewObjectConsumer(httpHandler,
		notification.WithRegistry(registry),
	)

	event := &notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "consume/data.json"),
	}

	err := handler.HandleEvents(t.Context(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(receivedBody) != `{"data":"value"}` {
		t.Errorf("expected body {\"data\":\"value\"}, got %s", string(receivedBody))
	}

	_, _, err = bucket.GetObject(t.Context(), "consume/data.json")
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Errorf("expected object to be deleted after consumption, got error: %v", err)
	}
}

func TestObjectHandlerFuncVariadic(t *testing.T) {
	var received []*notification.Event
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		received = events
		return nil
	})

	events := []*notification.Event{
		{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "file1.txt")},
		{Type: notification.ObjectDeleted, Object: uri.Join("s3", "bucket", "file2.txt")},
	}

	err := handler.HandleEvents(context.Background(), events...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 events, got %d", len(received))
	}
}

func TestObjectHandlerFuncSingleEvent(t *testing.T) {
	var received []*notification.Event
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		received = events
		return nil
	})

	event := &notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "bucket", "file.txt"),
	}

	err := handler.HandleEvents(context.Background(), event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received) != 1 {
		t.Fatalf("expected 1 event, got %d", len(received))
	}
	if received[0] != event {
		t.Error("expected the same event pointer")
	}
}

func TestObjectHandlerDeleteAfterProcessingOnlyOnSuccess(t *testing.T) {
	bucket := memory.NewBucket(&memory.Entry{
		Key:   "test/file.txt",
		Value: []byte("test content"),
	})
	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "processing failed", http.StatusInternalServerError)
	})

	handler := notification.NewObjectHandler(httpHandler,
		notification.WithRegistry(registry),
		notification.WithDeleteAfterProcessing(true),
	)

	event := &notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "test/file.txt"),
	}

	err := handler.HandleEvents(t.Context(), event)
	if err == nil {
		t.Fatal("expected error for handler failure")
	}

	_, _, getErr := bucket.GetObject(t.Context(), "test/file.txt")
	if getErr != nil {
		t.Errorf("expected object to NOT be deleted after failed processing, got error: %v", getErr)
	}
}
