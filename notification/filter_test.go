package notification_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/firetiger-oss/tigerblock/notification"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	"github.com/firetiger-oss/tigerblock/uri"
)

func TestFilterPrefix(t *testing.T) {
	filter := notification.FilterPrefix("sessions/")

	tests := []struct {
		key      string
		expected bool
	}{
		{"sessions/foo/bar.json", true},
		{"sessions/", true},
		{"other/path.txt", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			event := notification.Event{Object: uri.Join("s3", "bucket", tt.key)}
			ok, err := filter(t.Context(), &event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tt.expected {
				t.Errorf("FilterPrefix(%q) = %v, want %v", tt.key, ok, tt.expected)
			}
		})
	}
}

func TestFilterGlob(t *testing.T) {
	filter := notification.FilterGlob("*.json")

	tests := []struct {
		key      string
		expected bool
	}{
		{"file.json", true},
		{"data.json", true},
		{"file.txt", false},
		{"path/to/file.json", false}, // glob doesn't match path separators
	}

	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			event := notification.Event{Object: uri.Join("s3", "bucket", tt.key)}
			ok, err := filter(t.Context(), &event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tt.expected {
				t.Errorf("FilterGlob(%q) = %v, want %v", tt.key, ok, tt.expected)
			}
		})
	}
}

func TestFilterGlobInvalidPattern(t *testing.T) {
	filter := notification.FilterGlob("[invalid")
	event := notification.Event{Object: uri.Join("s3", "bucket", "test.txt")}

	_, err := filter(t.Context(), &event)
	if err == nil {
		t.Error("expected error for invalid glob pattern")
	}
}

func TestFilterEventType(t *testing.T) {
	filter := notification.FilterEventType(notification.ObjectCreated)

	tests := []struct {
		eventType notification.EventType
		expected  bool
	}{
		{notification.ObjectCreated, true},
		{notification.ObjectDeleted, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.eventType), func(t *testing.T) {
			event := notification.Event{Type: tt.eventType}
			ok, err := filter(t.Context(), &event)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tt.expected {
				t.Errorf("FilterEventType(%v) = %v, want %v", tt.eventType, ok, tt.expected)
			}
		})
	}
}

func TestFilterEventTypeMultiple(t *testing.T) {
	filter := notification.FilterEventType(notification.ObjectCreated, notification.ObjectDeleted)

	event := notification.Event{Type: notification.ObjectCreated}
	ok, err := filter(t.Context(), &event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected ObjectCreated to pass filter")
	}

	event = notification.Event{Type: notification.ObjectDeleted}
	ok, err = filter(t.Context(), &event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Error("expected ObjectDeleted to pass filter")
	}
}

func TestObjectHandlerWithFilterSkipped(t *testing.T) {
	// Track if GetObject is called
	getObjectCalled := false
	bucket := &trackingBucket{
		Bucket: memory.NewBucket(&memory.Entry{
			Key:   "other/file.txt",
			Value: []byte("content"),
		}),
		onGetObject: func() { getObjectCalled = true },
	}

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	handlerCalled := false
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	// Filter that rejects events not starting with "sessions/"
	objectHandler := notification.NewObjectHandler(httpHandler,
		notification.WithRegistry(registry),
		notification.WithFilter(notification.FilterPrefix("sessions/")),
	)

	event := notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "other/file.txt"), // Does NOT match filter
	}

	err := objectHandler.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("HandleEvents failed: %v", err)
	}

	if getObjectCalled {
		t.Error("GetObject should not be called when filter rejects event")
	}
	if handlerCalled {
		t.Error("HTTP handler should not be called when filter rejects event")
	}
}

func TestObjectHandlerWithFilterPassed(t *testing.T) {
	bucket := memory.NewBucket()
	_, err := bucket.PutObject(t.Context(), "sessions/data.json",
		strings.NewReader("test data"),
		storage.ContentType("application/json"),
	)
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	handlerCalled := false
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler,
		notification.WithRegistry(registry),
		notification.WithFilter(notification.FilterPrefix("sessions/")),
	)

	event := notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "sessions/data.json"), // Matches filter
	}

	err = objectHandler.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("HandleEvents failed: %v", err)
	}

	if !handlerCalled {
		t.Error("HTTP handler should be called when filter passes")
	}
}

func TestObjectHandlerMultipleFilters(t *testing.T) {
	bucket := memory.NewBucket()

	registry := storage.RegistryFunc(func(ctx context.Context, u string) (storage.Bucket, error) {
		return bucket, nil
	})

	handlerCalled := false
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled = true
		w.WriteHeader(http.StatusOK)
	})

	// Multiple filters: must be ObjectCreated AND start with "sessions/"
	objectHandler := notification.NewObjectHandler(httpHandler,
		notification.WithRegistry(registry),
		notification.WithFilter(notification.FilterEventType(notification.ObjectCreated)),
		notification.WithFilter(notification.FilterPrefix("sessions/")),
	)

	// Test 1: Fails first filter (wrong event type)
	event := notification.Event{
		Type:   notification.ObjectDeleted,
		Object: uri.Join("s3", "test-bucket", "sessions/data.json"),
	}
	err := objectHandler.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("HandleEvents failed: %v", err)
	}
	if handlerCalled {
		t.Error("handler should not be called when first filter rejects")
	}

	// Test 2: Fails second filter (wrong prefix)
	handlerCalled = false
	event = notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "other/data.json"),
	}
	err = objectHandler.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("HandleEvents failed: %v", err)
	}
	if handlerCalled {
		t.Error("handler should not be called when second filter rejects")
	}
}

func TestObjectHandlerFilterError(t *testing.T) {
	bucket := memory.NewBucket()

	registry := storage.RegistryFunc(func(ctx context.Context, u string) (storage.Bucket, error) {
		return bucket, nil
	})

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	expectedErr := errors.New("filter error")
	errorFilter := func(ctx context.Context, event *notification.Event) (bool, error) {
		return false, expectedErr
	}

	objectHandler := notification.NewObjectHandler(httpHandler,
		notification.WithRegistry(registry),
		notification.WithFilter(errorFilter),
	)

	event := notification.Event{
		Type:   notification.ObjectCreated,
		Object: uri.Join("s3", "test-bucket", "data.json"),
	}

	err := objectHandler.HandleEvents(t.Context(), &event)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestWithFiltersNoFilters(t *testing.T) {
	handlerCalled := false
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		handlerCalled = true
		return nil
	})

	wrapped := notification.WithFilters(handler)
	event := notification.Event{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "test.json")}

	err := wrapped.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler should be called when no filters")
	}
}

func TestWithFiltersPassesFilter(t *testing.T) {
	handlerCalled := false
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		handlerCalled = true
		return nil
	})

	wrapped := notification.WithFilters(handler,
		notification.FilterEventType(notification.ObjectCreated),
	)
	event := notification.Event{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "test.json")}

	err := wrapped.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler should be called when filter passes")
	}
}

func TestWithFiltersRejectsFilter(t *testing.T) {
	handlerCalled := false
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		handlerCalled = true
		return nil
	})

	wrapped := notification.WithFilters(handler,
		notification.FilterEventType(notification.ObjectCreated),
	)
	event := notification.Event{Type: notification.ObjectDeleted, Object: uri.Join("s3", "bucket", "test.json")}

	err := wrapped.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handlerCalled {
		t.Error("handler should not be called when filter rejects")
	}
}

func TestWithFiltersPropagatesError(t *testing.T) {
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	expectedErr := errors.New("filter error")
	errorFilter := func(ctx context.Context, event *notification.Event) (bool, error) {
		return false, expectedErr
	}

	wrapped := notification.WithFilters(handler, errorFilter)
	event := notification.Event{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "test.json")}

	err := wrapped.HandleEvents(t.Context(), &event)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

func TestWithFiltersMultipleFilters(t *testing.T) {
	handlerCalled := false
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		handlerCalled = true
		return nil
	})

	wrapped := notification.WithFilters(handler,
		notification.FilterEventType(notification.ObjectCreated),
		notification.FilterPrefix("sessions/"),
	)

	// Test: passes both filters
	event := notification.Event{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "sessions/data.json")}
	err := wrapped.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !handlerCalled {
		t.Error("handler should be called when all filters pass")
	}

	// Test: fails first filter
	handlerCalled = false
	event = notification.Event{Type: notification.ObjectDeleted, Object: uri.Join("s3", "bucket", "sessions/data.json")}
	err = wrapped.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handlerCalled {
		t.Error("handler should not be called when first filter rejects")
	}

	// Test: fails second filter
	handlerCalled = false
	event = notification.Event{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "other/data.json")}
	err = wrapped.HandleEvents(t.Context(), &event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if handlerCalled {
		t.Error("handler should not be called when second filter rejects")
	}
}

func TestWithFiltersBatchPassing(t *testing.T) {
	var received []*notification.Event
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		received = events
		return nil
	})

	wrapped := notification.WithFilters(handler,
		notification.FilterEventType(notification.ObjectCreated),
	)

	events := []*notification.Event{
		{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "file1.txt")},
		{Type: notification.ObjectDeleted, Object: uri.Join("s3", "bucket", "file2.txt")},
		{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "file3.txt")},
	}

	err := wrapped.HandleEvents(t.Context(), events...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 events, got %d", len(received))
	}
	if received[0].Object != uri.Join("s3", "bucket", "file1.txt") {
		t.Errorf("expected file1.txt, got %s", received[0].Object)
	}
	if received[1].Object != uri.Join("s3", "bucket", "file3.txt") {
		t.Errorf("expected file3.txt, got %s", received[1].Object)
	}
}

func TestWithFiltersBatchAllFiltered(t *testing.T) {
	handlerCalled := false
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		handlerCalled = true
		return nil
	})

	wrapped := notification.WithFilters(handler,
		notification.FilterEventType(notification.ObjectCreated),
	)

	events := []*notification.Event{
		{Type: notification.ObjectDeleted, Object: uri.Join("s3", "bucket", "file1.txt")},
		{Type: notification.ObjectDeleted, Object: uri.Join("s3", "bucket", "file2.txt")},
	}

	err := wrapped.HandleEvents(t.Context(), events...)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if handlerCalled {
		t.Error("handler should not be called when all events are filtered out")
	}
}

func TestWithFiltersBatchError(t *testing.T) {
	handler := notification.ObjectHandlerFunc(func(ctx context.Context, events ...*notification.Event) error {
		return nil
	})

	expectedErr := errors.New("filter error")
	errorFilter := func(ctx context.Context, event *notification.Event) (bool, error) {
		return false, expectedErr
	}

	wrapped := notification.WithFilters(handler, errorFilter)

	events := []*notification.Event{
		{Type: notification.ObjectCreated, Object: uri.Join("s3", "bucket", "file1.txt")},
	}

	err := wrapped.HandleEvents(t.Context(), events...)
	if err != expectedErr {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}
}

// trackingBucket wraps a bucket to track GetObject calls
type trackingBucket struct {
	storage.Bucket
	onGetObject func()
}

func (b *trackingBucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if b.onGetObject != nil {
		b.onGetObject()
	}
	return b.Bucket.GetObject(ctx, key, options...)
}
