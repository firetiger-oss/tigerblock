package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/firetiger-oss/storage/notification"
)

func TestR2EventHandler(t *testing.T) {
	eventTime := time.Date(2024, 5, 24, 19, 36, 44, 379000000, time.UTC)

	tests := []struct {
		name          string
		event         R2Event
		expectedType  notification.EventType
		expectedKey   string
		expectedSize  int64
		expectedError bool
	}{
		{
			name: "PutObject creates ObjectCreated event",
			event: R2Event{
				Account:   "test-account",
				Action:    "PutObject",
				Bucket:    "my-bucket",
				Object:    R2Object{Key: "path/to/file.txt", Size: 1234, ETag: "abc123"},
				EventTime: eventTime,
			},
			expectedType: notification.ObjectCreated,
			expectedKey:  "path/to/file.txt",
			expectedSize: 1234,
		},
		{
			name: "CopyObject creates ObjectCreated event",
			event: R2Event{
				Account:   "test-account",
				Action:    "CopyObject",
				Bucket:    "my-bucket",
				Object:    R2Object{Key: "copied/file.txt", Size: 5678},
				EventTime: eventTime,
				CopySource: &R2CopySource{
					Bucket: "source-bucket",
					Object: "original/file.txt",
				},
			},
			expectedType: notification.ObjectCreated,
			expectedKey:  "copied/file.txt",
			expectedSize: 5678,
		},
		{
			name: "CompleteMultipartUpload creates ObjectCreated event",
			event: R2Event{
				Account:   "test-account",
				Action:    "CompleteMultipartUpload",
				Bucket:    "my-bucket",
				Object:    R2Object{Key: "large/file.bin", Size: 100000000},
				EventTime: eventTime,
			},
			expectedType: notification.ObjectCreated,
			expectedKey:  "large/file.bin",
			expectedSize: 100000000,
		},
		{
			name: "DeleteObject creates ObjectDeleted event",
			event: R2Event{
				Account:   "test-account",
				Action:    "DeleteObject",
				Bucket:    "my-bucket",
				Object:    R2Object{Key: "deleted/file.txt"},
				EventTime: eventTime,
			},
			expectedType: notification.ObjectDeleted,
			expectedKey:  "deleted/file.txt",
			expectedSize: 0,
		},
		{
			name: "LifecycleDeletion creates ObjectDeleted event",
			event: R2Event{
				Account:   "test-account",
				Action:    "LifecycleDeletion",
				Bucket:    "my-bucket",
				Object:    R2Object{Key: "expired/file.txt"},
				EventTime: eventTime,
			},
			expectedType: notification.ObjectDeleted,
			expectedKey:  "expired/file.txt",
		},
		{
			name: "Unknown action returns error",
			event: R2Event{
				Account: "test-account",
				Action:  "UnknownAction",
				Bucket:  "my-bucket",
				Object:  R2Object{Key: "file.txt"},
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedEvent *notification.Event
			handler := NewR2EventHandler(notification.ObjectHandlerFunc(
				func(ctx context.Context, event *notification.Event) error {
					capturedEvent = event
					return nil
				}))

			err := handler.Handle(context.Background(), tt.event)

			if tt.expectedError {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if !errors.Is(err, notification.ErrInvalidEvent) {
					t.Errorf("expected ErrInvalidEvent, got %v", err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if capturedEvent == nil {
				t.Fatal("expected event to be captured")
			}

			if capturedEvent.Type != tt.expectedType {
				t.Errorf("expected type %q, got %q", tt.expectedType, capturedEvent.Type)
			}
			if capturedEvent.Bucket != tt.event.Bucket {
				t.Errorf("expected bucket %q, got %q", tt.event.Bucket, capturedEvent.Bucket)
			}
			if capturedEvent.Key != tt.expectedKey {
				t.Errorf("expected key %q, got %q", tt.expectedKey, capturedEvent.Key)
			}
			if capturedEvent.Size != tt.expectedSize {
				t.Errorf("expected size %d, got %d", tt.expectedSize, capturedEvent.Size)
			}
			if capturedEvent.Source != "cloudflare:r2" {
				t.Errorf("expected source cloudflare:r2, got %q", capturedEvent.Source)
			}
		})
	}
}

func TestQueuesHandler(t *testing.T) {
	t.Run("successful POST request", func(t *testing.T) {
		var capturedEvent *notification.Event
		objectHandler := notification.ObjectHandlerFunc(
			func(ctx context.Context, event *notification.Event) error {
				capturedEvent = event
				return nil
			})

		handler := NewQueuesHandler(objectHandler)

		event := R2Event{
			Account:   "test-account",
			Action:    "PutObject",
			Bucket:    "my-bucket",
			Object:    R2Object{Key: "test.txt", Size: 100},
			EventTime: time.Now(),
		}
		body, _ := json.Marshal(event)

		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
		if capturedEvent == nil {
			t.Error("expected event to be captured")
		}
	})

	t.Run("method not allowed", func(t *testing.T) {
		handler := NewQueuesHandler(notification.ObjectHandlerFunc(
			func(ctx context.Context, event *notification.Event) error {
				return nil
			}))

		req := httptest.NewRequest(http.MethodGet, "/webhook", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected status 405, got %d", rec.Code)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		handler := NewQueuesHandler(notification.ObjectHandlerFunc(
			func(ctx context.Context, event *notification.Event) error {
				return nil
			}))

		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader([]byte("invalid json")))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected status 400, got %d", rec.Code)
		}
	})

	t.Run("handler error", func(t *testing.T) {
		handler := NewQueuesHandler(notification.ObjectHandlerFunc(
			func(ctx context.Context, event *notification.Event) error {
				return errors.New("handler failed")
			}))

		event := R2Event{
			Account: "test-account",
			Action:  "PutObject",
			Bucket:  "my-bucket",
			Object:  R2Object{Key: "test.txt"},
		}
		body, _ := json.Marshal(event)

		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", rec.Code)
		}
	})
}

func TestBatchQueuesHandler(t *testing.T) {
	t.Run("successful batch request", func(t *testing.T) {
		var capturedEvents []*notification.Event
		objectHandler := notification.ObjectHandlerFunc(
			func(ctx context.Context, event *notification.Event) error {
				capturedEvents = append(capturedEvents, event)
				return nil
			})

		handler := NewBatchQueuesHandler(objectHandler)

		events := []R2Event{
			{
				Account: "test-account",
				Action:  "PutObject",
				Bucket:  "my-bucket",
				Object:  R2Object{Key: "file1.txt", Size: 100},
			},
			{
				Account: "test-account",
				Action:  "DeleteObject",
				Bucket:  "my-bucket",
				Object:  R2Object{Key: "file2.txt"},
			},
		}
		body, _ := json.Marshal(events)

		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
		if len(capturedEvents) != 2 {
			t.Errorf("expected 2 events, got %d", len(capturedEvents))
		}
		if capturedEvents[0].Type != notification.ObjectCreated {
			t.Errorf("expected first event type ObjectCreated, got %q", capturedEvents[0].Type)
		}
		if capturedEvents[1].Type != notification.ObjectDeleted {
			t.Errorf("expected second event type ObjectDeleted, got %q", capturedEvents[1].Type)
		}
	})

	t.Run("batch handler error returns error status", func(t *testing.T) {
		var callCount atomic.Int32
		objectHandler := notification.ObjectHandlerFunc(
			func(ctx context.Context, event *notification.Event) error {
				if callCount.Add(1) == 1 {
					return errors.New("first event failed")
				}
				return nil
			})

		handler := NewBatchQueuesHandler(objectHandler)

		events := []R2Event{
			{Action: "PutObject", Bucket: "bucket", Object: R2Object{Key: "file1.txt"}},
			{Action: "PutObject", Bucket: "bucket", Object: R2Object{Key: "file2.txt"}},
		}
		body, _ := json.Marshal(events)

		req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewReader(body))
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", rec.Code)
		}
		// With concurrent processing, both events may be processed before error is returned
		if callCount.Load() < 1 {
			t.Errorf("expected at least 1 call, got %d", callCount.Load())
		}
	})
}
