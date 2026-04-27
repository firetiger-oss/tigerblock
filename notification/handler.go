package notification

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/uri"
)

// NewObjectHandler creates an ObjectHandler that dispatches events to the given
// http.Handler using the default storage registry.
//
// For create events, it fetches the object content from storage and includes it
// in the request body. For delete events, it sends a DELETE request without a body.
//
// The synthetic HTTP request has:
//   - Method: POST for create events, DELETE for delete events
//   - Host: the bucket name
//   - URI: "/" + object key
//   - Body: object content (for create events only)
//   - Headers: object metadata (Content-Type, Content-Length, etc.)
func NewObjectHandler(handler http.Handler, options ...Option) ObjectHandler {
	opts := NewOptions(options...)
	return &objectHandler{
		handler:               handler,
		registry:              opts.Registry(),
		filters:               opts.Filters(),
		deleteAfterProcessing: opts.DeleteAfterProcessing(),
	}
}

// NewObjectConsumer creates an ObjectHandler that processes and then deletes
// objects after successful handling. This is a convenience wrapper around
// NewObjectHandler with WithDeleteAfterProcessing(true).
//
// Use this when objects should be consumed (processed once and deleted), such as
// when processing files uploaded to a staging bucket for ingestion.
func NewObjectConsumer(handler http.Handler, options ...Option) ObjectHandler {
	return NewObjectHandler(handler, append(options, WithDeleteAfterProcessing(true))...)
}

// NewObjectHandlerFrom creates an ObjectHandler that dispatches events to the given
// http.Handler using the provided storage registry.
func NewObjectHandlerFrom(registry storage.Registry, handler http.Handler) ObjectHandler {
	return &objectHandler{
		handler:  handler,
		registry: registry,
	}
}

type objectHandler struct {
	handler               http.Handler
	registry              storage.Registry
	filters               []Filter
	deleteAfterProcessing bool
}

func (h *objectHandler) HandleEvents(ctx context.Context, events ...*Event) error {
	for _, event := range events {
		if err := h.handleEvent(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

func (h *objectHandler) handleEvent(ctx context.Context, event *Event) error {
	// Run filters before any expensive operations
	for _, filter := range h.filters {
		ok, err := filter(ctx, event)
		if err != nil {
			return err
		}
		if !ok {
			return nil // Skip silently
		}
	}

	switch event.Type {
	case ObjectCreated:
		return h.handleCreate(ctx, event)
	case ObjectDeleted:
		return h.handleDelete(ctx, event)
	default:
		return fmt.Errorf("%w: unknown event type %q", ErrInvalidEvent, event.Type)
	}
}

func (h *objectHandler) handleCreate(ctx context.Context, event *Event) error {
	// Parse the object URI
	scheme, bucketName, key := uri.Split(event.Object)

	// Load the bucket from registry (just scheme + bucket, no key)
	bucket, err := h.registry.LoadBucket(ctx, uri.Join(scheme, bucketName))
	if err != nil {
		return fmt.Errorf("loading bucket %s: %w", bucketName, err)
	}

	// Fetch the object
	reader, info, err := bucket.GetObject(ctx, key)
	if err != nil {
		return fmt.Errorf("getting object %s/%s: %w", bucketName, key, err)
	}
	defer reader.Close()

	// Create synthetic HTTP request
	r := (&http.Request{
		Method: http.MethodPost,
		URL: &url.URL{
			Path: "/" + key,
		},
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        make(http.Header),
		Body:          reader,
		ContentLength: info.Size,
		Host:          bucketName,
	}).WithContext(ctx)

	// Set content headers from object metadata
	if info.ContentType != "" {
		r.Header.Set("Content-Type", info.ContentType)
	}
	r.Header.Set("Content-Length", strconv.FormatInt(info.Size, 10))

	if info.ContentEncoding != "" {
		r.Header.Set("Content-Encoding", info.ContentEncoding)
	}
	if info.CacheControl != "" {
		r.Header.Set("Cache-Control", info.CacheControl)
	}
	if info.ETag != "" {
		r.Header.Set("ETag", info.ETag)
	}

	// Add custom metadata as X-Amz-Meta-* headers
	for key, value := range info.Metadata {
		r.Header.Set("X-Amz-Meta-"+key, value)
	}

	// Add event metadata headers
	if !event.Time.IsZero() {
		r.Header.Set("X-Event-Time", event.Time.Format(time.RFC3339))
	}
	if scheme != "" {
		r.Header.Set("X-Event-Source", scheme)
	}

	// Execute request using status capture writer
	w := &statusCaptureWriter{}
	h.handler.ServeHTTP(w, r)
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}

	if w.statusCode >= 400 {
		return fmt.Errorf("%w: status %d", ErrHandlerFailed, w.statusCode)
	}

	if h.deleteAfterProcessing {
		if err := bucket.DeleteObject(ctx, key); err != nil {
			slog.ErrorContext(ctx, "failed to delete object after processing",
				"bucket", bucketName,
				"key", key,
				"error", err)
		}
	}

	return nil
}

func (h *objectHandler) handleDelete(ctx context.Context, event *Event) error {
	// Parse the object URI
	scheme, bucketName, key := uri.Split(event.Object)

	// Create synthetic DELETE request (no body)
	r := (&http.Request{
		Method: http.MethodDelete,
		URL: &url.URL{
			Path: "/" + key,
		},
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       bucketName,
	}).WithContext(ctx)

	// Add event metadata headers
	if !event.Time.IsZero() {
		r.Header.Set("X-Event-Time", event.Time.Format(time.RFC3339))
	}
	if scheme != "" {
		r.Header.Set("X-Event-Source", scheme)
	}

	// Execute request
	w := &statusCaptureWriter{}
	h.handler.ServeHTTP(w, r)
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}

	if w.statusCode >= 400 {
		return fmt.Errorf("%w: status %d", ErrHandlerFailed, w.statusCode)
	}

	return nil
}

// statusCaptureWriter captures the HTTP status code from a handler response.
type statusCaptureWriter struct {
	statusCode int
	written    bool
}

func (w *statusCaptureWriter) Header() http.Header {
	return make(http.Header)
}

func (w *statusCaptureWriter) Write([]byte) (int, error) {
	if !w.written {
		w.statusCode = http.StatusOK
		w.written = true
	}
	return 0, nil
}

func (w *statusCaptureWriter) WriteHeader(statusCode int) {
	if !w.written {
		w.statusCode = statusCode
		w.written = true
	}
}
