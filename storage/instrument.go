package storage

import (
	"context"
	"io"
	"iter"
	"time"

	"github.com/firetiger-oss/tigerblock/internal/oteltrace"
	"go.opentelemetry.io/otel/attribute"
)

// WithInstrumentation returns an adapter that wraps buckets with OpenTelemetry
// tracing. Each storage operation is recorded as a span with relevant attributes.
func WithInstrumentation() Adapter {
	return AdapterFunc(InstrumentedBucket)
}

// InstrumentedBucket wraps a bucket with OpenTelemetry tracing spans for all
// storage operations. Span attributes include bucket location, object keys,
// content metadata, and error information.
func InstrumentedBucket(bucket Bucket) Bucket {
	return &instrumentedBucket{base: bucket}
}

type instrumentedBucket struct{ base Bucket }

func (i *instrumentedBucket) Location() string                 { return i.base.Location() }
func (i *instrumentedBucket) Access(ctx context.Context) error { return i.base.Access(ctx) }
func (i *instrumentedBucket) Create(ctx context.Context) error { return i.base.Create(ctx) }

func (i *instrumentedBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.HeadObject",
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.head.object", key))
	defer span.End()
	object, err := i.base.HeadObject(ctx, key)
	oteltrace.RecordError(span, err)
	if err == nil {
		span.SetAttributes(
			attribute.String("storage.bucket.head.content-type", object.ContentType),
			attribute.String("storage.bucket.head.content-encoding", object.ContentEncoding),
			attribute.String("storage.bucket.head.etag", object.ETag),
			attribute.Int64("storage.bucket.head.size", object.Size),
		)
	}
	return object, err
}

func (i *instrumentedBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	getOptions := NewGetOptions(options...)
	start, _, _ := getOptions.BytesRange()
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.GetObject",
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.get.object", key),
		attribute.Int64("storage.bucket.get.offset", start))
	r, object, err := i.base.GetObject(ctx, key, options...)
	oteltrace.RecordError(span, err)
	if r != nil {
		r = oteltrace.ReadCloser(r, span, "storage.bucket.get.length")
	}
	if err == nil {
		span.SetAttributes(
			attribute.String("storage.bucket.get.content-type", object.ContentType),
			attribute.String("storage.bucket.get.content-encoding", object.ContentEncoding),
			attribute.String("storage.bucket.get.etag", object.ETag),
			attribute.Int64("storage.bucket.get.size", object.Size),
		)
	}
	return r, object, err
}

func (i *instrumentedBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	spanAttrs := []attribute.KeyValue{
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.put.object", key),
	}
	putOptions := NewPutOptions(options...)
	if ifMatch := putOptions.IfMatch(); ifMatch != "" {
		spanAttrs = append(spanAttrs,
			attribute.String("storage.bucket.put.if-match", ifMatch))
	}
	if ifNoneMatch := putOptions.IfNoneMatch(); ifNoneMatch != "" {
		spanAttrs = append(spanAttrs,
			attribute.String("storage.bucket.put.if-none-match", ifNoneMatch))
	}
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.PutObject", spanAttrs...)
	defer span.End()
	object, err := i.base.PutObject(ctx, key, value, options...)
	oteltrace.RecordError(span, err)
	span.SetAttributes(
		attribute.String("storage.bucket.put.content-type", object.ContentType),
		attribute.String("storage.bucket.put.content-encoding", object.ContentEncoding),
		attribute.String("storage.bucket.put.etag", object.ETag),
		attribute.Int64("storage.bucket.put.size", object.Size),
	)
	return object, err
}

func (i *instrumentedBucket) DeleteObject(ctx context.Context, key string) error {
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.DeleteObject",
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.delete.object", key))
	defer span.End()
	err := i.base.DeleteObject(ctx, key)
	oteltrace.RecordError(span, err)
	return err
}

func (i *instrumentedBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		ctx, span := oteltrace.Start(ctx, "storage.Bucket.DeleteObjects",
			attribute.String("storage.bucket.location", i.base.Location()))
		defer span.End()

		var count int
		var hasError bool
		for key, err := range i.base.DeleteObjects(ctx, objects) {
			if !yield(key, err) {
				break
			}
			if !hasError && err != nil {
				hasError = true
				oteltrace.RecordError(span, err)
			}
			count++
		}

		span.SetAttributes(attribute.Int("storage.bucket.delete.objects", count))
	}
}

func (i *instrumentedBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.CopyObject",
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.copy.from", from),
		attribute.String("storage.bucket.copy.to", to))
	defer span.End()
	err := i.base.CopyObject(ctx, from, to, options...)
	oteltrace.RecordError(span, err)
	return err
}

func (i *instrumentedBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		listOptions := NewListOptions(options...)
		ctx, span := oteltrace.Start(ctx, "storage.Bucket.ListObjects",
			attribute.String("storage.bucket.location", i.base.Location()),
			attribute.String("storage.bucket.list.prefix", listOptions.KeyPrefix()))
		defer span.End()

		oteltrace.RecordSeq(span, "storage.bucket.list.objects",
			i.base.ListObjects(ctx, options...),
		)(yield)
	}
}

func (i *instrumentedBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		listOptions := NewListOptions(options...)
		ctx, span := oteltrace.Start(ctx, "storage.Bucket.WatchObjects",
			attribute.String("storage.bucket.location", i.base.Location()),
			attribute.String("storage.bucket.watch.prefix", listOptions.KeyPrefix()))
		defer span.End()

		oteltrace.RecordSeq(span, "storage.bucket.watch.objects",
			i.base.WatchObjects(ctx, options...),
		)(yield)
	}
}

func (i *instrumentedBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.PresignGetObject",
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.presign.key", key))
	defer span.End()
	url, err := i.base.PresignGetObject(ctx, key, expiration, options...)
	oteltrace.RecordError(span, err)
	return url, err
}

func (i *instrumentedBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.PresignPutObject",
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.presign.key", key))
	defer span.End()
	url, err := i.base.PresignPutObject(ctx, key, expiration, options...)
	oteltrace.RecordError(span, err)
	return url, err
}

func (i *instrumentedBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.PresignHeadObject",
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.presign.key", key))
	defer span.End()
	url, err := i.base.PresignHeadObject(ctx, key, expiration)
	oteltrace.RecordError(span, err)
	return url, err
}

func (i *instrumentedBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	ctx, span := oteltrace.Start(ctx, "storage.Bucket.PresignDeleteObject",
		attribute.String("storage.bucket.location", i.base.Location()),
		attribute.String("storage.bucket.presign.key", key))
	defer span.End()
	url, err := i.base.PresignDeleteObject(ctx, key, expiration)
	oteltrace.RecordError(span, err)
	return url, err
}
