package storage

import (
	"context"
	"errors"
	"io"
	"iter"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/firetiger-oss/storage/uri"
)

func isExpectedError(err error) bool {
	var e interface{ Expected() bool }
	return errors.As(err, &e) && e.Expected()
}

func isTemporaryError(err error) bool {
	var e interface{ Temporary() bool }
	return errors.As(err, &e) && e.Temporary()
}

func logLevelOf(err error) slog.Level {
	// A canceled context is almost always a caller-driven early termination
	// (DuckDB closing a parquet reader after pruning a row group, a client
	// aborting a stream, etc.), not a storage failure. Demote to DEBUG so it
	// doesn't drown out real errors. DeadlineExceeded is intentionally left
	// at ERROR — that one means a timeout actually fired.
	if errors.Is(err, context.Canceled) {
		return slog.LevelDebug
	}
	if isExpectedError(err) {
		return slog.LevelDebug
	}
	if isTemporaryError(err) {
		return slog.LevelWarn
	}
	return slog.LevelError
}

// WithLogger returns an adapter that wraps buckets with structured logging
// of all operations using the provided slog.Logger.
func WithLogger(logger *slog.Logger) Adapter {
	return AdapterFunc(func(base Bucket) Bucket { return LoggedBucket(base, logger) })
}

// LoggedBucket wraps a bucket with structured logging. All operations are logged
// with relevant attributes such as key, size, duration, and error details.
func LoggedBucket(bucket Bucket, logger *slog.Logger) Bucket {
	return &loggedBucket{bucket: bucket, logger: logger}
}

type loggedBucket struct {
	bucket Bucket
	logger *slog.Logger
}

func (b *loggedBucket) Location() string {
	return b.bucket.Location()
}

func (b *loggedBucket) Access(ctx context.Context) error {
	start := time.Now()
	err := b.bucket.Access(ctx)

	const op = "Access"
	attrLocation := makeAttrLocation(b)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		b.logger.Log(ctx, logLevelOf(err), op, attrLocation, attrDuration, makeAttrError(err))
	} else {
		b.logger.DebugContext(ctx, op, attrLocation, attrDuration)
	}

	return err
}

func (b *loggedBucket) Create(ctx context.Context) error {
	start := time.Now()
	err := b.bucket.Create(ctx)

	const op = "Create"
	attrLocation := makeAttrLocation(b)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		b.logger.Log(ctx, logLevelOf(err), op, attrLocation, attrDuration, makeAttrError(err))
	} else {
		b.logger.DebugContext(ctx, op, attrLocation, attrDuration)
	}

	return err
}

func (b *loggedBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	start := time.Now()
	object, err := b.bucket.HeadObject(ctx, key)

	const op = "HeadObject"
	attrKey := makeAttrKey(b, key)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		b.logger.Log(ctx, logLevelOf(err), op, attrKey, attrDuration, makeAttrError(err))
	} else {
		attrSize := makeAttrSize(object.Size)
		attrETag := makeAttrETag(object.ETag, "")
		attrContentType := makeAttrContentType(object.ContentType)
		b.logger.DebugContext(ctx, op, attrKey, attrSize, attrETag, attrContentType, attrDuration)
	}

	return object, err
}

func (b *loggedBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	getOptions := NewGetOptions(options...)
	offset, _, _ := getOptions.BytesRange()

	start := time.Now()
	r, object, err := b.bucket.GetObject(ctx, key, options...)

	if err != nil {
		attrKey := makeAttrKey(b, key)
		attrDuration := makeAttrDuration(start)
		b.logger.Log(ctx, logLevelOf(err), "GetObject", attrKey, attrDuration, makeAttrError(err))
	} else {
		r = &loggedReadCloser{
			bucket: b,
			reader: r,
			ctx:    ctx,
			key:    key,
			offset: offset,
			start:  start,
		}
	}

	return r, object, err
}

type loggedReadCloser struct {
	bucket *loggedBucket
	reader io.ReadCloser
	ctx    context.Context
	key    string
	offset int64
	size   int64
	start  time.Time
	once   sync.Once
}

func (r *loggedReadCloser) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	r.size += int64(n)
	return n, err
}

func (r *loggedReadCloser) Close() error {
	err := r.reader.Close()

	defer r.once.Do(func() {
		attrKey := makeAttrKey(r.bucket, r.key)
		attrSize := makeAttrSize(r.size)
		attrOffset := makeAttrOffset(r.offset)
		attrDuration := makeAttrDuration(r.start)
		if err != nil && !errors.Is(err, io.EOF) {
			r.bucket.logger.Log(r.ctx, logLevelOf(err), "GetObject", attrKey, attrSize, attrOffset, attrDuration, makeAttrError(err))
		} else {
			r.bucket.logger.DebugContext(r.ctx, "GetObject", attrKey, attrSize, attrOffset, attrDuration)
		}
	})

	return err
}

func (b *loggedBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	start := time.Now()
	object, err := b.bucket.PutObject(ctx, key, value, options...)

	const op = "PutObject"
	attrKey := makeAttrKey(b, key)
	attrSize := makeAttrSize(object.Size)
	attrETag := makeAttrETag(object.ETag, "")
	attrContentType := makeAttrContentType(object.ContentType)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		b.logger.Log(ctx, logLevelOf(err), op, attrKey, attrSize, attrETag, attrContentType, attrDuration, makeAttrError(err))
	} else {
		b.logger.DebugContext(ctx, op, attrKey, attrSize, attrETag, attrContentType, attrDuration)
	}

	return object, err
}

func (b *loggedBucket) DeleteObject(ctx context.Context, key string) error {
	start := time.Now()
	err := b.bucket.DeleteObject(ctx, key)

	const op = "DeleteObject"
	attrKey := makeAttrKey(b, key)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		b.logger.Log(ctx, logLevelOf(err), op, attrKey, attrDuration, makeAttrError(err))
	} else {
		b.logger.DebugContext(ctx, op, attrKey, attrDuration)
	}

	return err
}

func (b *loggedBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		const op = "DeleteObjects"
		var hasError bool
		for key, err := range b.bucket.DeleteObjects(ctx, objects) {
			attrKey := makeAttrKey(b, key)
			if err != nil {
				hasError = true
				b.logger.Log(ctx, logLevelOf(err), op, attrKey, makeAttrError(err))
			}
			if !yield(key, err) {
				return
			}
		}
		if !hasError {
			b.logger.DebugContext(ctx, op)
		}
	}
}

func (b *loggedBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	start := time.Now()
	err := b.bucket.CopyObject(ctx, from, to, options...)

	const op = "CopyObject"
	attrFrom := makeAttrKey(b, from)
	attrTo := slog.String("to", to)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		b.logger.Log(ctx, logLevelOf(err), op, attrFrom, attrTo, attrDuration, makeAttrError(err))
	} else {
		b.logger.DebugContext(ctx, op, attrFrom, attrTo, attrDuration)
	}

	return err
}

func (b *loggedBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		start := time.Now()
		numObjects := 0
		listOptions := NewListOptions(options...)

		const op = "ListObjects"
		attrPrefix := makeAttrPrefix(b, listOptions.KeyPrefix())

		for object, err := range b.bucket.ListObjects(ctx, options...) {
			if err != nil {
				attrDuration := makeAttrDuration(start)
				b.logger.Log(ctx, logLevelOf(err), op, attrPrefix, attrDuration, makeAttrError(err))
				yield(Object{}, err)
				return
			}
			numObjects++
			if !yield(object, nil) {
				break
			}
		}

		attrCount := makeAttrCount(numObjects)
		attrDuration := makeAttrDuration(start)
		b.logger.DebugContext(ctx, op, attrPrefix, attrCount, attrDuration)
	}
}

func (b *loggedBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		listOptions := NewListOptions(options...)

		const op = "WatchObjects"
		attrPrefix := makeAttrPrefix(b, listOptions.KeyPrefix())

		for object, err := range b.bucket.WatchObjects(ctx, options...) {
			if err != nil {
				b.logger.Log(ctx, logLevelOf(err), op, attrPrefix, makeAttrError(err))
				yield(Object{}, err)
				return
			}
			if strings.HasSuffix(object.Key, "/") {
				b.logger.DebugContext(ctx, op, makeAttrKey(b, object.Key))
			} else {
				b.logger.DebugContext(ctx, op, makeAttrKey(b, object.Key), makeAttrSize(object.Size))
			}
			if !yield(object, nil) {
				break
			}
		}
	}
}

func (b *loggedBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	start := time.Now()
	url, err := b.bucket.PresignGetObject(ctx, key, expiration, options...)

	const op = "Presign"
	attrMethod := slog.String("method", "GetObject")
	attrKey := makeAttrKey(b.bucket, key)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		if errors.Is(err, ErrPresignNotSupported) {
			b.logger.WarnContext(ctx, op, attrMethod, attrKey, attrDuration, makeAttrError(err))
		} else {
			b.logger.Log(ctx, logLevelOf(err), op, attrMethod, attrKey, attrDuration, makeAttrError(err))
		}
	} else {
		b.logger.DebugContext(ctx, op, attrMethod, attrKey, attrDuration)
	}

	return url, err
}

func (b *loggedBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	start := time.Now()
	url, err := b.bucket.PresignPutObject(ctx, key, expiration, options...)

	const op = "Presign"
	attrMethod := slog.String("method", "PutObject")
	attrKey := makeAttrKey(b.bucket, key)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		if errors.Is(err, ErrPresignNotSupported) {
			b.logger.WarnContext(ctx, op, attrMethod, attrKey, attrDuration, makeAttrError(err))
		} else {
			b.logger.Log(ctx, logLevelOf(err), op, attrMethod, attrKey, attrDuration, makeAttrError(err))
		}
	} else {
		b.logger.DebugContext(ctx, op, attrMethod, attrKey, attrDuration)
	}

	return url, err
}

func (b *loggedBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	start := time.Now()
	url, err := b.bucket.PresignHeadObject(ctx, key, expiration)

	const op = "Presign"
	attrMethod := slog.String("method", "HeadObject")
	attrKey := makeAttrKey(b.bucket, key)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		if errors.Is(err, ErrPresignNotSupported) {
			b.logger.WarnContext(ctx, op, attrMethod, attrKey, attrDuration, makeAttrError(err))
		} else {
			b.logger.Log(ctx, logLevelOf(err), op, attrMethod, attrKey, attrDuration, makeAttrError(err))
		}
	} else {
		b.logger.DebugContext(ctx, op, attrMethod, attrKey, attrDuration)
	}

	return url, err
}

func (b *loggedBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	start := time.Now()
	url, err := b.bucket.PresignDeleteObject(ctx, key, expiration)

	const op = "Presign"
	attrMethod := slog.String("method", "DeleteObject")
	attrKey := makeAttrKey(b.bucket, key)
	attrDuration := makeAttrDuration(start)
	if err != nil {
		if errors.Is(err, ErrPresignNotSupported) {
			b.logger.WarnContext(ctx, op, attrMethod, attrKey, attrDuration, makeAttrError(err))
		} else {
			b.logger.Log(ctx, logLevelOf(err), op, attrMethod, attrKey, attrDuration, makeAttrError(err))
		}
	} else {
		b.logger.DebugContext(ctx, op, attrMethod, attrKey, attrDuration)
	}

	return url, err
}

func makeAttrKey(b Bucket, key string) slog.Attr {
	scheme, location, path := uri.Split(b.Location())
	return slog.String("key", uri.Join(scheme, location, path, key))
}

func makeAttrETag(oldETag, newETag string) slog.Attr {
	switch {
	case newETag == "":
		return slog.String("etag", oldETag)
	case oldETag == "":
		return slog.String("etag", newETag)
	default:
		return slog.String("etag", oldETag+"→"+newETag)
	}
}

func makeAttrContentType(contentType string) slog.Attr {
	return slog.String("content-type", contentType)
}

func makeAttrSize(size int64) slog.Attr {
	return slog.Int64("size", size)
}

func makeAttrOffset(off int64) slog.Attr {
	return slog.Int64("offset", off)
}

func makeAttrCount(n int) slog.Attr {
	return slog.Int("count", n)
}

func makeAttrLocation(b Bucket) slog.Attr {
	return slog.String("location", b.Location())
}

func makeAttrPrefix(b Bucket, p string) slog.Attr {
	scheme, location, path := uri.Split(b.Location())
	return slog.String("key-prefix", uri.Join(scheme, location, path, p))
}

func makeAttrDuration(start time.Time) slog.Attr {
	return slog.Duration("duration", time.Since(start))
}

func makeAttrError(err error) slog.Attr {
	return slog.String("error", err.Error())
}
