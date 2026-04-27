package storage

import (
	"cmp"
	"context"
	"io"
	"iter"
	"time"
)

// EmptyBucket returns a read-only bucket that contains no objects.
// All read operations (HeadObject, GetObject, ListObjects) will behave
// as if the bucket exists but is empty. All write operations will return
// ErrBucketReadOnly.
func EmptyBucket() Bucket { return emptyBucket{} }

type emptyBucket struct{}

func (emptyBucket) Location() string { return ":none:" }

func (emptyBucket) Access(ctx context.Context) error {
	return context.Cause(ctx)
}

func (emptyBucket) Create(ctx context.Context) error {
	return cmp.Or(context.Cause(ctx), ErrBucketReadOnly)
}

func (emptyBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	return ObjectInfo{}, cmp.Or(context.Cause(ctx), ValidObjectKey(key), ErrObjectNotFound)
}

func (emptyBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	return nil, ObjectInfo{}, cmp.Or(context.Cause(ctx), ValidObjectKey(key), ErrObjectNotFound)
}

func (emptyBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	return ObjectInfo{}, cmp.Or(context.Cause(ctx), ErrBucketReadOnly)
}

func (emptyBucket) DeleteObject(ctx context.Context, key string) error {
	return cmp.Or(context.Cause(ctx), ErrBucketReadOnly)
}

func (emptyBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		yield("", cmp.Or(context.Cause(ctx), ErrBucketReadOnly))
	}
}

func (emptyBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	return cmp.Or(context.Cause(ctx), ValidObjectKey(from), ErrObjectNotFound)
}

func (emptyBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		if err := context.Cause(ctx); err != nil {
			yield(Object{}, err)
		}
	}
}

func (emptyBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		// Block forever but never yield anything
		<-ctx.Done()
		yield(Object{}, context.Cause(ctx))
	}
}

func (emptyBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	return "", cmp.Or(context.Cause(ctx), ErrPresignNotSupported)
}

func (emptyBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	return "", cmp.Or(context.Cause(ctx), ErrBucketReadOnly)
}

func (emptyBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return "", cmp.Or(context.Cause(ctx), ErrPresignNotSupported)
}

func (emptyBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return "", cmp.Or(context.Cause(ctx), ErrBucketReadOnly)
}
