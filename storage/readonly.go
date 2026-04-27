package storage

import (
	"context"
	"io"
	"iter"
	"time"
)

// WithReadOnly returns an adapter that wraps buckets to reject all write
// operations with ErrBucketReadOnly.
func WithReadOnly() Adapter {
	return AdapterFunc(ReadOnlyBucket)
}

// ReadOnlyBucket wraps a bucket to reject all write operations with
// ErrBucketReadOnly. Read operations are passed through to the underlying bucket.
func ReadOnlyBucket(bucket Bucket) Bucket {
	return &readOnlyBucket{bucket: bucket}
}

type readOnlyBucket struct {
	bucket Bucket
}

func (b *readOnlyBucket) Location() string {
	return b.bucket.Location()
}

func (b *readOnlyBucket) Access(ctx context.Context) error {
	return b.bucket.Access(ctx)
}

func (b *readOnlyBucket) Create(ctx context.Context) error {
	return ErrBucketReadOnly
}

func (b *readOnlyBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	return b.bucket.HeadObject(ctx, key)
}

func (b *readOnlyBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	return b.bucket.GetObject(ctx, key, options...)
}

func (b *readOnlyBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	return ObjectInfo{}, ErrBucketReadOnly
}

func (b *readOnlyBucket) DeleteObject(ctx context.Context, key string) error {
	return ErrBucketReadOnly
}

func (b *readOnlyBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for key, err := range objects {
			if err == nil {
				err = ErrBucketReadOnly
			}
			if !yield(key, err) {
				return
			}
		}
	}
}

func (b *readOnlyBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	return ErrBucketReadOnly
}

func (b *readOnlyBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return b.bucket.ListObjects(ctx, options...)
}

func (b *readOnlyBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return b.bucket.WatchObjects(ctx, options...)
}

func (b *readOnlyBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	return b.bucket.PresignGetObject(ctx, key, expiration, options...)
}

func (b *readOnlyBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	return "", ErrBucketReadOnly
}

func (b *readOnlyBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return b.bucket.PresignHeadObject(ctx, key, expiration)
}

func (b *readOnlyBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return "", ErrBucketReadOnly
}
