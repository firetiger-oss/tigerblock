package storage

import (
	"context"
	"io"
	"iter"
	"strings"
	"time"

	"github.com/firetiger-oss/tigerblock/uri"
)

func WithPrefix(prefix string) Adapter {
	return AdapterFunc(func(b Bucket) Bucket { return Prefix(b, prefix) })
}

func Prefix(bucket Bucket, prefix string) Bucket {
	return &prefixedBucket{
		bucket: bucket,
		prefix: prefix,
	}
}

type prefixedBucket struct {
	bucket Bucket
	prefix string
}

func (b *prefixedBucket) Location() string {
	scheme, location, prefix := uri.Split(b.bucket.Location())
	return uri.Join(scheme, location, prefix, b.prefix)
}

func (b *prefixedBucket) Access(ctx context.Context) error {
	return b.bucket.Access(ctx)
}

func (b *prefixedBucket) Create(ctx context.Context) error {
	return b.bucket.Create(ctx)
}

func (b *prefixedBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	if err := ValidObjectKey(key); err != nil {
		return ObjectInfo{}, err
	}
	return b.bucket.HeadObject(ctx, b.prefix+key)
}

func (b *prefixedBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	if err := ValidObjectKey(key); err != nil {
		return nil, ObjectInfo{}, err
	}
	return b.bucket.GetObject(ctx, b.prefix+key, options...)
}

func (b *prefixedBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	if err := ValidObjectKey(key); err != nil {
		return ObjectInfo{}, err
	}
	return b.bucket.PutObject(ctx, b.prefix+key, value, options...)
}

func (b *prefixedBucket) DeleteObject(ctx context.Context, key string) error {
	if err := ValidObjectKey(key); err != nil {
		return err
	}
	return b.bucket.DeleteObject(ctx, b.prefix+key)
}

func (b *prefixedBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for key, err := range b.bucket.DeleteObjects(ctx, func(yield func(string, error) bool) {
			for key, err := range objects {
				// Validate the user-facing key before prefixing so invalid
				// inputs are rejected uniformly — after concatenation a
				// trailing-slash prefix can turn an invalid user key (e.g. "")
				// into a valid one at the backend, which would bypass the
				// rejection.
				if err == nil {
					err = ValidObjectKey(key)
				}
				if !yield(b.prefix+key, err) {
					return
				}
			}
		}) {
			if !yield(strings.TrimPrefix(key, b.prefix), err) {
				return
			}
		}
	}
}

func (b *prefixedBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	if err := ValidObjectKey(from); err != nil {
		return err
	}
	if err := ValidObjectKey(to); err != nil {
		return err
	}
	return b.bucket.CopyObject(ctx, b.prefix+from, b.prefix+to, options...)
}

func (b *prefixedBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		prefixOptions := b.listOptions(options...)

		for object, err := range b.bucket.ListObjects(ctx, prefixOptions...) {
			if err != nil {
				yield(Object{}, err)
				return
			}
			object.Key = strings.TrimPrefix(object.Key, b.prefix)
			if !yield(object, nil) {
				return
			}
		}
	}
}

func (b *prefixedBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		prefixOptions := b.listOptions(options...)

		for object, err := range b.bucket.WatchObjects(ctx, prefixOptions...) {
			if err != nil {
				yield(Object{}, err)
				return
			}
			object.Key = strings.TrimPrefix(object.Key, b.prefix)
			if !yield(object, nil) {
				return
			}
		}
	}
}

func (b *prefixedBucket) listOptions(options ...ListOption) []ListOption {
	listOptions := NewListOptions(options...)

	var prefixOptions []ListOption
	prefixOptions = append(prefixOptions, KeyPrefix(b.prefix+listOptions.KeyPrefix()))
	if startAfter := listOptions.StartAfter(); startAfter != "" {
		prefixOptions = append(prefixOptions, StartAfter(b.prefix+startAfter))
	}
	if keyDelimiter := listOptions.KeyDelimiter(); keyDelimiter != "" {
		prefixOptions = append(prefixOptions, KeyDelimiter(keyDelimiter))
	}
	if maxKeys := listOptions.MaxKeys(); maxKeys > 0 {
		prefixOptions = append(prefixOptions, MaxKeys(maxKeys))
	}
	return prefixOptions
}

func (b *prefixedBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	if err := ValidObjectKey(key); err != nil {
		return "", err
	}
	return b.bucket.PresignGetObject(ctx, b.prefix+key, expiration, options...)
}

func (b *prefixedBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	if err := ValidObjectKey(key); err != nil {
		return "", err
	}
	return b.bucket.PresignPutObject(ctx, b.prefix+key, expiration, options...)
}

func (b *prefixedBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := ValidObjectKey(key); err != nil {
		return "", err
	}
	return b.bucket.PresignHeadObject(ctx, b.prefix+key, expiration)
}

func (b *prefixedBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := ValidObjectKey(key); err != nil {
		return "", err
	}
	return b.bucket.PresignDeleteObject(ctx, b.prefix+key, expiration)
}
