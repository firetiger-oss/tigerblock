package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
	"time"
)

// WithMount returns an adapter that mounts a bucket at a specific prefix.
// When the mount prefix matches, operations are delegated to the mounted bucket.
// Location, Create, and Access operations are unchanged and passed to the
// underlying bucket.
func WithMount(prefix string, bucket Bucket) Adapter {
	return AdapterFunc(func(base Bucket) Bucket { return Mount(base, prefix, bucket) })
}

// Mount creates a new bucket that mounts another bucket at the specified prefix.
// Operations on keys matching the prefix are delegated to the mounted bucket,
// while other operations are handled by the underlying bucket.
func Mount(base Bucket, prefix string, mount Bucket) Bucket {
	return &mountedBucket{
		bucket: base,
		mount: mountedPrefixBucket{
			bucket: mount,
			prefix: prefix,
		},
	}
}

type mountedBucket struct {
	bucket Bucket
	mount  mountedPrefixBucket
}

func (b *mountedBucket) Location() string {
	return b.bucket.Location()
}

func (b *mountedBucket) Access(ctx context.Context) error {
	return b.bucket.Access(ctx)
}

func (b *mountedBucket) Create(ctx context.Context) error {
	return b.bucket.Create(ctx)
}

func (b *mountedBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	if strings.HasPrefix(key, b.mount.prefix) {
		return b.mount.HeadObject(ctx, key)
	}
	return b.bucket.HeadObject(ctx, key)
}

func (b *mountedBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	if strings.HasPrefix(key, b.mount.prefix) {
		return b.mount.GetObject(ctx, key, options...)
	}
	return b.bucket.GetObject(ctx, key, options...)
}

func (b *mountedBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	if strings.HasPrefix(key, b.mount.prefix) {
		return b.mount.PutObject(ctx, key, value, options...)
	}
	return b.bucket.PutObject(ctx, key, value, options...)
}

func (b *mountedBucket) DeleteObject(ctx context.Context, key string) error {
	var err1 = b.bucket.DeleteObject(ctx, key)
	var err2 error
	if strings.HasPrefix(key, b.mount.prefix) {
		err2 = b.mount.DeleteObject(ctx, key)
	}
	return errors.Join(err1, err2)
}

func (b *mountedBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	fromInMount := strings.HasPrefix(from, b.mount.prefix)
	toInMount := strings.HasPrefix(to, b.mount.prefix)

	// Same context - delegate to appropriate bucket
	if fromInMount && toInMount {
		return b.mount.CopyObject(ctx, from, to, options...)
	}
	if !fromInMount && !toInMount {
		return b.bucket.CopyObject(ctx, from, to, options...)
	}

	// Cross-mount copy - need streaming fallback
	return copyObjectStreaming(ctx, b, from, b, to, options...)
}

func (b *mountedBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return b.mount.DeleteObjects(ctx, func(yield func(string, error) bool) {
		for key, err := range b.bucket.DeleteObjects(ctx, objects) {
			if err != nil {
				if !yield(key, err) {
					return
				}
			} else if strings.HasPrefix(key, b.mount.prefix) {
				if !yield(key, nil) {
					return
				}
			}
		}
	})
}

func (b *mountedBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		listOptions := NewListOptions(options...)

		if strings.HasPrefix(listOptions.KeyPrefix(), b.mount.prefix) {
			b.mount.ListObjects(ctx, options...)(yield)
			return
		}

		var maxKeys = listOptions.MaxKeys()
		var listedMountPoint bool
		for object, err := range b.bucket.ListObjects(ctx, options...) {
			if err != nil {
				yield(Object{}, err)
				return
			}
			if !listedMountPoint && object.Key >= b.mount.prefix {
				listedMountPoint = true
				for object, err := range b.mount.ListObjects(ctx, options...) {
					if !yield(object, err) {
						return
					}
					if maxKeys--; maxKeys == 0 {
						return
					}
				}
			}
			if !strings.HasPrefix(object.Key, b.mount.prefix) {
				if !yield(object, nil) {
					return
				}
				if maxKeys--; maxKeys == 0 {
					return
				}
			}
		}

		if !listedMountPoint {
			b.mount.ListObjects(ctx, options...)(yield)
		}
	}
}

func (b *mountedBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		listOptions := NewListOptions(options...)

		if strings.HasPrefix(listOptions.KeyPrefix(), b.mount.prefix) {
			b.mount.WatchObjects(ctx, options...)(yield)
			return
		}

		type result struct {
			obj Object
			err error
		}

		spawn := func(seq iter.Seq2[Object, error]) <-chan result {
			ch := make(chan result)
			go func() {
				defer close(ch)
				for obj, err := range seq {
					ch <- result{obj: obj, err: err}
				}
			}()
			return ch
		}

		ctx, cancel := context.WithCancel(ctx)
		objects1 := spawn(b.mount.WatchObjects(ctx, options...))
		objects2 := spawn(func(yield func(Object, error) bool) {
			for object, err := range b.bucket.WatchObjects(ctx, options...) {
				if err != nil {
					yield(Object{}, err)
					return
				}
				if !strings.HasPrefix(object.Key, b.mount.prefix) {
					if !yield(object, nil) {
						return
					}
				}
			}
		})

		flush := func(ch <-chan result) {
			for range ch {
			}
		}
		defer flush(objects1)
		defer flush(objects2)
		defer cancel()

		var maxKeys = listOptions.MaxKeys()
		var res result
		var ok1 bool
		var ok2 bool
		for objects1 != nil || objects2 != nil {
			select {
			case res, ok1 = <-objects1:
				if !ok1 {
					objects1 = nil
					continue
				}
			case res, ok2 = <-objects2:
				if !ok2 {
					objects2 = nil
					continue
				}
			}
			if !yield(res.obj, res.err) {
				return
			}
			if maxKeys--; maxKeys == 0 {
				return
			}
		}
	}
}

func (b *mountedBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	if strings.HasPrefix(key, b.mount.prefix) {
		return b.mount.PresignGetObject(ctx, key, expiration, options...)
	}
	return b.bucket.PresignGetObject(ctx, key, expiration, options...)
}

func (b *mountedBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	if strings.HasPrefix(key, b.mount.prefix) {
		return b.mount.PresignPutObject(ctx, key, expiration, options...)
	}
	return b.bucket.PresignPutObject(ctx, key, expiration, options...)
}

func (b *mountedBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if strings.HasPrefix(key, b.mount.prefix) {
		return b.mount.PresignHeadObject(ctx, key, expiration)
	}
	return b.bucket.PresignHeadObject(ctx, key, expiration)
}

func (b *mountedBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if strings.HasPrefix(key, b.mount.prefix) {
		return b.mount.PresignDeleteObject(ctx, key, expiration)
	}
	return b.bucket.PresignDeleteObject(ctx, key, expiration)
}

type mountedPrefixBucket struct {
	bucket Bucket
	prefix string
}

func (b *mountedPrefixBucket) Location() string {
	return b.bucket.Location()
}

func (b *mountedPrefixBucket) Access(ctx context.Context) error {
	return b.bucket.Access(ctx)
}

func (b *mountedPrefixBucket) Create(ctx context.Context) error {
	return b.bucket.Create(ctx)
}

func (b *mountedPrefixBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	if !strings.HasPrefix(key, b.prefix) {
		return ObjectInfo{}, fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	strippedKey := strings.TrimPrefix(key, b.prefix)
	object, err := b.bucket.HeadObject(ctx, strippedKey)
	if err != nil {
		err = b.scopeError(err)
	}
	return object, err
}

func (b *mountedPrefixBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	if !strings.HasPrefix(key, b.prefix) {
		return nil, ObjectInfo{}, fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	strippedKey := strings.TrimPrefix(key, b.prefix)
	r, object, err := b.bucket.GetObject(ctx, strippedKey, options...)
	if err != nil {
		err = b.scopeError(err)
	}
	return r, object, err
}

func (b *mountedPrefixBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	if !strings.HasPrefix(key, b.prefix) {
		return ObjectInfo{}, fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	strippedKey := strings.TrimPrefix(key, b.prefix)
	object, err := b.bucket.PutObject(ctx, strippedKey, value, options...)
	if err != nil {
		err = b.scopeError(err)
	}
	return object, err
}

func (b *mountedPrefixBucket) DeleteObject(ctx context.Context, key string) error {
	if !strings.HasPrefix(key, b.prefix) {
		return nil
	}
	return b.bucket.DeleteObject(ctx, strings.TrimPrefix(key, b.prefix))
}

func (b *mountedPrefixBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	if !strings.HasPrefix(from, b.prefix) {
		return fmt.Errorf("%s: %w", from, ErrObjectNotFound)
	}
	if !strings.HasPrefix(to, b.prefix) {
		return fmt.Errorf("%s: %w", to, ErrObjectNotFound)
	}
	strippedFrom := strings.TrimPrefix(from, b.prefix)
	strippedTo := strings.TrimPrefix(to, b.prefix)
	err := b.bucket.CopyObject(ctx, strippedFrom, strippedTo, options...)
	if err != nil {
		err = b.scopeError(err)
	}
	return err
}

func (b *mountedPrefixBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		b.bucket.DeleteObjects(ctx, func(yield func(string, error) bool) {
			for key, err := range objects {
				if err != nil || strings.HasPrefix(key, b.prefix) {
					if !yield(strings.TrimPrefix(key, b.prefix), err) {
						return
					}
				}
			}
		})(yield)
	}
}

func (b *mountedPrefixBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		strippedOptions := b.listOptions(options)
		if strippedOptions == nil {
			return
		}
		for object, err := range b.bucket.ListObjects(ctx, strippedOptions...) {
			if err != nil {
				yield(Object{}, b.scopeError(err))
				return
			}
			object.Key = b.prefix + object.Key
			if !yield(object, nil) {
				return
			}
		}
	}
}

func (b *mountedPrefixBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		strippedOptions := b.listOptions(options)
		if strippedOptions == nil {
			return
		}
		for object, err := range b.bucket.WatchObjects(ctx, strippedOptions...) {
			if err != nil {
				yield(Object{}, b.scopeError(err))
				return
			}
			object.Key = b.prefix + object.Key
			if !yield(object, nil) {
				return
			}
		}
	}
}

func (b *mountedPrefixBucket) listOptions(options []ListOption) []ListOption {
	listOptions := NewListOptions(options...)
	keyPrefix := listOptions.KeyPrefix()
	startAfter := listOptions.StartAfter()
	if keyPrefix != "" && !strings.HasPrefix(keyPrefix, b.prefix) {
		return nil
	}
	if startAfter != "" && !strings.HasPrefix(startAfter, b.prefix) {
		return nil
	}
	if keyPrefix != "" {
		options = append(options, KeyPrefix(strings.TrimPrefix(keyPrefix, b.prefix)))
	}
	if startAfter != "" {
		options = append(options, StartAfter(strings.TrimPrefix(startAfter, b.prefix)))
	}
	return options
}

func (b *mountedPrefixBucket) scopeError(err error) error {
	return fmt.Errorf("%s: %w", b.prefix, err)
}

func (b *mountedPrefixBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	if !strings.HasPrefix(key, b.prefix) {
		return "", fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	strippedKey := strings.TrimPrefix(key, b.prefix)
	url, err := b.bucket.PresignGetObject(ctx, strippedKey, expiration, options...)
	if err != nil {
		err = b.scopeError(err)
	}
	return url, err
}

func (b *mountedPrefixBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	if !strings.HasPrefix(key, b.prefix) {
		return "", fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	strippedKey := strings.TrimPrefix(key, b.prefix)
	url, err := b.bucket.PresignPutObject(ctx, strippedKey, expiration, options...)
	if err != nil {
		err = b.scopeError(err)
	}
	return url, err
}

func (b *mountedPrefixBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if !strings.HasPrefix(key, b.prefix) {
		return "", fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	strippedKey := strings.TrimPrefix(key, b.prefix)
	url, err := b.bucket.PresignHeadObject(ctx, strippedKey, expiration)
	if err != nil {
		err = b.scopeError(err)
	}
	return url, err
}

func (b *mountedPrefixBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if !strings.HasPrefix(key, b.prefix) {
		return "", fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	strippedKey := strings.TrimPrefix(key, b.prefix)
	url, err := b.bucket.PresignDeleteObject(ctx, strippedKey, expiration)
	if err != nil {
		err = b.scopeError(err)
	}
	return url, err
}
