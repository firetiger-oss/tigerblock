package storage

import (
	"cmp"
	"context"
	"errors"
	"io"
	"iter"
	"time"

	"github.com/achille-roussel/kway-go"
)

func WithOverlay(readLayer Bucket) Adapter {
	return AdapterFunc(func(writeLayer Bucket) Bucket {
		return Overlay(writeLayer, readLayer)
	})
}

func Overlay(writeLayer, readLayer Bucket) Bucket {
	return &overlayBucket{
		writeLayer: writeLayer,
		readLayer:  readLayer,
	}
}

type overlayBucket struct {
	writeLayer Bucket
	readLayer  Bucket
}

func (o *overlayBucket) Location() string {
	return o.writeLayer.Location()
}

func (o *overlayBucket) Access(ctx context.Context) error {
	writeErr := o.writeLayer.Access(ctx)
	readErr := o.readLayer.Access(ctx)
	return errors.Join(writeErr, readErr)
}

func (o *overlayBucket) Create(ctx context.Context) error {
	return o.writeLayer.Create(ctx)
}

func (o *overlayBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	info, err := o.writeLayer.HeadObject(ctx, key)
	if err != nil && errors.Is(err, ErrObjectNotFound) {
		return o.readLayer.HeadObject(ctx, key)
	}
	return info, err
}

func (o *overlayBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	reader, info, err := o.writeLayer.GetObject(ctx, key, options...)
	if err != nil && errors.Is(err, ErrObjectNotFound) {
		return o.readLayer.GetObject(ctx, key, options...)
	}
	return reader, info, err
}

func (o *overlayBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	return o.writeLayer.PutObject(ctx, key, value, options...)
}

func (o *overlayBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	return copyObjectStreaming(ctx, o, from, o, to, options...)
}

// DeleteObject deletes the object from the write layer only.
//
// KNOWN LIMITATION: If the object exists in the read layer, it will "reappear"
// after deletion because this implementation does not track deletions. To properly
// support deletes, we would need to implement "whiteout" files (similar to OCI/Docker
// image layers) that mark a key as deleted. A whiteout file would be a marker object
// (e.g., ".wh.{key}") in the write layer that indicates the key should be treated as
// deleted even if it exists in the read layer.
//
// Implementation sketch for whiteout support:
//   - DeleteObject: write a whiteout marker ".wh.{key}" to the write layer
//   - HeadObject/GetObject: check for whiteout marker before falling back to read layer
//   - ListObjects: filter out keys that have whiteout markers, filter out whiteout files themselves
//   - PutObject: delete any existing whiteout marker for the key
//   - DeleteObjects: same as DeleteObject but batched
//
// Edge cases to consider:
//   - Deleting a key that only exists in write layer (no whiteout needed)
//   - Deleting a key that only exists in read layer (whiteout needed)
//   - Deleting a key that exists in both layers (whiteout needed after write layer delete)
//   - Re-creating a key after deletion (must remove whiteout)
//   - Listing should not expose whiteout files to callers
//   - Prefix handling: ".wh." prefix could conflict with user keys
func (o *overlayBucket) DeleteObject(ctx context.Context, key string) error {
	return o.writeLayer.DeleteObject(ctx, key)
}

func (o *overlayBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return o.writeLayer.DeleteObjects(ctx, objects)
}

func (o *overlayBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		merged := kway.MergeFunc(
			func(a, b Object) int { return cmp.Compare(a.Key, b.Key) },
			o.writeLayer.ListObjects(ctx, options...),
			o.readLayer.ListObjects(ctx, options...),
		)

		var lastKey string
		for obj, err := range merged {
			if err != nil {
				yield(Object{}, err)
				return
			}
			if obj.Key == lastKey {
				continue
			}
			lastKey = obj.Key
			if !yield(obj, nil) {
				return
			}
		}
	}
}

func (o *overlayBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return o.writeLayer.WatchObjects(ctx, options...)
}

func (o *overlayBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	return o.writeLayer.PresignGetObject(ctx, key, expiration, options...)
}

func (o *overlayBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	return o.writeLayer.PresignPutObject(ctx, key, expiration, options...)
}

func (o *overlayBucket) PresignHeadObject(ctx context.Context, key string) (string, error) {
	return o.writeLayer.PresignHeadObject(ctx, key)
}

func (o *overlayBucket) PresignDeleteObject(ctx context.Context, key string) (string, error) {
	return o.writeLayer.PresignDeleteObject(ctx, key)
}
