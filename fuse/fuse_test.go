package fuse_test

import (
	"bytes"
	"context"
	"io"
	"iter"
	"testing"
	"time"

	storage "github.com/firetiger-oss/storage"
	storagefuse "github.com/firetiger-oss/storage/fuse"
	"github.com/firetiger-oss/storage/memory"
)

// errorBucket wraps a storage.Bucket and injects configurable errors on
// specific operations, enabling error-path testing without a mock framework.
type errorBucket struct {
	storage.Bucket
	headErr   error
	getErr    error
	putErr    error
	listErr   error
	deleteErr error
}

func (e *errorBucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if e.headErr != nil {
		return storage.ObjectInfo{}, e.headErr
	}
	return e.Bucket.HeadObject(ctx, key)
}

func (e *errorBucket) GetObject(ctx context.Context, key string, opts ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if e.getErr != nil {
		return nil, storage.ObjectInfo{}, e.getErr
	}
	return e.Bucket.GetObject(ctx, key, opts...)
}

func (e *errorBucket) PutObject(ctx context.Context, key string, value io.Reader, opts ...storage.PutOption) (storage.ObjectInfo, error) {
	if e.putErr != nil {
		return storage.ObjectInfo{}, e.putErr
	}
	return e.Bucket.PutObject(ctx, key, value, opts...)
}

func (e *errorBucket) ListObjects(ctx context.Context, opts ...storage.ListOption) iter.Seq2[storage.Object, error] {
	if e.listErr != nil {
		return func(yield func(storage.Object, error) bool) {
			yield(storage.Object{}, e.listErr)
		}
	}
	return e.Bucket.ListObjects(ctx, opts...)
}

func (e *errorBucket) WatchObjects(ctx context.Context, opts ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return e.Bucket.WatchObjects(ctx, opts...)
}

func (e *errorBucket) DeleteObject(ctx context.Context, key string) error {
	if e.deleteErr != nil {
		return e.deleteErr
	}
	return e.Bucket.DeleteObject(ctx, key)
}

func (e *errorBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return e.Bucket.DeleteObjects(ctx, objects)
}

func (e *errorBucket) CopyObject(ctx context.Context, from, to string, opts ...storage.PutOption) error {
	return e.Bucket.CopyObject(ctx, from, to, opts...)
}

func (e *errorBucket) PresignGetObject(ctx context.Context, key string, exp time.Duration, opts ...storage.GetOption) (string, error) {
	return e.Bucket.PresignGetObject(ctx, key, exp, opts...)
}

func (e *errorBucket) PresignPutObject(ctx context.Context, key string, exp time.Duration, opts ...storage.PutOption) (string, error) {
	return e.Bucket.PresignPutObject(ctx, key, exp, opts...)
}

func (e *errorBucket) PresignHeadObject(ctx context.Context, key string, exp time.Duration) (string, error) {
	return e.Bucket.PresignHeadObject(ctx, key, exp)
}

func (e *errorBucket) PresignDeleteObject(ctx context.Context, key string, exp time.Duration) (string, error) {
	return e.Bucket.PresignDeleteObject(ctx, key, exp)
}

func mountBucket(t *testing.T, bucket storage.Bucket) string {
	t.Helper()
	dir := t.TempDir()
	server, err := storagefuse.Mount(dir, bucket)
	if err != nil {
		t.Skipf("FUSE not available: %v", err)
	}
	t.Cleanup(func() {
		if err := server.Unmount(); err != nil {
			t.Logf("unmount: %v", err)
		}
		server.Wait()
	})
	return dir
}

func newBucket(t *testing.T) storage.Bucket {
	t.Helper()
	return memory.NewBucket()
}

func put(t *testing.T, bucket storage.Bucket, key string, data []byte) {
	t.Helper()
	if _, err := bucket.PutObject(t.Context(), key, bytes.NewReader(data)); err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
}
