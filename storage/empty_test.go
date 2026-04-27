package storage_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/firetiger-oss/tigerblock/internal/sequtil"
	"github.com/firetiger-oss/tigerblock/storage"
)

func TestEmptyBucket(t *testing.T) {
	ctx := t.Context()
	bucket := storage.EmptyBucket()

	t.Run("Location", func(t *testing.T) {
		location := bucket.Location()
		if location != ":none:" {
			t.Errorf("expected location %q, got %q", ":none:", location)
		}
	})

	t.Run("Access", func(t *testing.T) {
		err := bucket.Access(ctx)
		if err != nil {
			t.Errorf("expected no error for Access, got %v", err)
		}
	})

	t.Run("Create", func(t *testing.T) {
		err := bucket.Create(ctx)
		if !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("expected ErrBucketReadOnly for Create, got %v", err)
		}
	})

	t.Run("HeadObject", func(t *testing.T) {
		// Test with valid key
		_, err := bucket.HeadObject(ctx, "test/object.txt")
		if !errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("expected ErrObjectNotFound for HeadObject, got %v", err)
		}

		// Test with invalid key
		_, err = bucket.HeadObject(ctx, "")
		if !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected ErrInvalidObjectKey for invalid key, got %v", err)
		}
	})

	t.Run("GetObject", func(t *testing.T) {
		// Test with valid key
		_, _, err := bucket.GetObject(ctx, "test/object.txt")
		if !errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("expected ErrObjectNotFound for GetObject, got %v", err)
		}

		// Test with invalid key
		_, _, err = bucket.GetObject(ctx, "")
		if !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected ErrInvalidObjectKey for invalid key, got %v", err)
		}
	})

	t.Run("PutObject", func(t *testing.T) {
		reader := strings.NewReader("test content")
		_, err := bucket.PutObject(ctx, "test/object.txt", reader)
		if !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("expected ErrBucketReadOnly for PutObject, got %v", err)
		}
	})

	t.Run("DeleteObject", func(t *testing.T) {
		err := bucket.DeleteObject(ctx, "test/object.txt")
		if !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("expected ErrBucketReadOnly for DeleteObject, got %v", err)
		}
	})

	t.Run("DeleteObjects", func(t *testing.T) {
		keys := []string{"test/object1.txt", "test/object2.txt"}
		results := bucket.DeleteObjects(ctx, sequtil.Values(keys))
		for _, err := range results {
			if !errors.Is(err, storage.ErrBucketReadOnly) {
				t.Errorf("expected ErrBucketReadOnly for DeleteObjects, got %v", err)
			}
		}
	})

	t.Run("ListObjects", func(t *testing.T) {
		count := 0
		for object, err := range bucket.ListObjects(ctx) {
			if err != nil {
				t.Errorf("unexpected error in ListObjects: %v", err)
				break
			}
			count++
			_ = object // Should never execute
		}
		if count != 0 {
			t.Errorf("expected 0 objects, got %d", count)
		}
	})

	t.Run("WatchObjects", func(t *testing.T) {
		// Test that WatchObjects respects context cancellation
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		count := 0
		for object, err := range bucket.WatchObjects(ctx) {
			if err != nil {
				// Should get context.DeadlineExceeded
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("expected context.DeadlineExceeded, got %v", err)
				}
				break
			}
			count++
			_ = object // Should never execute unless there's an error
		}
		if count != 0 {
			t.Errorf("expected 0 objects before timeout, got %d", count)
		}
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		// Test that all methods respect context cancellation
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel immediately

		// Test a few key methods
		err := bucket.Access(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled for Access, got %v", err)
		}

		_, err = bucket.HeadObject(ctx, "test.txt")
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled for HeadObject, got %v", err)
		}

		_, _, err = bucket.GetObject(ctx, "test.txt")
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled for GetObject, got %v", err)
		}

		_, err = bucket.PresignGetObject(ctx, "test.txt", time.Hour)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled for PresignGetObject, got %v", err)
		}

		// Test ListObjects
		for _, err := range bucket.ListObjects(ctx) {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("expected context.Canceled for ListObjects, got %v", err)
			}
			break // Should only yield the error
		}
	})

	t.Run("PresignGetObject", func(t *testing.T) {
		_, err := bucket.PresignGetObject(ctx, "test/object.txt", time.Hour)
		if !errors.Is(err, storage.ErrPresignNotSupported) {
			t.Errorf("expected ErrPresignNotSupported for PresignGetObject, got %v", err)
		}
	})

	t.Run("PresignPutObject", func(t *testing.T) {
		_, err := bucket.PresignPutObject(ctx, "test/object.txt", time.Hour)
		if !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("expected ErrBucketReadOnly for PresignPutObject, got %v", err)
		}
	})

	t.Run("PresignHeadObject", func(t *testing.T) {
		_, err := bucket.PresignHeadObject(ctx, "test/object.txt", time.Hour)
		if !errors.Is(err, storage.ErrPresignNotSupported) {
			t.Errorf("expected ErrPresignNotSupported for PresignHeadObject, got %v", err)
		}
	})

	t.Run("PresignDeleteObject", func(t *testing.T) {
		_, err := bucket.PresignDeleteObject(ctx, "test/object.txt", time.Hour)
		if !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("expected ErrBucketReadOnly for PresignDeleteObject, got %v", err)
		}
	})
}
