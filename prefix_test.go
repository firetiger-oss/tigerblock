package storage_test

import (
	"bytes"
	"errors"
	"testing"

	storage "github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/memory"
)

// TestPrefixedBucketValidatesUserKey guards against a regression in which the
// prefixed adapter, relying on backend validation of the concatenated key,
// could silently accept invalid user-facing keys when the prefix happened to
// make the combined result valid. After relaxing ValidObjectKey to allow
// trailing-slash keys, an empty user key with a trailing-slash prefix would
// pass through as "prefix/" and reach the backend. The adapter must reject
// the user key directly.
func TestPrefixedBucketValidatesUserKey(t *testing.T) {
	ctx := t.Context()
	scoped := storage.Prefix(memory.NewBucket(), "scope/")

	cases := []string{"", "/", "//", "../escape", "a/../b"}
	for _, key := range cases {
		if _, err := scoped.HeadObject(ctx, key); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("HeadObject(%q): got %v, want ErrInvalidObjectKey", key, err)
		}
		if _, err := scoped.PutObject(ctx, key, bytes.NewReader(nil)); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("PutObject(%q): got %v, want ErrInvalidObjectKey", key, err)
		}
		if err := scoped.DeleteObject(ctx, key); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("DeleteObject(%q): got %v, want ErrInvalidObjectKey", key, err)
		}
		if err := scoped.CopyObject(ctx, key, "dest"); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("CopyObject(%q, dest): got %v, want ErrInvalidObjectKey", key, err)
		}
	}
}

// TestPrefixedBucketDoesNotAcceptPrefixAsTrailingMarker ensures that even
// though ValidObjectKey now accepts "scope/" as a valid key, the prefixed
// bucket does not let callers address it via an empty user key.
func TestPrefixedBucketDoesNotAcceptPrefixAsTrailingMarker(t *testing.T) {
	ctx := t.Context()
	bucket := memory.NewBucket()
	// Put the marker directly at the root bucket so we know it's there.
	if _, err := bucket.PutObject(ctx, "scope/", bytes.NewReader(nil)); err != nil {
		t.Fatal(err)
	}
	scoped := storage.Prefix(bucket, "scope/")

	if _, err := scoped.HeadObject(ctx, ""); !errors.Is(err, storage.ErrInvalidObjectKey) {
		t.Errorf("HeadObject(\"\"): got %v, want ErrInvalidObjectKey", err)
	}
}
