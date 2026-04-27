package storage_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/internal/sequtil"
	"github.com/firetiger-oss/tigerblock/storage/memory"
)

func TestReadOnlyBucket(t *testing.T) {
	bucket := storage.ReadOnlyBucket(new(memory.Bucket))
	assert := func(err error) {
		if !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Helper()
			t.Fatal("expected ErrBucketReadOnly, got", err)
		}
	}
	ctx := t.Context()
	assert(bucket.Create(ctx))
	assert(bucket.DeleteObject(ctx, "key"))
	// Test DeleteObjects returns error for each key
	for _, err := range bucket.DeleteObjects(ctx, sequtil.Values([]string{"key1", "key2"})) {
		assert(err)
	}
	_, err := bucket.PutObject(ctx, "key", strings.NewReader("value"))
	assert(err)
}
