package memory_test

import (
	"context"
	"iter"
	"strings"
	"testing"
	"time"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	storagetest "github.com/firetiger-oss/tigerblock/test"
)

func TestMemoryStorage(t *testing.T) {
	storagetest.TestStorage(t, func(*testing.T) (storage.Bucket, error) {
		return new(memory.Bucket), nil
	})
}

func TestPutObjectRejectsContentLengthMismatch(t *testing.T) {
	bucket := new(memory.Bucket)
	ctx := t.Context()

	body := strings.NewReader("twelve bytes")
	_, err := bucket.PutObject(ctx, "x", body, storage.ContentLength(999))
	if err == nil {
		t.Fatal("expected error from content length mismatch")
	}
	if !strings.Contains(err.Error(), "content length") {
		t.Fatalf("error = %v; want one mentioning content length", err)
	}

	// The object must not have been stored.
	if _, err := bucket.HeadObject(ctx, "x"); err == nil {
		t.Fatal("object stored despite content length mismatch")
	}
}

func TestPutObjectAcceptsMatchingContentLength(t *testing.T) {
	bucket := new(memory.Bucket)
	ctx := t.Context()

	body := strings.NewReader("twelve bytes")
	info, err := bucket.PutObject(ctx, "x", body, storage.ContentLength(int64(len("twelve bytes"))))
	if err != nil {
		t.Fatalf("PutObject: %v", err)
	}
	if info.Size != int64(len("twelve bytes")) {
		t.Fatalf("Size = %d; want %d", info.Size, len("twelve bytes"))
	}
}

func TestWatchDelimiterNotifiesOnNestedWrite(t *testing.T) {
	// Regression test: a WatchObjects call with a delimiter must still be
	// notified when a key is written that is deeper than the delimiter.
	// For example, writing "a/b/c.json" should wake a watcher on
	// prefix="a/" delimiter="/" because the write may create a new
	// common-prefix entry "a/b/" that wasn't in the previous listing.
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	bucket := new(memory.Bucket)

	// Start watching with a delimiter before any objects exist.
	next, stop := iter.Pull2(bucket.WatchObjects(ctx,
		storage.KeyPrefix("a/"),
		storage.KeyDelimiter("/"),
	))
	defer stop()

	// Write a nested key that the delimiter would collapse into "a/b/".
	if _, err := bucket.PutObject(ctx, "a/b/c.json", strings.NewReader("data")); err != nil {
		t.Fatal("PutObject:", err)
	}

	// The watcher should yield the common-prefix entry "a/b/".
	obj, err, more := next()
	if !more {
		t.Fatal("watch iterator ended unexpectedly")
	}
	if err != nil {
		t.Fatal("unexpected watch error:", err)
	}
	if obj.Key != "a/b/" {
		t.Fatalf("expected key %q, got %q", "a/b/", obj.Key)
	}
}

func TestWatchDelimiterNotifiesOnDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	bucket := new(memory.Bucket)

	// Seed two nested keys so the common-prefix "a/b/" exists.
	for _, key := range []string{"a/b/1.json", "a/b/2.json"} {
		if _, err := bucket.PutObject(ctx, key, strings.NewReader("data")); err != nil {
			t.Fatal("PutObject:", err)
		}
	}

	next, stop := iter.Pull2(bucket.WatchObjects(ctx,
		storage.KeyPrefix("a/"),
		storage.KeyDelimiter("/"),
	))
	defer stop()

	// Consume the initial "a/b/" listing entry.
	obj, err, more := next()
	if !more || err != nil {
		t.Fatalf("expected initial listing, got more=%v err=%v", more, err)
	}
	if obj.Key != "a/b/" {
		t.Fatalf("expected initial key %q, got %q", "a/b/", obj.Key)
	}

	// Delete both nested keys — the common-prefix entry should disappear.
	for _, key := range []string{"a/b/1.json", "a/b/2.json"} {
		if err := bucket.DeleteObject(ctx, key); err != nil {
			t.Fatal("DeleteObject:", err)
		}
	}

	// The watcher should yield a deletion marker for "a/b/".
	obj, err, more = next()
	if !more {
		t.Fatal("watch iterator ended unexpectedly")
	}
	if err != nil {
		t.Fatal("unexpected watch error:", err)
	}
	if obj.Key != "a/b/" || obj.Size != -1 {
		t.Fatalf("expected deletion of %q (size -1), got key=%q size=%d", "a/b/", obj.Key, obj.Size)
	}
}
