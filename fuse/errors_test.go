package fuse_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	storage "github.com/firetiger-oss/storage"
)

// TestReadErrorPropagation verifies that a GetObject error surfaces through
// the FUSE layer as os.ErrNotExist when the object is not found.
func TestReadErrorPropagation(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	eb := &errorBucket{Bucket: bucket, getErr: storage.ErrObjectNotFound}
	dir := mountBucket(t, eb)

	if _, err := os.ReadFile(filepath.Join(dir, "foo.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected not-exist error, got %v", err)
	}
}

// TestWriteToReadOnlyBucket verifies that a PutObject error (ErrBucketReadOnly)
// surfaces as an error when closing a written file.
func TestWriteToReadOnlyBucket(t *testing.T) {
	bucket := newBucket(t)
	eb := &errorBucket{Bucket: bucket, putErr: storage.ErrBucketReadOnly}
	dir := mountBucket(t, eb)

	if err := os.WriteFile(filepath.Join(dir, "out.txt"), []byte("data"), 0644); err == nil {
		t.Fatal("expected error writing to read-only bucket, got nil")
	}
}

// TestReaddirError verifies that a ListObjects error propagates through Readdir.
func TestReaddirError(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("x"))
	eb := &errorBucket{Bucket: bucket, listErr: storage.ErrBucketNotFound}
	dir := mountBucket(t, eb)

	if _, err := os.ReadDir(dir); err == nil {
		t.Fatal("expected error from ReadDir, got nil")
	}
}

// TestLookupHeadObjectError verifies that a non-ENOENT HeadObject error during
// Lookup propagates as a non-ErrNotExist error (not silently treated as missing).
func TestLookupHeadObjectError(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("x"))
	eb := &errorBucket{Bucket: bucket, headErr: storage.ErrTooManyRequests}
	dir := mountBucket(t, eb)

	_, err := os.Stat(filepath.Join(dir, "foo.txt"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if os.IsNotExist(err) {
		t.Fatalf("expected non-ErrNotExist error, got ErrNotExist: %v", err)
	}
}

// TestUnlinkDeleteError verifies that a DeleteObject error propagates through
// the Unlink handler rather than being masked.
func TestUnlinkDeleteError(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	eb := &errorBucket{Bucket: bucket, deleteErr: storage.ErrObjectNotFound}
	dir := mountBucket(t, eb)

	err := os.Remove(filepath.Join(dir, "foo.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
}

// TestCopyOnOpenError verifies that a non-ENOENT GetObject error during
// copy-on-open (O_RDWR without O_TRUNC) propagates to the caller rather than
// silently producing an empty write buffer.
func TestCopyOnOpenError(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	eb := &errorBucket{Bucket: bucket, getErr: storage.ErrTooManyRequests}
	dir := mountBucket(t, eb)

	if _, err := os.OpenFile(filepath.Join(dir, "foo.txt"), os.O_RDWR, 0644); err == nil {
		t.Fatal("expected error opening file with failing GetObject, got nil")
	}
}

// TestTruncateRemoteGetObjectError verifies that a GetObject error in
// truncateRemote (no open fd, size > 0) propagates to the os.Truncate caller.
func TestTruncateRemoteGetObjectError(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello world"))
	eb := &errorBucket{Bucket: bucket, getErr: storage.ErrTooManyRequests}
	dir := mountBucket(t, eb)

	if err := os.Truncate(filepath.Join(dir, "foo.txt"), 5); err == nil {
		t.Fatal("expected error from Truncate when GetObject fails, got nil")
	}
}

// TestTruncateRemotePutObjectError verifies that a PutObject error in
// truncateRemote propagates to the os.Truncate caller.
func TestTruncateRemotePutObjectError(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello world"))
	eb := &errorBucket{Bucket: bucket, putErr: storage.ErrBucketReadOnly}
	dir := mountBucket(t, eb)

	if err := os.Truncate(filepath.Join(dir, "foo.txt"), 5); err == nil {
		t.Fatal("expected error from Truncate when PutObject fails, got nil")
	}
}

// TestTruncateRemotePreservesMetadata verifies that truncateRemote preserves
// ContentType, CacheControl, and custom metadata on re-upload.
func TestTruncateRemotePreservesMetadata(t *testing.T) {
	bucket := newBucket(t)
	if _, err := bucket.PutObject(t.Context(), "foo.txt",
		bytes.NewReader([]byte("hello world")),
		storage.ContentType("text/plain"),
		storage.CacheControl("no-cache"),
		storage.Metadata("x-custom", "preserved"),
	); err != nil {
		t.Fatal(err)
	}
	dir := mountBucket(t, bucket)

	if err := os.Truncate(filepath.Join(dir, "foo.txt"), 5); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte("hello"); string(got) != string(want) {
		t.Fatalf("content: got %q, want %q", got, want)
	}

	info, err := bucket.HeadObject(t.Context(), "foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.ContentType != "text/plain" {
		t.Errorf("ContentType: got %q, want %q", info.ContentType, "text/plain")
	}
	if info.CacheControl != "no-cache" {
		t.Errorf("CacheControl: got %q, want %q", info.CacheControl, "no-cache")
	}
	if info.Metadata["x-custom"] != "preserved" {
		t.Errorf("Metadata[x-custom]: got %q, want %q", info.Metadata["x-custom"], "preserved")
	}
}
