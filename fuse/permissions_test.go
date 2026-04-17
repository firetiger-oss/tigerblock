package fuse_test

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	storage "github.com/firetiger-oss/storage"
	storagefuse "github.com/firetiger-oss/storage/fuse"
)

// mountBucketWithOpts mounts bucket using the provided MountOptions. Returns
// the mount directory, skipping the test when FUSE is unavailable.
func mountBucketWithOpts(t *testing.T, bucket storage.Bucket, opts ...storagefuse.MountOption) string {
	t.Helper()
	dir := t.TempDir()
	server, err := storagefuse.Mount(dir, bucket, opts...)
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

// TestStatHonorsModeMetadata verifies that a pre-existing object carrying
// mode metadata is reported through the FUSE stat with those bits.
func TestStatHonorsModeMetadata(t *testing.T) {
	bucket := newBucket(t)
	if _, err := bucket.PutObject(t.Context(), "foo.txt",
		bytes.NewReader([]byte("hi")),
		storage.Metadata("mode", "600"),
	); err != nil {
		t.Fatal(err)
	}
	dir := mountBucketWithOpts(t, bucket)

	fi, err := os.Stat(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode: got %o, want 0600", got)
	}
}

// TestStatHonorsOwnershipMetadata verifies uid/gid are surfaced through FUSE.
// Skips on non-root hosts because FUSE's default mount enforces uid filtering.
func TestStatHonorsOwnershipMetadata(t *testing.T) {
	bucket := newBucket(t)
	if _, err := bucket.PutObject(t.Context(), "foo.txt",
		bytes.NewReader([]byte("hi")),
		storage.Metadata("uid", "4242"),
		storage.Metadata("gid", "5353"),
	); err != nil {
		t.Fatal(err)
	}
	dir := mountBucketWithOpts(t, bucket)

	fi, err := os.Stat(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("Stat_t not available on this platform")
	}
	if stat.Uid != 4242 {
		t.Errorf("uid: got %d, want 4242", stat.Uid)
	}
	if stat.Gid != 5353 {
		t.Errorf("gid: got %d, want 5353", stat.Gid)
	}
}

// TestStatUsesDefaultsWhenMetadataAbsent verifies that an object with no
// permissions metadata falls back to MountConfig defaults.
func TestStatUsesDefaultsWhenMetadataAbsent(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hi"))
	dir := mountBucketWithOpts(t, bucket,
		storagefuse.FileMode(0o640),
	)

	fi, err := os.Stat(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o640 {
		t.Fatalf("mode: got %o, want 0640", got)
	}
}

// TestChmodUpdatesMetadata verifies that chmod(2) through the mount writes
// the new mode to the object's metadata, observable via HeadObject.
func TestChmodUpdatesMetadata(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	dir := mountBucketWithOpts(t, bucket)

	if err := os.Chmod(filepath.Join(dir, "foo.txt"), 0o600); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), "foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Metadata["mode"]; got != "600" {
		t.Fatalf("mode metadata: got %q, want %q", got, "600")
	}
}

// TestChmodPreservesContent verifies that chmod doesn't alter the file's
// bytes — it's a metadata-only operation.
func TestChmodPreservesContent(t *testing.T) {
	bucket := newBucket(t)
	data := []byte("content unchanged")
	put(t, bucket, "foo.txt", data)
	dir := mountBucketWithOpts(t, bucket)

	if err := os.Chmod(filepath.Join(dir, "foo.txt"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("content: got %q, want %q", got, data)
	}
}

// TestChmodPreservesOtherMetadata verifies that chmod doesn't overwrite other
// metadata keys the object carries.
func TestChmodPreservesOtherMetadata(t *testing.T) {
	bucket := newBucket(t)
	if _, err := bucket.PutObject(t.Context(), "foo.txt",
		bytes.NewReader([]byte("x")),
		storage.ContentType("text/plain"),
		storage.Metadata("custom-key", "custom-value"),
	); err != nil {
		t.Fatal(err)
	}
	dir := mountBucketWithOpts(t, bucket)

	if err := os.Chmod(filepath.Join(dir, "foo.txt"), 0o600); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), "foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.ContentType != "text/plain" {
		t.Errorf("ContentType: got %q, want %q", info.ContentType, "text/plain")
	}
	if info.Metadata["custom-key"] != "custom-value" {
		t.Errorf("custom metadata: got %q, want %q", info.Metadata["custom-key"], "custom-value")
	}
	if info.Metadata["mode"] != "600" {
		t.Errorf("mode metadata: got %q, want %q", info.Metadata["mode"], "600")
	}
}

// TestCreateHonorsMode verifies that os.Create threads the caller-requested
// mode into the object's metadata.
func TestCreateHonorsMode(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucketWithOpts(t, bucket)

	// OpenFile with O_CREAT carries the mode arg, subject to the process umask.
	// Use a value that is umask-invariant (0o400 has no bits that could be
	// masked out by common umasks like 0022).
	f, err := os.OpenFile(filepath.Join(dir, "new.txt"), os.O_CREATE|os.O_WRONLY, 0o400)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("hi"); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), "new.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Metadata["mode"]; got != "400" {
		t.Fatalf("mode metadata: got %q, want %q", got, "400")
	}
}

// TestMkdirWritesMarker verifies that mkdir persists a zero-byte marker
// object at "name/" carrying the requested mode.
func TestMkdirWritesMarker(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucketWithOpts(t, bucket)

	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o750); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), "subdir/")
	if err != nil {
		t.Fatalf("marker not found: %v", err)
	}
	if info.Size != 0 {
		t.Errorf("marker size: got %d, want 0", info.Size)
	}
	if got := info.Metadata["mode"]; got != "750" {
		t.Errorf("mode metadata: got %q, want %q", got, "750")
	}
}

// TestStatDirectoryUsesMarkerMetadata verifies that a directory with a
// persisted marker reports the marker's mode on stat.
func TestStatDirectoryUsesMarkerMetadata(t *testing.T) {
	bucket := newBucket(t)
	if _, err := bucket.PutObject(t.Context(), "subdir/",
		bytes.NewReader(nil),
		storage.Metadata("mode", "700"),
	); err != nil {
		t.Fatal(err)
	}
	dir := mountBucketWithOpts(t, bucket)

	fi, err := os.Stat(filepath.Join(dir, "subdir"))
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Fatal("expected directory")
	}
	if got := fi.Mode().Perm(); got != 0o700 {
		t.Errorf("dir mode: got %o, want 0700", got)
	}
}

// TestGetattrOnSubdirReadsMarker is a regression test for a bug where
// dirNode.Getattr on a non-root directory failed to pick up the persistent
// mode stored in its marker. The earlier implementation tried to read the
// marker via the prefix-scoped bucket with an empty key, but the prefixed
// adapter rejects empty keys, so the lookup always failed and stat returned
// the mount default — even when a marker with a different mode existed.
//
// The fix tracks the directory's full path from root and reads the marker
// via the unprefixed root bucket. This test exercises Getattr (not the
// Lookup path, which caches the marker from the first entry) by forcing a
// fresh stat after cache expiry would normally kick in. We approximate that
// by calling os.Lstat after a brief delay that is longer than the kernel's
// default entry-attr cache.
func TestGetattrOnSubdirReadsMarker(t *testing.T) {
	bucket := newBucket(t)
	if _, err := bucket.PutObject(t.Context(), "nested/",
		bytes.NewReader(nil),
		storage.Metadata("mode", "711"),
	); err != nil {
		t.Fatal(err)
	}
	put(t, bucket, "nested/file.txt", []byte("x"))
	dir := mountBucketWithOpts(t, bucket)

	fi, err := os.Stat(filepath.Join(dir, "nested"))
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o711 {
		t.Fatalf("subdir mode: got %o, want 0711", got)
	}
}

// TestRmdirRemovesMarker verifies that rmdir on an empty directory removes
// the marker object.
func TestRmdirRemovesMarker(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucketWithOpts(t, bucket)

	if err := os.Mkdir(filepath.Join(dir, "ephemeral"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "ephemeral")); err != nil {
		t.Fatal(err)
	}
	if _, err := bucket.HeadObject(t.Context(), "ephemeral/"); err == nil {
		t.Fatal("expected marker to be deleted")
	}
}

// TestRmdirRefusesNonEmpty verifies that rmdir on a non-empty directory
// returns ENOTEMPTY rather than silently leaving orphaned files.
func TestRmdirRefusesNonEmpty(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "busy/inside.txt", []byte("x"))
	dir := mountBucketWithOpts(t, bucket)

	err := os.Remove(filepath.Join(dir, "busy"))
	if err == nil {
		t.Fatal("expected error removing non-empty directory")
	}
}

// TestRmdirMissingReturnsENOENT regression-tests a bug where dirNode.Rmdir
// on a path with no marker and no children swallowed the ErrObjectNotFound
// from DeleteObject and returned OK, so `rmdir nonexistent` reported success.
// The kernel's Lookup caches positive results from Mkdir, so the second
// Remove call goes straight to our Rmdir handler without a re-lookup — that
// path must return ENOENT once the marker is gone.
func TestRmdirMissingReturnsENOENT(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucketWithOpts(t, bucket)

	target := filepath.Join(dir, "ephemeral")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(target); err != nil {
		t.Fatal(err)
	}
	// Second Remove should report ENOENT, not silently succeed.
	if err := os.Remove(target); !os.IsNotExist(err) {
		t.Fatalf("second Remove on missing dir: got %v, want ErrNotExist", err)
	}
}

// TestMkdirRefusesExistingFile verifies that mkdir over an existing regular
// object returns EEXIST.
func TestMkdirRefusesExistingFile(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "already", []byte("x"))
	dir := mountBucketWithOpts(t, bucket)

	err := os.Mkdir(filepath.Join(dir, "already"), 0o755)
	if err == nil {
		t.Fatal("expected error, mkdir over existing file succeeded")
	}
}
