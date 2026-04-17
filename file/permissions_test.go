package file_test

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/file"
	"golang.org/x/sys/unix"
)

func newFileBucket(t *testing.T) storage.Bucket {
	t.Helper()
	root := filepath.Join(t.TempDir(), "bucket")
	bucket, err := file.NewRegistry(root).LoadBucket(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	if err := bucket.Create(t.Context()); err != nil {
		t.Fatal(err)
	}
	return bucket
}

// fileBucketRoot returns the on-disk directory backing bucket so tests can
// poke at the raw filesystem. Only works for buckets created via NewRegistry.
func fileBucketRoot(bucket storage.Bucket) string {
	loc := bucket.Location()
	return strings.TrimSuffix(strings.TrimPrefix(loc, "file://"), "/")
}

// TestHeadObjectReportsInodePerms verifies that mode/uid/gid surfaced via
// ObjectInfo.Metadata reflect the real inode, not a stored JSON blob.
func TestHeadObjectReportsInodePerms(t *testing.T) {
	bucket := newFileBucket(t)
	const key = "foo.txt"

	if _, err := bucket.PutObject(t.Context(), key, strings.NewReader("hi")); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	for _, k := range []string{"mode", "uid", "gid"} {
		if _, ok := info.Metadata[k]; !ok {
			t.Errorf("missing %q in metadata: %v", k, info.Metadata)
		}
	}

	// Confirm uid/gid match the calling process — PutObject creates the file
	// without trying to chown it.
	wantUID := strconv.FormatUint(uint64(os.Getuid()), 10)
	wantGID := strconv.FormatUint(uint64(os.Getgid()), 10)
	if got := info.Metadata["uid"]; got != wantUID {
		t.Errorf("uid: got %q, want %q", got, wantUID)
	}
	if got := info.Metadata["gid"]; got != wantGID {
		t.Errorf("gid: got %q, want %q", got, wantGID)
	}
}

// TestPutObjectModeAppliedViaFchmod verifies that mode metadata passed to
// PutObject is applied to the real inode via fchmod, not just stored in the
// JSON xattr.
func TestPutObjectModeAppliedViaFchmod(t *testing.T) {
	bucket := newFileBucket(t)
	const key = "locked.txt"

	if _, err := bucket.PutObject(t.Context(), key, strings.NewReader("x"),
		storage.Metadata("mode", "600"),
	); err != nil {
		t.Fatal(err)
	}

	// Stat the underlying file directly — bypassing ObjectInfo — to confirm
	// the kernel sees the new mode.
	fi, err := os.Stat(filepath.Join(fileBucketRoot(bucket), key))
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Fatalf("on-disk mode: got %o, want 0600", got)
	}

	info, err := bucket.HeadObject(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Metadata["mode"]; got != "600" {
		t.Fatalf("metadata mode: got %q, want %q", got, "600")
	}
}

// TestCopyObjectSameKeyDoesNotRewriteBytes verifies that an in-place same-key
// CopyObject updates permissions without changing the file's bytes —
// exercising the fast-path that avoids re-copying bytes.
func TestCopyObjectSameKeyDoesNotRewriteBytes(t *testing.T) {
	bucket := newFileBucket(t)
	const key = "pin.txt"
	const data = "bytes that should not be rewritten"

	if _, err := bucket.PutObject(t.Context(), key, strings.NewReader(data)); err != nil {
		t.Fatal(err)
	}

	before, err := bucket.HeadObject(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	if err := bucket.CopyObject(t.Context(), key, key,
		storage.Metadata("mode", "640"),
	); err != nil {
		t.Fatal(err)
	}

	after, err := bucket.HeadObject(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}

	if before.ETag != "" && before.ETag != after.ETag {
		t.Errorf("ETag changed across same-key copy: %q -> %q", before.ETag, after.ETag)
	}
	if after.Size != before.Size {
		t.Errorf("Size changed: %d -> %d", before.Size, after.Size)
	}
	if got := after.Metadata["mode"]; got != "640" {
		t.Errorf("mode not applied: got %q, want %q", got, "640")
	}

	rc, _, err := bucket.GetObject(t.Context(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	buf := make([]byte, 128)
	n, _ := rc.Read(buf)
	if string(buf[:n]) != data {
		t.Errorf("content: got %q, want %q", buf[:n], data)
	}
}

// TestExternallyCreatedFileCarriesInodePerms verifies that a file dropped
// into the bucket root outside of PutObject (e.g. by another process) still
// surfaces its actual inode perms through HeadObject.
func TestExternallyCreatedFileCarriesInodePerms(t *testing.T) {
	bucket := newFileBucket(t)
	path := filepath.Join(fileBucketRoot(bucket), "external.txt")
	if err := os.WriteFile(path, []byte("hi"), 0o640); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), "external.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Metadata["mode"]; got != "640" {
		t.Errorf("mode: got %q, want %q", got, "640")
	}
}

// TestCopyObjectDirToDir verifies that CopyObject works when both source and
// destination are directory markers. The initial file-backend CopyObject
// opened the source (a directory) and tried to Seek on its fd, which fails
// with EINVAL on directory file descriptors — so every cross-key dir copy
// used to error instead of materializing a destination marker with the
// source's metadata.
func TestCopyObjectDirToDir(t *testing.T) {
	bucket := newFileBucket(t)
	ctx := t.Context()

	if _, err := bucket.PutObject(ctx, "src/", strings.NewReader(""),
		storage.Metadata("custom", "from-src"),
	); err != nil {
		t.Fatal(err)
	}

	if err := bucket.CopyObject(ctx, "src/", "dst/"); err != nil {
		t.Fatalf("CopyObject(src/, dst/): %v", err)
	}

	info, err := bucket.HeadObject(ctx, "dst/")
	if err != nil {
		t.Fatalf("HeadObject(dst/): %v", err)
	}
	if info.Size != 0 {
		t.Errorf("dest size: got %d, want 0", info.Size)
	}
	if info.Metadata["custom"] != "from-src" {
		t.Errorf("dest metadata[custom]: got %q, want %q", info.Metadata["custom"], "from-src")
	}

	fi, err := os.Stat(filepath.Join(fileBucketRoot(bucket), "dst"))
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Errorf("dst should be a real directory, got mode %v", fi.Mode())
	}
}

// TestCopyObjectSameKeyHonorsConditionals regression-tests the in-place
// same-key fast path. Before this fix the path ignored IfMatch / IfNoneMatch,
// so same-key conditional copies always succeeded even when the caller asked
// for strict preconditions (e.g. "only update if ETag matches X").
func TestCopyObjectSameKeyHonorsConditionals(t *testing.T) {
	bucket := newFileBucket(t)
	ctx := t.Context()
	const key = "target.txt"
	if _, err := bucket.PutObject(ctx, key, strings.NewReader("body")); err != nil {
		t.Fatal(err)
	}
	info, err := bucket.HeadObject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// IfNoneMatch="*" on an existing object must fail.
	if err := bucket.CopyObject(ctx, key, key,
		storage.Metadata("mode", "600"),
		storage.IfNoneMatch("*"),
	); !errors.Is(err, storage.ErrObjectNotMatch) {
		t.Errorf("IfNoneMatch=*: got %v, want ErrObjectNotMatch", err)
	}

	// IfMatch with a mismatched ETag must fail.
	if err := bucket.CopyObject(ctx, key, key,
		storage.Metadata("mode", "600"),
		storage.IfMatch("deadbeef"),
	); !errors.Is(err, storage.ErrObjectNotMatch) {
		t.Errorf("IfMatch=deadbeef: got %v, want ErrObjectNotMatch", err)
	}

	// IfMatch with the correct ETag must succeed.
	if err := bucket.CopyObject(ctx, key, key,
		storage.Metadata("mode", "600"),
		storage.IfMatch(info.ETag),
	); err != nil {
		t.Errorf("IfMatch=<current etag>: %v", err)
	}

	// IfNoneMatch with a value other than "*" is invalid.
	if err := bucket.CopyObject(ctx, key, key,
		storage.IfNoneMatch("deadbeef"),
	); !errors.Is(err, storage.ErrInvalidObjectTag) {
		t.Errorf("IfNoneMatch=deadbeef: got %v, want ErrInvalidObjectTag", err)
	}
}

// TestCopyObjectDirToFileRejected covers the semantic constraint that a POSIX
// filesystem can't have "foo" and "foo/" share an inode: attempting a copy
// that mixes a dir source with a non-dir dest (or vice versa) must fail
// cleanly rather than silently producing a garbage destination.
func TestCopyObjectDirToFileRejected(t *testing.T) {
	bucket := newFileBucket(t)
	ctx := t.Context()
	if _, err := bucket.PutObject(ctx, "d/", strings.NewReader("")); err != nil {
		t.Fatal(err)
	}

	if err := bucket.CopyObject(ctx, "d/", "f"); !errors.Is(err, storage.ErrInvalidObjectKey) {
		t.Errorf("dir→file: got %v, want ErrInvalidObjectKey", err)
	}

	if _, err := bucket.PutObject(ctx, "file", strings.NewReader("body")); err != nil {
		t.Fatal(err)
	}
	if err := bucket.CopyObject(ctx, "file", "newdir/"); !errors.Is(err, storage.ErrInvalidObjectKey) {
		t.Errorf("file→dir: got %v, want ErrInvalidObjectKey", err)
	}
}

// TestPutDirConditional covers IfMatch/IfNoneMatch semantics on directory
// markers. The initial putDir implementation returned early before the
// regular PutObject's precondition check, silently ignoring these options
// and letting callers re-create a marker that was meant to be "create only",
// or succeed on an IfMatch against a fresh key. All three regressions are
// now rejected.
func TestPutDirConditional(t *testing.T) {
	bucket := newFileBucket(t)
	ctx := t.Context()

	// IfNoneMatch="*" on a fresh key succeeds.
	if _, err := bucket.PutObject(ctx, "d/", strings.NewReader(""),
		storage.IfNoneMatch("*"),
	); err != nil {
		t.Fatalf("initial create with IfNoneMatch=*: %v", err)
	}

	// IfNoneMatch="*" on an existing marker must fail with ErrObjectNotMatch.
	if _, err := bucket.PutObject(ctx, "d/", strings.NewReader(""),
		storage.IfNoneMatch("*"),
	); !errors.Is(err, storage.ErrObjectNotMatch) {
		t.Errorf("recreate with IfNoneMatch=*: got %v, want ErrObjectNotMatch", err)
	}

	// IfNoneMatch with any value other than "*" is invalid.
	if _, err := bucket.PutObject(ctx, "e/", strings.NewReader(""),
		storage.IfNoneMatch("deadbeef"),
	); !errors.Is(err, storage.ErrInvalidObjectTag) {
		t.Errorf("IfNoneMatch=deadbeef: got %v, want ErrInvalidObjectTag", err)
	}

	// IfMatch is not meaningful on directory markers.
	if _, err := bucket.PutObject(ctx, "d/", strings.NewReader(""),
		storage.IfMatch("deadbeef"),
	); !errors.Is(err, storage.ErrInvalidObjectTag) {
		t.Errorf("IfMatch on marker: got %v, want ErrInvalidObjectTag", err)
	}
}

// TestMetadataXattrDoesNotStoreModeUIDGID confirms the design invariant:
// mode/uid/gid are reflected from the inode, never duplicated in the JSON blob.
func TestMetadataXattrDoesNotStoreModeUIDGID(t *testing.T) {
	bucket := newFileBucket(t)
	const key = "check.txt"

	if _, err := bucket.PutObject(t.Context(), key, strings.NewReader("x"),
		storage.Metadata("mode", "600"),
		storage.Metadata("custom", "v"),
	); err != nil {
		t.Fatal(err)
	}

	blob := readXattr(t, filepath.Join(fileBucketRoot(bucket), key), "user.storage.metadata")
	for _, forbidden := range []string{`"mode"`, `"uid"`, `"gid"`} {
		if strings.Contains(string(blob), forbidden) {
			t.Errorf("xattr JSON leaked %s: %s", forbidden, blob)
		}
	}
	if !strings.Contains(string(blob), `"custom"`) {
		t.Errorf("xattr JSON missing custom key: %s", blob)
	}
}

func readXattr(t *testing.T, path, name string) []byte {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fd := int(f.Fd())
	size, err := unix.Fgetxattr(fd, name, nil)
	if err != nil {
		t.Fatalf("fgetxattr size: %v", err)
	}
	if size == 0 {
		return nil
	}
	buf := make([]byte, size)
	n, err := unix.Fgetxattr(fd, name, buf)
	if err != nil {
		t.Fatalf("fgetxattr read: %v", err)
	}
	return buf[:n]
}
