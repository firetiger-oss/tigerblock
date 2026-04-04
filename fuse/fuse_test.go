package fuse_test

import (
	"bytes"
	"context"
	"io"
	"iter"
	"os"
	"path/filepath"
	"sync"
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
	server, err := storagefuse.Mount(dir, bucket, nil)
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

func TestReadFile(t *testing.T) {
	bucket := newBucket(t)
	want := []byte("hello, world")
	put(t, bucket, "foo.txt", want)

	dir := mountBucket(t, bucket)
	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestWriteNewFile(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	data := []byte("new content")
	if err := os.WriteFile(filepath.Join(dir, "out.txt"), data, 0644); err != nil {
		t.Fatal(err)
	}

	rc, _, err := bucket.GetObject(t.Context(), "out.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(rc); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Fatalf("got %q, want %q", buf.Bytes(), data)
	}
}

func TestOverwriteFile(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("old"))
	dir := mountBucket(t, bucket)

	newData := []byte("new content here")
	if err := os.WriteFile(filepath.Join(dir, "foo.txt"), newData, 0644); err != nil {
		t.Fatal(err)
	}

	rc, _, err := bucket.GetObject(t.Context(), "foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	var buf bytes.Buffer
	buf.ReadFrom(rc)
	if !bytes.Equal(buf.Bytes(), newData) {
		t.Fatalf("got %q, want %q", buf.Bytes(), newData)
	}
}

func TestStatFile(t *testing.T) {
	bucket := newBucket(t)
	data := []byte("12345")
	put(t, bucket, "foo.txt", data)
	dir := mountBucket(t, bucket)

	fi, err := os.Stat(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.IsDir() {
		t.Fatal("expected regular file, got directory")
	}
	if fi.Size() != int64(len(data)) {
		t.Fatalf("got size %d, want %d", fi.Size(), len(data))
	}
}

func TestStatDir(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "dir/bar.txt", []byte("x"))
	dir := mountBucket(t, bucket)

	fi, err := os.Stat(filepath.Join(dir, "dir"))
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Fatal("expected directory")
	}
}

func TestReaddirRoot(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("a"))
	put(t, bucket, "dir/bar.txt", []byte("b"))
	dir := mountBucket(t, bucket)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	want := []string{"dir", "foo.txt"}
	if len(names) != len(want) {
		t.Fatalf("got %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("entry[%d]: got %q, want %q", i, names[i], want[i])
		}
	}
}

func TestReaddirSubdir(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "dir/a.txt", []byte("a"))
	put(t, bucket, "dir/b.txt", []byte("b"))
	dir := mountBucket(t, bucket)

	entries, err := os.ReadDir(filepath.Join(dir, "dir"))
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name()
	}
	want := []string{"a.txt", "b.txt"}
	if len(names) != len(want) {
		t.Fatalf("got %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("entry[%d]: got %q, want %q", i, names[i], want[i])
		}
	}
}

func TestLookupNotFound(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	_, err := os.Open(filepath.Join(dir, "nonexistent.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected not-exist error, got %v", err)
	}
}

func TestDeleteFile(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	dir := mountBucket(t, bucket)

	if err := os.Remove(filepath.Join(dir, "foo.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := bucket.HeadObject(t.Context(), "foo.txt"); err == nil {
		t.Fatal("expected object to be deleted")
	}
}

func TestCreateThenStat(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	f, err := os.Create(filepath.Join(dir, "new.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("data"); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := bucket.HeadObject(t.Context(), "new.txt"); err != nil {
		t.Fatalf("expected object to exist: %v", err)
	}
}

func TestTruncateToZero(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	dir := mountBucket(t, bucket)

	if err := os.Truncate(filepath.Join(dir, "foo.txt"), 0); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), "foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 0 {
		t.Fatalf("expected size 0, got %d", info.Size)
	}
}

func TestNestedPath(t *testing.T) {
	bucket := newBucket(t)
	want := []byte("deep content")
	put(t, bucket, "a/b/c/file.txt", want)
	dir := mountBucket(t, bucket)

	got, err := os.ReadFile(filepath.Join(dir, "a", "b", "c", "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestEmptyBucket(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected empty dir, got %v", entries)
	}
}

func TestConcurrentReads(t *testing.T) {
	bucket := newBucket(t)
	want := []byte("shared content")
	put(t, bucket, "shared.txt", want)
	dir := mountBucket(t, bucket)

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)
	results := make([][]byte, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			data, err := os.ReadFile(filepath.Join(dir, "shared.txt"))
			errs[i] = err
			results[i] = data
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
		if !bytes.Equal(results[i], want) {
			t.Errorf("goroutine %d: got %q, want %q", i, results[i], want)
		}
	}
}

// --- P0: copy-on-open and non-zero truncation ---

// TestPartialOverwrite exercises the copy-on-open path in writeHandle: opening
// an existing file with O_RDWR (no O_TRUNC) downloads the current content into
// the temp file first, so a partial write preserves the surrounding bytes.
func TestPartialOverwrite(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello world"))
	dir := mountBucket(t, bucket)

	f, err := os.OpenFile(filepath.Join(dir, "foo.txt"), os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("HELLO"); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte("HELLO world"); !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestTruncateOpenFile exercises the non-zero Setattr truncation path in
// fileNode, which resizes the open writeHandle's temp file via wh.truncate.
func TestTruncateOpenFile(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	f, err := os.Create(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("x"), 100)
	if _, err := f.Write(data); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Truncate(50); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), "foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 50 {
		t.Fatalf("expected size 50, got %d", info.Size)
	}
}

// --- P1: error propagation ---

// TestReadErrorPropagation verifies that a GetObject error surfaces through
// the FUSE layer as os.ErrNotExist when the object is not found.
func TestReadErrorPropagation(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	eb := &errorBucket{Bucket: bucket, getErr: storage.ErrObjectNotFound}
	dir := mountBucket(t, eb)

	_, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected not-exist error, got %v", err)
	}
}

// TestWriteToReadOnlyBucket verifies that a PutObject error (ErrBucketReadOnly)
// surfaces as an error when closing a written file.
func TestWriteToReadOnlyBucket(t *testing.T) {
	bucket := newBucket(t)
	eb := &errorBucket{Bucket: bucket, putErr: storage.ErrBucketReadOnly}
	dir := mountBucket(t, eb)

	err := os.WriteFile(filepath.Join(dir, "out.txt"), []byte("data"), 0644)
	if err == nil {
		t.Fatal("expected error writing to read-only bucket, got nil")
	}
}

// TestReaddirError verifies that a ListObjects error propagates through Readdir.
func TestReaddirError(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("x"))
	eb := &errorBucket{Bucket: bucket, listErr: storage.ErrBucketNotFound}
	dir := mountBucket(t, eb)

	_, err := os.ReadDir(dir)
	if err == nil {
		t.Fatal("expected error from ReadDir, got nil")
	}
}

// TestLookupHeadObjectError verifies that a non-ENOENT HeadObject error
// propagates as a non-ErrNotExist error (not silently treated as missing).
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

// --- P2: behavioral edge cases ---

// TestWriteWithoutChanges verifies the dirty=false branch in Flush: opening a
// file for writing without modifying it does not overwrite the bucket object.
func TestWriteWithoutChanges(t *testing.T) {
	bucket := newBucket(t)
	want := []byte("original content")
	put(t, bucket, "foo.txt", want)
	dir := mountBucket(t, bucket)

	// Open for write but do nothing.
	f, err := os.OpenFile(filepath.Join(dir, "foo.txt"), os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestConcurrentWrites verifies that concurrent file creations are isolated
// (each writeHandle uses its own temp file) and all writes reach the bucket.
func TestConcurrentWrites(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	const n = 10
	var wg sync.WaitGroup
	errs := make([]error, n)

	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := filepath.Join(dir, "file"+string(rune('0'+i))+".txt")
			errs[i] = os.WriteFile(name, []byte{byte(i)}, 0644)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
	// Verify all files landed in the bucket.
	for i := range n {
		key := "file" + string(rune('0'+i)) + ".txt"
		if _, err := bucket.HeadObject(t.Context(), key); err != nil {
			t.Errorf("key %q missing from bucket: %v", key, err)
		}
	}
}

// TestLargeFile verifies that reading and writing a file larger than a single
// FUSE I/O page works correctly, exercising range-read stitching in readHandle.
func TestLargeFile(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	want := bytes.Repeat([]byte("abcdefgh"), 128*1024/8) // 128 KB
	if err := os.WriteFile(filepath.Join(dir, "large.bin"), want, 0644); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "large.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("large file round-trip mismatch: got %d bytes, want %d bytes", len(got), len(want))
	}
}

// TestTruncateShrinkNoHandle verifies that truncate(2) without an open fd
// shrinks the object: only the first N bytes are kept.
func TestTruncateShrinkNoHandle(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello world"))
	dir := mountBucket(t, bucket)

	if err := os.Truncate(filepath.Join(dir, "foo.txt"), 5); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte("hello"); !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestTruncateExtendNoHandle verifies that truncate(2) without an open fd
// extends the object with NUL bytes when size > current file size.
func TestTruncateExtendNoHandle(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	dir := mountBucket(t, bucket)

	if err := os.Truncate(filepath.Join(dir, "foo.txt"), 10); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	want := append([]byte("hello"), make([]byte, 5)...)
	if !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestUnlinkNonExistent verifies that removing a path that does not exist
// returns os.ErrNotExist, consistent with POSIX unlink semantics. Our Unlink
// handler is idempotent, but the kernel's Lookup returns ENOENT before Unlink
// is ever reached for truly missing paths.
func TestUnlinkNonExistent(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	err := os.Remove(filepath.Join(dir, "ghost.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
}

// TestTruncateWithoutWrite verifies that truncating an open file without any
// Write calls still persists the truncation to the bucket. This exercises the
// dirty flag set in writeHandle.truncate.
func TestTruncateWithoutWrite(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello world"))
	dir := mountBucket(t, bucket)

	f, err := os.OpenFile(filepath.Join(dir, "foo.txt"), os.O_RDWR, 0644)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(5); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte("hello"); !bytes.Equal(got, want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestTruncateToZeroOpenFile verifies that truncating an open (dirty) file to
// zero persists the empty content, not the pre-truncation bytes.
func TestTruncateToZeroOpenFile(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	f, err := os.Create(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte("x"), 100)); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := bucket.HeadObject(t.Context(), "foo.txt")
	if err != nil {
		t.Fatal(err)
	}
	if info.Size != 0 {
		t.Fatalf("expected size 0, got %d", info.Size)
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

	_, err := os.OpenFile(filepath.Join(dir, "foo.txt"), os.O_RDWR, 0644)
	if err == nil {
		t.Fatal("expected error opening file with failing GetObject, got nil")
	}
}

// TestUnlinkDeleteError verifies that a DeleteObject error (including ENOENT)
// propagates through the Unlink handler rather than being masked.
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
