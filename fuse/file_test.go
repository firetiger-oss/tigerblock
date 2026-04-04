package fuse_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

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

// TestTruncateToZero verifies Setattr size==0 with no open handle: the object
// is replaced by an empty PutObject directly on the bucket.
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

// TestTruncateOpenFile exercises the Setattr size>0 path with an open
// writeHandle: wh.truncate is called on the temp file.
func TestTruncateOpenFile(t *testing.T) {
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

// TestTruncateWithoutWrite verifies that truncating an open file without any
// Write calls still persists the truncation. This exercises the dirty flag set
// in writeHandle.truncate.
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
	if want := []byte("hello"); string(got) != string(want) {
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

// TestTruncateShrinkNoHandle verifies truncateRemote shrink: only the first N
// bytes are kept when truncate(2) is called without an open fd.
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
	if want := []byte("hello"); string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestTruncateExtendNoHandle verifies truncateRemote extend: the object is
// zero-filled when truncate(2) requests a size larger than the current content.
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
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestTruncateShrinkExactSize verifies that truncating to the current size is a
// no-op: content is unchanged.
func TestTruncateShrinkExactSize(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	dir := mountBucket(t, bucket)

	if err := os.Truncate(filepath.Join(dir, "foo.txt"), 5); err != nil {
		t.Fatal(err)
	}

	got, err := os.ReadFile(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte("hello"); string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TestConcurrentGetattr exercises concurrent Stat calls on the same file.
// Under -race this catches data races on any shared fileNode state.
func TestConcurrentGetattr(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello"))
	dir := mountBucket(t, bucket)

	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	sizes := make([]int64, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			fi, err := os.Stat(filepath.Join(dir, "foo.txt"))
			errs[i] = err
			if fi != nil {
				sizes[i] = fi.Size()
			}
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d stat: %v", i, err)
		}
		if sizes[i] != 5 {
			t.Errorf("goroutine %d size: got %d, want 5", i, sizes[i])
		}
	}
}

// TestConcurrentGetattrAndTruncate runs concurrent Stat and Truncate calls on
// the same file. Under -race this verifies there are no data races between
// concurrent FUSE requests on the same fileNode.
func TestConcurrentGetattrAndTruncate(t *testing.T) {
	bucket := newBucket(t)
	put(t, bucket, "foo.txt", []byte("hello world"))
	dir := mountBucket(t, bucket)

	var wg sync.WaitGroup

	wg.Add(1)
	var truncErr error
	go func() {
		defer wg.Done()
		truncErr = os.Truncate(filepath.Join(dir, "foo.txt"), 5)
	}()

	const n = 10
	statErrs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, statErrs[i] = os.Stat(filepath.Join(dir, "foo.txt"))
		}(i)
	}
	wg.Wait()

	if truncErr != nil {
		t.Fatalf("truncate: %v", truncErr)
	}
	for i, err := range statErrs {
		if err != nil {
			t.Errorf("stat goroutine %d: %v", i, err)
		}
	}

	fi, err := os.Stat(filepath.Join(dir, "foo.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 5 {
		t.Fatalf("expected size 5 after truncate, got %d", fi.Size())
	}
}
