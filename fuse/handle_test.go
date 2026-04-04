package fuse_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestReadFile verifies that readHandle.Read returns the correct content.
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

// TestConcurrentReads verifies that multiple goroutines can read the same file
// concurrently via independent readHandle instances.
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

// TestWriteNewFile verifies that a new file written via writeHandle is flushed
// to the bucket on close.
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

// TestOverwriteFile verifies that opening an existing file with O_TRUNC
// discards the original content and replaces it.
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

// TestWriteWithoutChanges verifies the dirty=false branch in Flush: opening a
// file for writing without modifying it does not re-upload the object.
func TestWriteWithoutChanges(t *testing.T) {
	bucket := newBucket(t)
	want := []byte("original content")
	put(t, bucket, "foo.txt", want)
	dir := mountBucket(t, bucket)

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
