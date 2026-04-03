package fuse_test

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"

	gofs "github.com/hanwen/go-fuse/v2/fs"

	storage "github.com/firetiger-oss/storage"
	storagefuse "github.com/firetiger-oss/storage/fuse"
	"github.com/firetiger-oss/storage/memory"
)

func mountBucket(t *testing.T, bucket storage.Bucket) string {
	t.Helper()
	dir := t.TempDir()
	server, err := storagefuse.Mount(dir, bucket, &gofs.Options{})
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
