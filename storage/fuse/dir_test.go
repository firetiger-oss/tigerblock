package fuse_test

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestLookupNotFound(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	_, err := os.Open(filepath.Join(dir, "nonexistent.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected not-exist error, got %v", err)
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

func TestNestedPath(t *testing.T) {
	bucket := newBucket(t)
	want := []byte("deep content")
	put(t, bucket, "a/b/c/file.txt", want)
	dir := mountBucket(t, bucket)

	got, err := os.ReadFile(filepath.Join(dir, "a", "b", "c", "file.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("got %q, want %q", got, want)
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

// TestUnlinkNonExistent verifies that removing a path that does not exist
// returns os.ErrNotExist. The kernel's Lookup returns ENOENT before our Unlink
// handler is reached, so this exercises the Lookup → ENOENT path.
func TestUnlinkNonExistent(t *testing.T) {
	bucket := newBucket(t)
	dir := mountBucket(t, bucket)

	err := os.Remove(filepath.Join(dir, "ghost.txt"))
	if !os.IsNotExist(err) {
		t.Fatalf("expected ErrNotExist, got %v", err)
	}
}

// TestConcurrentWrites verifies that concurrent file creations are isolated —
// each writeHandle uses its own temp file — and all writes reach the bucket.
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
	for i := range n {
		key := "file" + string(rune('0'+i)) + ".txt"
		if _, err := bucket.HeadObject(t.Context(), key); err != nil {
			t.Errorf("key %q missing from bucket: %v", key, err)
		}
	}
}
