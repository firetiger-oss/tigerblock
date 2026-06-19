package storage_test

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"iter"
	"slices"
	"testing"
	"testing/fstest"

	"github.com/firetiger-oss/tigerblock/internal/sequtil"
	"github.com/firetiger-oss/tigerblock/storage"
)

func newTestFSBucket() storage.Bucket {
	return storage.NewBucketFromFS(fstest.MapFS{
		"hello.txt":     {Data: []byte("hello world")},
		"dir/a.txt":     {Data: []byte("aaa")},
		"dir/b.txt":     {Data: []byte("bbb")},
		"dir/sub/c.txt": {Data: []byte("ccc")},
		"other/d.txt":   {Data: []byte("dddd")},
	})
}

func collectKeys(seq iter.Seq2[storage.Object, error]) ([]string, error) {
	objects, err := sequtil.Collect(seq)
	keys := make([]string, len(objects))
	for i, o := range objects {
		keys[i] = o.Key
	}
	return keys, err
}

func TestNewBucketFromFS(t *testing.T) {
	ctx := t.Context()
	bucket := newTestFSBucket()

	t.Run("Location", func(t *testing.T) {
		if got := bucket.Location(); got != ":fs:" {
			t.Errorf("expected location %q, got %q", ":fs:", got)
		}
	})

	t.Run("Access", func(t *testing.T) {
		if err := bucket.Access(ctx); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("HeadObject", func(t *testing.T) {
		info, err := bucket.HeadObject(ctx, "hello.txt")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info.Size != int64(len("hello world")) {
			t.Errorf("expected size %d, got %d", len("hello world"), info.Size)
		}
	})

	t.Run("HeadObjectNotFound", func(t *testing.T) {
		_, err := bucket.HeadObject(ctx, "missing.txt")
		if !errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("expected ErrObjectNotFound, got %v", err)
		}
	})

	t.Run("HeadObjectOnDirIsNotFound", func(t *testing.T) {
		_, err := bucket.HeadObject(ctx, "dir")
		if !errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("expected ErrObjectNotFound, got %v", err)
		}
	})

	t.Run("GetObject", func(t *testing.T) {
		r, info, err := bucket.GetObject(ctx, "hello.txt")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(data) != "hello world" {
			t.Errorf("expected %q, got %q", "hello world", data)
		}
		if info.Size != int64(len("hello world")) {
			t.Errorf("expected size %d, got %d", len("hello world"), info.Size)
		}
	})

	t.Run("GetObjectRange", func(t *testing.T) {
		r, info, err := bucket.GetObject(ctx, "hello.txt", storage.BytesRange(6, 10))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(data) != "world" {
			t.Errorf("expected %q, got %q", "world", data)
		}
		// Size reflects the full object, not the range.
		if info.Size != int64(len("hello world")) {
			t.Errorf("expected size %d, got %d", len("hello world"), info.Size)
		}
	})

	t.Run("GetObjectOpenEndedRange", func(t *testing.T) {
		r, _, err := bucket.GetObject(ctx, "hello.txt", storage.BytesRange(6, -1))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		data, _ := io.ReadAll(r)
		if string(data) != "world" {
			t.Errorf("expected %q, got %q", "world", data)
		}
	})

	t.Run("GetObjectRangePastEnd", func(t *testing.T) {
		r, _, err := bucket.GetObject(ctx, "hello.txt", storage.BytesRange(100, -1))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer r.Close()
		data, _ := io.ReadAll(r)
		if len(data) != 0 {
			t.Errorf("expected empty read, got %q", data)
		}
	})

	t.Run("GetObjectNotFound", func(t *testing.T) {
		_, _, err := bucket.GetObject(ctx, "missing.txt")
		if !errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("expected ErrObjectNotFound, got %v", err)
		}
	})

	t.Run("ListObjects", func(t *testing.T) {
		got, err := collectKeys(bucket.ListObjects(ctx))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"dir/a.txt", "dir/b.txt", "dir/sub/c.txt", "hello.txt", "other/d.txt"}
		if !slices.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("ListObjectsPrefix", func(t *testing.T) {
		got, err := collectKeys(bucket.ListObjects(ctx, storage.KeyPrefix("dir/")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"dir/a.txt", "dir/b.txt", "dir/sub/c.txt"}
		if !slices.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("ListObjectsDelimiter", func(t *testing.T) {
		got, err := collectKeys(bucket.ListObjects(ctx, storage.KeyPrefix("dir/"), storage.KeyDelimiter("/")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"dir/a.txt", "dir/b.txt", "dir/sub/"}
		if !slices.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("ListObjectsMaxKeys", func(t *testing.T) {
		got, err := collectKeys(bucket.ListObjects(ctx, storage.MaxKeys(2)))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"dir/a.txt", "dir/b.txt"}
		if !slices.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("ListObjectsStartAfter", func(t *testing.T) {
		got, err := collectKeys(bucket.ListObjects(ctx, storage.StartAfter("dir/b.txt")))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := []string{"dir/sub/c.txt", "hello.txt", "other/d.txt"}
		if !slices.Equal(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("WriteOperationsReadOnly", func(t *testing.T) {
		if _, err := bucket.PutObject(ctx, "x.txt", nil); !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("PutObject: expected ErrBucketReadOnly, got %v", err)
		}
		if err := bucket.DeleteObject(ctx, "hello.txt"); !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("DeleteObject: expected ErrBucketReadOnly, got %v", err)
		}
		if err := bucket.CopyObject(ctx, "hello.txt", "copy.txt"); !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("CopyObject: expected ErrBucketReadOnly, got %v", err)
		}
		if err := bucket.Create(ctx); !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("Create: expected ErrBucketReadOnly, got %v", err)
		}
	})
}

// TestFSBucketListOrdering pins the key-sorted ordering across directory
// boundaries. A file ("data.txt") must sort before the contents of a
// sibling directory of a lexically-smaller name ("data/"), because '.' < '/'.
// fs.WalkDir would visit them in the opposite order.
func TestFSBucketListOrdering(t *testing.T) {
	ctx := t.Context()
	bucket := storage.NewBucketFromFS(fstest.MapFS{
		"data/x.txt":   {Data: []byte("x")},
		"data.txt":     {Data: []byte("d")},
		"data-1.txt":   {Data: []byte("1")},
		"data/a/y.txt": {Data: []byte("y")},
	})

	got, err := collectKeys(bucket.ListObjects(ctx))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"data-1.txt", "data.txt", "data/a/y.txt", "data/x.txt"}
	if !slices.Equal(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}

	// With a "/" delimiter the directory collapses to a single common prefix,
	// still correctly ordered relative to the sibling files.
	got, err = collectKeys(bucket.ListObjects(ctx, storage.KeyDelimiter("/")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want = []string{"data-1.txt", "data.txt", "data/"}
	if !slices.Equal(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestNewBucketFromNewFSUsesContext(t *testing.T) {
	type ctxKey struct{}

	bucket := storage.NewBucketFromNewFS(func(ctx context.Context) fs.FS {
		name, _ := ctx.Value(ctxKey{}).(string)
		return fstest.MapFS{"name.txt": {Data: []byte(name)}}
	})

	ctx := context.WithValue(t.Context(), ctxKey{}, "captured")
	r, _, err := bucket.GetObject(ctx, "name.txt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer r.Close()
	data, _ := io.ReadAll(r)
	if string(data) != "captured" {
		t.Errorf("expected %q, got %q", "captured", data)
	}
}
