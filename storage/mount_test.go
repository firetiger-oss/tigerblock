package storage_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/firetiger-oss/tigerblock/internal/sequtil"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	"github.com/firetiger-oss/tigerblock/test"
)

func TestMountedBucket(t *testing.T) {
	test.TestStorage(t, func(t *testing.T) (storage.Bucket, error) {
		base, mount := new(memory.Bucket), new(memory.Bucket)
		return storage.Mount(base, "test/", mount), nil
	})
}

func TestMountHeadObject(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	if _, err := mount.PutObject(ctx, "readme.txt", strings.NewReader("mounted content")); err != nil {
		t.Fatal("unexpected error:", err)
	}

	info, err := bucket.HeadObject(ctx, "docs/readme.txt")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	if info.Size != 15 {
		t.Errorf("expected size 15, got %d", info.Size)
	}
}

func TestMountGetObject(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	if _, err := mount.PutObject(ctx, "readme.txt", strings.NewReader("mounted content")); err != nil {
		t.Fatal("unexpected error:", err)
	}

	r, info, err := bucket.GetObject(ctx, "docs/readme.txt")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	defer r.Close()

	content := make([]byte, info.Size)
	if _, err := r.Read(content); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if string(content) != "mounted content" {
		t.Errorf("expected 'mounted content', got '%s'", string(content))
	}
}

func TestMountPutObject(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	info, err := bucket.PutObject(ctx, "docs/readme.txt", strings.NewReader("new content"))
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	if info.Size != 11 {
		t.Errorf("expected size 11, got %d", info.Size)
	}

	r, _, err := mount.GetObject(ctx, "readme.txt")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	defer r.Close()

	content := make([]byte, 11)
	if _, err := r.Read(content); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if string(content) != "new content" {
		t.Errorf("expected 'new content', got '%s'", string(content))
	}
}

func TestMountDeleteObject(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	if _, err := base.PutObject(ctx, "docs/readme.txt", strings.NewReader("base content")); err != nil {
		t.Fatal("unexpected error:", err)
	}
	if _, err := mount.PutObject(ctx, "readme.txt", strings.NewReader("mount content")); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if err := bucket.DeleteObject(ctx, "docs/readme.txt"); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if _, _, err := base.GetObject(ctx, "docs/readme.txt"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected object not found in base bucket")
	}

	if _, _, err := mount.GetObject(ctx, "readme.txt"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected object not found in mount bucket")
	}
}

func TestMountDeleteObjects(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	if _, err := base.PutObject(ctx, "docs/readme.txt", strings.NewReader("base content")); err != nil {
		t.Fatal("unexpected error:", err)
	}
	if _, err := base.PutObject(ctx, "config.yaml", strings.NewReader("config")); err != nil {
		t.Fatal("unexpected error:", err)
	}
	if _, err := mount.PutObject(ctx, "readme.txt", strings.NewReader("mount content")); err != nil {
		t.Fatal("unexpected error:", err)
	}

	// Delete objects using iterator API
	results := bucket.DeleteObjects(ctx, sequtil.Values([]string{"docs/readme.txt", "config.yaml"}))
	for key, err := range results {
		if err != nil {
			t.Fatalf("unexpected error deleting object %s: %v", key, err)
		}
	}

	if _, _, err := base.GetObject(ctx, "docs/readme.txt"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected object not found in base bucket")
	}
	if _, _, err := base.GetObject(ctx, "config.yaml"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected config.yaml not found in base bucket")
	}
	if _, _, err := mount.GetObject(ctx, "readme.txt"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected object not found in mount bucket")
	}
}

func TestMountNonMatchingPrefix(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	if _, err := base.PutObject(ctx, "config.yaml", strings.NewReader("config")); err != nil {
		t.Fatal("unexpected error:", err)
	}

	r, info, err := bucket.GetObject(ctx, "config.yaml")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	defer r.Close()

	content := make([]byte, info.Size)
	if _, err := r.Read(content); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if string(content) != "config" {
		t.Errorf("expected 'config', got '%s'", string(content))
	}
}

func TestMountExactPrefixMatch(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	if _, err := mount.PutObject(ctx, "index", strings.NewReader("root content")); err != nil {
		t.Fatal("unexpected error:", err)
	}

	r, info, err := bucket.GetObject(ctx, "docs/index")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	defer r.Close()

	content := make([]byte, info.Size)
	if _, err := r.Read(content); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if string(content) != "root content" {
		t.Errorf("expected 'root content', got '%s'", string(content))
	}
}

func TestMountLocationAndAccess(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	if bucket.Location() != base.Location() {
		t.Errorf("expected location %s, got %s", base.Location(), bucket.Location())
	}

	if err := bucket.Access(ctx); err != nil {
		t.Fatal("unexpected error:", err)
	}
}

func TestMountErrorScoping(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := storage.ReadOnlyBucket(new(memory.Bucket))
	bucket := storage.WithMount("docs/", mount).AdaptBucket(base)

	_, err := bucket.PutObject(ctx, "docs/readme.txt", strings.NewReader("content"))
	if err == nil {
		t.Fatal("expected error")
	}

	errorMessage := err.Error()
	if !strings.Contains(errorMessage, "docs/:") {
		t.Errorf("expected error to be scoped with 'docs:', got %s", errorMessage)
	}
}

func TestMountPrefixBoundary(t *testing.T) {
	ctx := t.Context()
	base := new(memory.Bucket)
	mount := new(memory.Bucket)
	bucket := storage.WithMount("doc/", mount).AdaptBucket(base)

	if _, err := base.PutObject(ctx, "document.txt", strings.NewReader("base content")); err != nil {
		t.Fatal("unexpected error:", err)
	}

	r, info, err := bucket.GetObject(ctx, "document.txt")
	if err != nil {
		t.Fatal("unexpected error:", err)
	}
	defer r.Close()

	content := make([]byte, info.Size)
	if _, err := r.Read(content); err != nil {
		t.Fatal("unexpected error:", err)
	}

	if string(content) != "base content" {
		t.Errorf("expected 'base content', got '%s'", string(content))
	}
}
