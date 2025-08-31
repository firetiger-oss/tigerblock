package storage_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/memory"
	storagetest "github.com/firetiger-oss/storage/test"
)

func TestLocation(t *testing.T) {
	tests := []struct {
		location string
		path     string
		result   string
	}{
		{
			location: "",
			path:     "path/to/table",
			result:   "path/to/table",
		},

		{
			location: "s3://bucket/path/to/table",
			path:     "",
			result:   "s3://bucket/path/to/table",
		},

		{
			location: "s3://bucket/path/to/table",
			path:     "manifests",
			result:   "s3://bucket/path/to/table/manifests",
		},

		{
			location: "file://",
			path:     "path/to/table",
			result:   "file:///path/to/table",
		},

		{
			location: "memory://",
			path:     "",
			result:   "memory://",
		},

		{
			location: "path",
			path:     "to/table",
			result:   "path/to/table",
		},

		{
			location: "path",
			path:     "/to/table",
			result:   "path/to/table",
		},
	}

	for _, test := range tests {
		t.Run(test.location, func(t *testing.T) {
			result := storage.Location(test.location, test.path)
			if result != test.result {
				t.Fatalf("unexpected result: %q != %q", result, test.result)
			}
		})
	}
}

type fakeBucket struct {
	storage.Bucket
	uri string
}

func (b *fakeBucket) Location() string {
	return b.uri
}

func TestLoadBucket(t *testing.T) {
	loadBucket := func(ctx context.Context, bucketURI string) (storage.Bucket, error) {
		return &fakeBucket{uri: bucketURI}, nil
	}
	storage.Register("TestLoadBucket-1", storage.RegistryFunc(loadBucket))
	storage.Register("TestLoadBucket-2", storage.RegistryFunc(loadBucket))
	storage.Register("TestLoadBucket-3", storage.RegistryFunc(loadBucket))

	ctx := t.Context()

	bucket1, err := storage.LoadBucket(ctx, "TestLoadBucket-1://bucket-A")
	if err != nil {
		t.Fatal("unexpected error loading bucket:", err)
	}
	bucket2, err := storage.LoadBucket(ctx, "TestLoadBucket-2://bucket-B")
	if err != nil {
		t.Fatal("unexpected error loading bucket:", err)
	}
	bucket3, err := storage.LoadBucket(ctx, "TestLoadBucket-3://bucket-C")
	if err != nil {
		t.Fatal("unexpected error loading bucket:", err)
	}

	if bucket1.Location() != "TestLoadBucket-1://bucket-A" {
		t.Fatal("unexpected bucket location:", bucket1.Location())
	}
	if bucket2.Location() != "TestLoadBucket-2://bucket-B" {
		t.Fatal("unexpected bucket location:", bucket2.Location())
	}
	if bucket3.Location() != "TestLoadBucket-3://bucket-C" {
		t.Fatal("unexpected bucket location:", bucket3.Location())
	}

	if _, err := storage.LoadBucket(ctx, "TestLoadBucket-4://bucket-D"); !errors.Is(err, storage.ErrBucketNotFound) {
		t.Fatal("expected bucket not found error:", err)
	}
}

func TestGetObject(t *testing.T) {
	bucket := memory.NewBucket(
		&memory.Entry{Key: "key-1", Value: []byte("value-1")},
		&memory.Entry{Key: "key-2", Value: []byte("value-2")},
		&memory.Entry{Key: "key-3", Value: []byte("value-3")},
	)
	storage.Register("TestGetObject", storage.SingleBucketRegistry(bucket))

	ctx := t.Context()
	for _, test := range []struct {
		key   string
		value string
	}{
		{"key-1", "value-1"},
		{"key-2", "value-2"},
		{"key-3", "value-3"},
	} {
		(func() {
			r, object, err := storage.GetObject(ctx, "TestGetObject://:memory:/"+test.key)
			if err != nil {
				t.Fatal("unexpected error reading object:", err)
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				t.Fatal("unexpected error reading object:", err)
			}
			if string(b) != test.value {
				t.Fatalf("unexpected object value: %q != %q", b, test.value)
			}
			if int(object.Size) != len(test.value) {
				t.Fatalf("unexpected object size: %d != %d", object.Size, len(test.value))
			}
		})()
	}

	if _, _, err := storage.GetObject(ctx, "TestGetObject://:memory:/key-4"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected object not found error:", err)
	}
}

func TestPutObject(t *testing.T) {
	bucket := memory.NewBucket()
	storage.Register("TestPutObject", storage.SingleBucketRegistry(bucket))

	ctx := t.Context()
	if object, err := storage.PutObject(ctx, "TestPutObject://:memory:/key-1", strings.NewReader("value-1")); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 7 {
		t.Fatalf("unexpected object size: %d != %d", object.Size, 7)
	}

	r, _, err := storage.GetObject(ctx, "TestPutObject://:memory:/key-1")
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	if string(b) != "value-1" {
		t.Fatalf("unexpected object value: %q != %q", b, "value-1")
	}
}

func TestPutObjectWriter(t *testing.T) {
	bucket := memory.NewBucket()
	storage.Register("TestPutObject", storage.SingleBucketRegistry(bucket))

	t.Run("write", func(t *testing.T) {
		w := storage.PutObjectWriter(t.Context(), "TestPutObject://:memory:/key-1")
		if _, err := w.Write([]byte("value-1")); err != nil {
			t.Fatal(err)
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}

		r, _, err := storage.GetObject(t.Context(), "TestPutObject://:memory:/key-1")
		if err != nil {
			t.Fatal("unexpected error reading object:", err)
		}
		defer r.Close()
		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatal("unexpected error reading object:", err)
		}
		if string(b) != "value-1" {
			t.Fatalf("unexpected object value: %q != %q", b, "value-1")
		}
	})

	t.Run("invalid path", func(t *testing.T) {
		w := storage.PutObjectWriter(t.Context(), "invalid://path")
		if _, err := w.Write([]byte("payload")); err == nil {
			t.Error("expected error")
		}
		if err := w.Close(); err == nil {
			t.Error("expected error")
		}
	})

	t.Run("canceled", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(t.Context())
		cancel()

		w := storage.PutObjectWriter(canceledCtx, "TestPutObject://:memory:/key-1")
		if _, err := w.Write([]byte("value-1")); !errors.Is(err, context.Canceled) {
			t.Errorf("expect %v got %v", context.Canceled, err)
		}
		if err := w.Close(); !errors.Is(err, context.Canceled) {
			t.Errorf("expect %v got %v", context.Canceled, err)
		}
	})
}

func TestDeleteObject(t *testing.T) {
	bucket := memory.NewBucket(
		&memory.Entry{Key: "key-1", Value: []byte("value-1")},
		&memory.Entry{Key: "key-2", Value: []byte("value-2")},
		&memory.Entry{Key: "key-3", Value: []byte("value-3")},
	)
	storage.Register("TestDeleteObject", storage.SingleBucketRegistry(bucket))

	ctx := t.Context()
	if err := storage.DeleteObject(ctx, "TestDeleteObject://:memory:/key-1"); err != nil {
		t.Fatal("unexpected error deleting object:", err)
	}
	if _, _, err := storage.GetObject(ctx, "TestDeleteObject://:memory:/key-1"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected object not found error:", err)
	}
}

func TestDeleteObjects(t *testing.T) {
	bucket1 := memory.NewBucket(
		&memory.Entry{Key: "key-1", Value: []byte("value-1")},
		&memory.Entry{Key: "key-2", Value: []byte("value-2")},
		&memory.Entry{Key: "key-3", Value: []byte("value-3")},
	)

	bucket2 := memory.NewBucket(
		&memory.Entry{Key: "key-1", Value: []byte("value-1")},
		&memory.Entry{Key: "key-2", Value: []byte("value-2")},
		&memory.Entry{Key: "key-3", Value: []byte("value-3")},
	)

	storage.Register("TestDeleteObjects1", storage.SingleBucketRegistry(bucket1))
	storage.Register("TestDeleteObjects2", storage.SingleBucketRegistry(bucket2))

	ctx := t.Context()

	if err := storage.DeleteObjects(ctx, []string{
		"TestDeleteObjects1://:memory:/key-1",
		"TestDeleteObjects1://:memory:/key-3",
		"TestDeleteObjects2://:memory:/key-2",
	}); err != nil {
		t.Fatal("unexpected error deleting objects:", err)
	}

	for _, key := range []string{
		"TestDeleteObjects1://:memory:/key-1",
		"TestDeleteObjects2://:memory:/key-2",
	} {
		if _, _, err := storage.GetObject(ctx, key); !errors.Is(err, storage.ErrObjectNotFound) {
			t.Fatal("expected object not found error:", err)
		}
	}
}

func TestListObjects(t *testing.T) {
	bucket := memory.NewBucket(
		&memory.Entry{Key: "path/to/file1.txt", Value: []byte("content1")},
		&memory.Entry{Key: "path/to/file2.txt", Value: []byte("content2")},
		&memory.Entry{Key: "path/other/file3.txt", Value: []byte("content3")},
		&memory.Entry{Key: "different/file4.txt", Value: []byte("content4")},
	)
	storage.Register("TestListObjects", storage.SingleBucketRegistry(bucket))

	t.Run("list all objects", func(t *testing.T) {
		var objects []storage.Object
		for obj, err := range storage.ListObjects(t.Context(), "TestListObjects://:memory:") {
			if err != nil {
				t.Fatal("unexpected error listing objects:", err)
			}
			objects = append(objects, obj)
		}

		if len(objects) != 4 {
			t.Fatalf("expected 4 objects, got %d", len(objects))
		}

		// Verify all keys are absolute URIs
		expectedKeys := []string{
			"TestListObjects://:memory:path/to/file1.txt",
			"TestListObjects://:memory:path/to/file2.txt",
			"TestListObjects://:memory:path/other/file3.txt",
			"TestListObjects://:memory:different/file4.txt",
		}

		actualKeys := make([]string, len(objects))
		for i, obj := range objects {
			actualKeys[i] = obj.Key
		}

		for _, expectedKey := range expectedKeys {
			found := slices.Contains(actualKeys, expectedKey)
			if !found {
				t.Errorf("expected key %s not found in results", expectedKey)
			}
		}
	})

	t.Run("list with prefix", func(t *testing.T) {
		var objects []storage.Object
		for obj, err := range storage.ListObjects(t.Context(), "TestListObjects://:memory:/path/to") {
			if err != nil {
				t.Fatal("unexpected error listing objects:", err)
			}
			objects = append(objects, obj)
		}

		if len(objects) != 2 {
			t.Fatalf("expected 2 objects, got %d", len(objects))
		}

		// Verify objects have absolute URIs and correct prefix
		for _, obj := range objects {
			if !strings.HasPrefix(obj.Key, "TestListObjects://:memory:path/to/") {
				t.Errorf("object key %s does not have expected prefix", obj.Key)
			}
		}
	})
}

func TestValidObjectKey(t *testing.T) {
	for _, path := range storagetest.InvalidPaths {
		if err := storage.ValidObjectKey(path); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected invalid object key error: %v", err)
		}
	}
}
