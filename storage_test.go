package storage_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/internal/sequtil"
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

	// Delete objects using iterator API
	results := storage.DeleteObjects(ctx, sequtil.Values([]string{
		"TestDeleteObjects1://:memory:/key-1",
		"TestDeleteObjects1://:memory:/key-3",
		"TestDeleteObjects2://:memory:/key-2",
	}))

	// Consume results and check for errors
	for key, err := range results {
		if err != nil {
			t.Fatalf("unexpected error deleting object %s: %v", key, err)
		}
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

func TestDeleteObjectsErrorHandling(t *testing.T) {
	bucket := memory.NewBucket(
		&memory.Entry{Key: "key-1", Value: []byte("value-1")},
	)
	storage.Register("TestDeleteObjectsErrorHandling", storage.SingleBucketRegistry(bucket))

	ctx := t.Context()

	t.Run("invalid URI format", func(t *testing.T) {
		results := storage.DeleteObjects(ctx, sequtil.Values([]string{
			"invalid-uri-no-scheme",
		}))

		for key, err := range results {
			if err == nil {
				t.Fatalf("expected error for invalid URI %s, got nil", key)
			}
		}
	})

	t.Run("non-existent bucket", func(t *testing.T) {
		results := storage.DeleteObjects(ctx, sequtil.Values([]string{
			"NonExistentBucket://:memory:/key-1",
		}))

		var gotError bool
		for _, err := range results {
			if err != nil {
				gotError = true
				if !errors.Is(err, storage.ErrBucketNotFound) {
					t.Errorf("expected ErrBucketNotFound, got %v", err)
				}
			}
		}
		if !gotError {
			t.Fatal("expected error for non-existent bucket")
		}
	})

	t.Run("input iterator with errors", func(t *testing.T) {
		inputError := errors.New("input error")
		results := storage.DeleteObjects(ctx, func(yield func(string, error) bool) {
			yield("TestDeleteObjectsErrorHandling://:memory:/key-1", nil)
			yield("", inputError)
			yield("TestDeleteObjectsErrorHandling://:memory:/key-2", nil)
		})

		var foundInputError bool
		for _, err := range results {
			if errors.Is(err, inputError) {
				foundInputError = true
			}
		}
		if !foundInputError {
			t.Fatal("expected to receive input error in results")
		}
	})

	t.Run("invalid object key", func(t *testing.T) {
		results := storage.DeleteObjects(ctx, sequtil.Values([]string{
			"TestDeleteObjectsErrorHandling://:memory:/../invalid",
		}))

		var gotError bool
		for _, err := range results {
			if err != nil {
				gotError = true
			}
		}
		if !gotError {
			t.Fatal("expected error for invalid object key")
		}
	})
}

func TestDeleteObjectsContextCancellation(t *testing.T) {
	bucket := memory.NewBucket()
	// Add many objects
	for i := 0; i < 100; i++ {
		key := "key-" + string(rune('A'+i))
		bucket.PutObject(context.Background(), key, strings.NewReader("value"))
	}
	storage.Register("TestDeleteObjectsContextCancellation", storage.SingleBucketRegistry(bucket))

	t.Run("cancel during deletion", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		keys := make([]string, 100)
		for i := 0; i < 100; i++ {
			keys[i] = "TestDeleteObjectsContextCancellation://:memory:/key-" + string(rune('A'+i))
		}

		results := storage.DeleteObjects(ctx, sequtil.Values(keys))

		// Read a few results, then cancel
		count := 0
		for range results {
			count++
			if count == 5 {
				cancel()
			}
		}

		// Should have gotten at least some results before cancellation
		if count < 5 {
			t.Errorf("expected at least 5 results before cancellation, got %d", count)
		}
	})
}

func TestDeleteObjectsEdgeCases(t *testing.T) {
	ctx := t.Context()

	t.Run("empty iterator", func(t *testing.T) {
		results := storage.DeleteObjects(ctx, sequtil.Values([]string{}))

		count := 0
		for range results {
			count++
		}
		if count != 0 {
			t.Errorf("expected 0 results for empty iterator, got %d", count)
		}
	})

	t.Run("single object", func(t *testing.T) {
		bucket := memory.NewBucket(
			&memory.Entry{Key: "key-1", Value: []byte("value-1")},
		)
		storage.Register("TestDeleteObjectsEdgeCasesSingle", storage.SingleBucketRegistry(bucket))

		results := storage.DeleteObjects(ctx, sequtil.Values([]string{
			"TestDeleteObjectsEdgeCasesSingle://:memory:/key-1",
		}))

		count := 0
		for _, err := range results {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 result, got %d", count)
		}
	})

	t.Run("all objects from same bucket", func(t *testing.T) {
		bucket := memory.NewBucket(
			&memory.Entry{Key: "key-1", Value: []byte("value-1")},
			&memory.Entry{Key: "key-2", Value: []byte("value-2")},
			&memory.Entry{Key: "key-3", Value: []byte("value-3")},
		)
		storage.Register("TestDeleteObjectsEdgeCasesSame", storage.SingleBucketRegistry(bucket))

		results := storage.DeleteObjects(ctx, sequtil.Values([]string{
			"TestDeleteObjectsEdgeCasesSame://:memory:/key-1",
			"TestDeleteObjectsEdgeCasesSame://:memory:/key-2",
			"TestDeleteObjectsEdgeCasesSame://:memory:/key-3",
		}))

		count := 0
		for _, err := range results {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
		}
		if count != 3 {
			t.Errorf("expected 3 results, got %d", count)
		}
	})

	t.Run("objects distributed across many buckets", func(t *testing.T) {
		for i := 1; i <= 10; i++ {
			bucket := memory.NewBucket(
				&memory.Entry{Key: "key-1", Value: []byte("value-1")},
			)
			storage.Register("TestDeleteObjectsEdgeCasesMany"+string(rune('0'+i)), storage.SingleBucketRegistry(bucket))
		}

		keys := make([]string, 10)
		for i := 1; i <= 10; i++ {
			keys[i-1] = "TestDeleteObjectsEdgeCasesMany" + string(rune('0'+i)) + "://:memory:/key-1"
		}

		results := storage.DeleteObjects(ctx, sequtil.Values(keys))

		count := 0
		for _, err := range results {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
		}
		if count != 10 {
			t.Errorf("expected 10 results, got %d", count)
		}
	})
}

func TestDeleteObjectsStreaming(t *testing.T) {
	ctx := t.Context()

	t.Run("early termination", func(t *testing.T) {
		bucket := memory.NewBucket(
			&memory.Entry{Key: "key-1", Value: []byte("value-1")},
			&memory.Entry{Key: "key-2", Value: []byte("value-2")},
			&memory.Entry{Key: "key-3", Value: []byte("value-3")},
		)
		storage.Register("TestDeleteObjectsStreamingEarly", storage.SingleBucketRegistry(bucket))

		results := storage.DeleteObjects(ctx, sequtil.Values([]string{
			"TestDeleteObjectsStreamingEarly://:memory:/key-1",
			"TestDeleteObjectsStreamingEarly://:memory:/key-2",
			"TestDeleteObjectsStreamingEarly://:memory:/key-3",
		}))

		count := 0
		for range results {
			count++
			if count >= 2 {
				break // Stop early
			}
		}
		if count != 2 {
			t.Errorf("expected to read 2 results before stopping, got %d", count)
		}
	})

	t.Run("large number of objects", func(t *testing.T) {
		bucket := memory.NewBucket()
		// Add 100 objects
		for i := 0; i < 100; i++ {
			key := "key-" + string(rune('A'+i))
			bucket.PutObject(context.Background(), key, strings.NewReader("value"))
		}
		storage.Register("TestDeleteObjectsStreamingLarge", storage.SingleBucketRegistry(bucket))

		keys := make([]string, 100)
		for i := 0; i < 100; i++ {
			keys[i] = "TestDeleteObjectsStreamingLarge://:memory:/key-" + string(rune('A'+i))
		}

		results := storage.DeleteObjects(ctx, sequtil.Values(keys))

		count := 0
		for _, err := range results {
			if err != nil {
				t.Fatalf("unexpected error at count %d: %v", count, err)
			}
			count++
		}
		if count != 100 {
			t.Errorf("expected 100 results, got %d", count)
		}
	})
}

func TestDeleteObjectsConcurrency(t *testing.T) {
	bucket1 := memory.NewBucket(
		&memory.Entry{Key: "key-1", Value: []byte("value-1")},
		&memory.Entry{Key: "key-2", Value: []byte("value-2")},
	)
	bucket2 := memory.NewBucket(
		&memory.Entry{Key: "key-1", Value: []byte("value-1")},
		&memory.Entry{Key: "key-2", Value: []byte("value-2")},
	)
	bucket3 := memory.NewBucket(
		&memory.Entry{Key: "key-1", Value: []byte("value-1")},
		&memory.Entry{Key: "key-2", Value: []byte("value-2")},
	)

	storage.Register("TestDeleteObjectsConcurrency1", storage.SingleBucketRegistry(bucket1))
	storage.Register("TestDeleteObjectsConcurrency2", storage.SingleBucketRegistry(bucket2))
	storage.Register("TestDeleteObjectsConcurrency3", storage.SingleBucketRegistry(bucket3))

	ctx := t.Context()

	results := storage.DeleteObjects(ctx, sequtil.Values([]string{
		"TestDeleteObjectsConcurrency1://:memory:/key-1",
		"TestDeleteObjectsConcurrency2://:memory:/key-1",
		"TestDeleteObjectsConcurrency3://:memory:/key-1",
		"TestDeleteObjectsConcurrency1://:memory:/key-2",
		"TestDeleteObjectsConcurrency2://:memory:/key-2",
		"TestDeleteObjectsConcurrency3://:memory:/key-2",
	}))

	count := 0
	receivedKeys := make(map[string]bool)
	for key, err := range results {
		if err != nil {
			t.Fatalf("unexpected error for key %s: %v", key, err)
		}
		if key != "" {
			receivedKeys[key] = true
		}
		count++
	}

	if count < 6 {
		t.Errorf("expected at least 6 results, got %d", count)
	}

	// Verify all keys were received (note: URI format returned omits leading slash on object key)
	expectedKeys := []string{
		"TestDeleteObjectsConcurrency1://:memory:key-1",
		"TestDeleteObjectsConcurrency2://:memory:key-1",
		"TestDeleteObjectsConcurrency3://:memory:key-1",
		"TestDeleteObjectsConcurrency1://:memory:key-2",
		"TestDeleteObjectsConcurrency2://:memory:key-2",
		"TestDeleteObjectsConcurrency3://:memory:key-2",
	}
	for _, key := range expectedKeys {
		if !receivedKeys[key] {
			t.Errorf("expected key %s not received (received %d keys: %v)", key, len(receivedKeys), receivedKeys)
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

func TestRegistryFuncCaching(t *testing.T) {
	loadCount := 0
	reg := storage.RegistryFunc(func(ctx context.Context, bucketURI string) (storage.Bucket, error) {
		loadCount++
		return &fakeBucket{uri: bucketURI}, nil
	})

	ctx := t.Context()

	// First load
	bucket1, err := reg.LoadBucket(ctx, "test://bucket")
	if err != nil {
		t.Fatal(err)
	}

	// Second load - should return cached instance
	bucket2, err := reg.LoadBucket(ctx, "test://bucket")
	if err != nil {
		t.Fatal(err)
	}

	// Should be the same instance
	if bucket1 != bucket2 {
		t.Error("expected same bucket instance to be returned from cache")
	}

	// Load function should only have been called once
	if loadCount != 1 {
		t.Errorf("expected load function to be called 1 time, got %d", loadCount)
	}

	// Different URI should trigger a new load
	bucket3, err := reg.LoadBucket(ctx, "test://other-bucket")
	if err != nil {
		t.Fatal(err)
	}

	if bucket1 == bucket3 {
		t.Error("expected different bucket instance for different URI")
	}

	if loadCount != 2 {
		t.Errorf("expected load function to be called 2 times, got %d", loadCount)
	}
}

func TestRegistryFuncURINormalization(t *testing.T) {
	loadCount := 0
	reg := storage.RegistryFunc(func(ctx context.Context, bucketURI string) (storage.Bucket, error) {
		loadCount++
		return &fakeBucket{uri: bucketURI}, nil
	})

	ctx := t.Context()

	// Load with trailing slash
	bucket1, err := reg.LoadBucket(ctx, "test://bucket/")
	if err != nil {
		t.Fatal(err)
	}

	// Load without trailing slash - should return same cached instance
	bucket2, err := reg.LoadBucket(ctx, "test://bucket")
	if err != nil {
		t.Fatal(err)
	}

	// Should be the same instance due to URI normalization
	if bucket1 != bucket2 {
		t.Error("expected same bucket instance for normalized URIs")
	}

	// Load function should only have been called once
	if loadCount != 1 {
		t.Errorf("expected load function to be called 1 time, got %d", loadCount)
	}
}

// TestLoadBucketWithPath verifies that storage.LoadBucket correctly passes
// the full URI path to registries. Previously, LoadBucket stripped the path
// and used WithPrefix, which broke backends like HTTP where the path is part
// of the URL rather than an S3-style key prefix.
func TestLoadBucketWithPath(t *testing.T) {
	t.Run("memoryRegistryWithPath", func(t *testing.T) {
		// Verify that loading a memory bucket with a path correctly
		// treats the path as a prefix for object operations
		bucket := memory.NewBucket(
			&memory.Entry{Key: "sub/path/file1.txt", Value: []byte("v1")},
			&memory.Entry{Key: "sub/path/file2.txt", Value: []byte("v2")},
			&memory.Entry{Key: "other/file3.txt", Value: []byte("v3")},
		)
		storage.Register("TestLoadBucketWithPath", storage.SingleBucketRegistry(bucket))

		// Load with a path — should only see objects under "sub/path/"
		prefixBucket, err := storage.LoadBucket(t.Context(), "TestLoadBucketWithPath://:memory:/sub/path")
		if err != nil {
			t.Fatal(err)
		}

		// ListObjects should only return objects under the prefix
		var keys []string
		for obj, err := range prefixBucket.ListObjects(t.Context()) {
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, obj.Key)
		}
		if len(keys) != 2 {
			t.Fatalf("expected 2 objects under sub/path/, got %d: %v", len(keys), keys)
		}

		// GetObject should work with keys relative to the prefix
		r, _, err := prefixBucket.GetObject(t.Context(), "file1.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		data, _ := io.ReadAll(r)
		if string(data) != "v1" {
			t.Errorf("expected 'v1', got %q", data)
		}

		// PutObject should write under the prefix
		if _, err := prefixBucket.PutObject(t.Context(), "file4.txt", strings.NewReader("v4")); err != nil {
			t.Fatal(err)
		}
		// Verify via the underlying bucket
		r2, _, err := bucket.GetObject(t.Context(), "sub/path/file4.txt")
		if err != nil {
			t.Fatal(err)
		}
		defer r2.Close()
		data2, _ := io.ReadAll(r2)
		if string(data2) != "v4" {
			t.Errorf("expected 'v4', got %q", data2)
		}
	})

	t.Run("registryReceivesFullPath", func(t *testing.T) {
		// Verify that the registry's load function receives the full path
		// (bucket name + path), not just the bucket name
		var receivedURI string
		reg := storage.RegistryFunc(func(ctx context.Context, bucketURI string) (storage.Bucket, error) {
			receivedURI = bucketURI
			return new(memory.Bucket), nil
		})
		storage.Register("TestRegistryFullPath", reg)

		_, err := storage.LoadBucket(t.Context(), "TestRegistryFullPath://my-bucket/some/prefix")
		if err != nil {
			t.Fatal(err)
		}

		// The registry should receive "my-bucket/some/prefix", not just "my-bucket"
		if receivedURI != "my-bucket/some/prefix" {
			t.Errorf("expected registry to receive 'my-bucket/some/prefix', got %q", receivedURI)
		}
	})

	t.Run("registryReceivesEmptyPathForRootBucket", func(t *testing.T) {
		// Verify that root-level buckets still work (no path component)
		var receivedURI string
		reg := storage.RegistryFunc(func(ctx context.Context, bucketURI string) (storage.Bucket, error) {
			receivedURI = bucketURI
			return new(memory.Bucket), nil
		})
		storage.Register("TestRegistryNoPath", reg)

		_, err := storage.LoadBucket(t.Context(), "TestRegistryNoPath://my-bucket")
		if err != nil {
			t.Fatal(err)
		}

		if receivedURI != "my-bucket" {
			t.Errorf("expected registry to receive 'my-bucket', got %q", receivedURI)
		}
	})
}

func TestWithAdapters(t *testing.T) {
	bucket := memory.NewBucket()
	baseRegistry := storage.SingleBucketRegistry(bucket)

	// Create a simple adapter that wraps buckets
	adapterCalled := false
	adapter := storage.AdapterFunc(func(b storage.Bucket) storage.Bucket {
		adapterCalled = true
		return b
	})

	// Wrap the registry with adapters
	adaptedReg := storage.WithAdapters(baseRegistry, adapter)

	ctx := t.Context()
	_, err := adaptedReg.LoadBucket(ctx, bucket.Location())
	if err != nil {
		t.Fatal(err)
	}

	if !adapterCalled {
		t.Error("expected adapter to be called")
	}
}

func TestWithAdaptersEmpty(t *testing.T) {
	bucket := memory.NewBucket()
	baseRegistry := storage.SingleBucketRegistry(bucket)

	// WithAdapters with no adapters should return the same registry
	adaptedReg := storage.WithAdapters(baseRegistry)

	if adaptedReg != baseRegistry {
		t.Error("expected WithAdapters with no adapters to return same registry")
	}
}

func TestWithAdaptersMultiple(t *testing.T) {
	bucket := memory.NewBucket()
	baseRegistry := storage.SingleBucketRegistry(bucket)

	// Track the order adapters are called
	var order []int
	adapter1 := storage.AdapterFunc(func(b storage.Bucket) storage.Bucket {
		order = append(order, 1)
		return b
	})
	adapter2 := storage.AdapterFunc(func(b storage.Bucket) storage.Bucket {
		order = append(order, 2)
		return b
	})
	adapter3 := storage.AdapterFunc(func(b storage.Bucket) storage.Bucket {
		order = append(order, 3)
		return b
	})

	adaptedReg := storage.WithAdapters(baseRegistry, adapter1, adapter2, adapter3)

	ctx := t.Context()
	_, err := adaptedReg.LoadBucket(ctx, bucket.Location())
	if err != nil {
		t.Fatal(err)
	}

	// Adapters should be called in order
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Errorf("expected adapters to be called in order [1, 2, 3], got %v", order)
	}
}

func TestCopyObject(t *testing.T) {
	bucket := memory.NewBucket(
		&memory.Entry{Key: "source.txt", Value: []byte("hello, world!")},
	)
	storage.Register("TestCopyObject", storage.SingleBucketRegistry(bucket))

	ctx := t.Context()

	t.Run("same bucket copy", func(t *testing.T) {
		err := storage.CopyObject(ctx, "TestCopyObject://:memory:/source.txt", "TestCopyObject://:memory:/dest.txt")
		if err != nil {
			t.Fatal("unexpected error copying object:", err)
		}

		// Verify destination exists
		r, info, err := storage.GetObject(ctx, "TestCopyObject://:memory:/dest.txt")
		if err != nil {
			t.Fatal("unexpected error reading destination object:", err)
		}
		defer r.Close()

		b, err := io.ReadAll(r)
		if err != nil {
			t.Fatal("unexpected error reading destination object:", err)
		}
		if string(b) != "hello, world!" {
			t.Fatalf("unexpected object data: %q", b)
		}
		if info.Size != 13 {
			t.Fatalf("unexpected object size: %d", info.Size)
		}

		// Verify source still exists
		r, _, err = storage.GetObject(ctx, "TestCopyObject://:memory:/source.txt")
		if err != nil {
			t.Fatal("source object should still exist:", err)
		}
		r.Close()
	})

	t.Run("source not found", func(t *testing.T) {
		err := storage.CopyObject(ctx, "TestCopyObject://:memory:/does-not-exist", "TestCopyObject://:memory:/dest2.txt")
		if !errors.Is(err, storage.ErrObjectNotFound) {
			t.Fatalf("expected ErrObjectNotFound, got: %v", err)
		}
	})

	t.Run("invalid source URI", func(t *testing.T) {
		err := storage.CopyObject(ctx, "invalid-uri", "TestCopyObject://:memory:/dest.txt")
		if err == nil {
			t.Fatal("expected error for invalid source URI")
		}
	})

	t.Run("invalid dest URI", func(t *testing.T) {
		err := storage.CopyObject(ctx, "TestCopyObject://:memory:/source.txt", "invalid-uri")
		if err == nil {
			t.Fatal("expected error for invalid dest URI")
		}
	})

	t.Run("bucket not found", func(t *testing.T) {
		err := storage.CopyObject(ctx, "NonExistent://:memory:/source.txt", "NonExistent://:memory:/dest.txt")
		if !errors.Is(err, storage.ErrBucketNotFound) {
			t.Fatalf("expected ErrBucketNotFound, got: %v", err)
		}
	})
}

func TestCopyObjectCrossBucket(t *testing.T) {
	srcBucket := memory.NewBucket(
		&memory.Entry{Key: "cross-src.txt", Value: []byte("cross-bucket content")},
	)
	dstBucket := memory.NewBucket()

	storage.Register("TestCopyObjectCrossSrc", storage.SingleBucketRegistry(srcBucket))
	storage.Register("TestCopyObjectCrossDst", storage.SingleBucketRegistry(dstBucket))

	ctx := t.Context()

	// Copy between different buckets - should use streaming fallback
	err := storage.CopyObject(ctx,
		"TestCopyObjectCrossSrc://:memory:/cross-src.txt",
		"TestCopyObjectCrossDst://:memory:/cross-dest.txt",
	)
	if err != nil {
		t.Fatal("unexpected error copying object across buckets:", err)
	}

	// Verify destination exists in destination bucket
	r, info, err := storage.GetObject(ctx, "TestCopyObjectCrossDst://:memory:/cross-dest.txt")
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}
	defer r.Close()

	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}
	if string(b) != "cross-bucket content" {
		t.Fatalf("unexpected object data: %q", b)
	}
	if info.Size != int64(len("cross-bucket content")) {
		t.Fatalf("unexpected object size: %d", info.Size)
	}

	// Verify source still exists in source bucket
	r, _, err = storage.GetObject(ctx, "TestCopyObjectCrossSrc://:memory:/cross-src.txt")
	if err != nil {
		t.Fatal("source object should still exist:", err)
	}
	r.Close()
}

func TestCopyObjectWithOptions(t *testing.T) {
	bucket := memory.NewBucket()
	storage.Register("TestCopyObjectWithOptions", storage.SingleBucketRegistry(bucket))

	ctx := t.Context()

	// Create source object with metadata using PutObject
	_, err := bucket.PutObject(ctx, "source.txt", strings.NewReader("content"),
		storage.ContentType("text/plain"),
		storage.CacheControl("max-age=3600"),
		storage.Metadata("key1", "value1"),
	)
	if err != nil {
		t.Fatal("unexpected error writing source object:", err)
	}

	// Copy with override options
	err = storage.CopyObject(ctx,
		"TestCopyObjectWithOptions://:memory:/source.txt",
		"TestCopyObjectWithOptions://:memory:/dest.txt",
		storage.ContentType("application/json"),
		storage.Metadata("key2", "value2"),
	)
	if err != nil {
		t.Fatal("unexpected error copying object:", err)
	}

	// Verify destination has overridden metadata
	info, err := bucket.HeadObject(ctx, "dest.txt")
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}

	// Content-Type should be overridden
	if info.ContentType != "application/json" {
		t.Errorf("content type not overridden: %q", info.ContentType)
	}

	// Cache-Control should be preserved from source
	if info.CacheControl != "max-age=3600" {
		t.Errorf("cache control should be preserved: %q", info.CacheControl)
	}

	// Metadata should be merged
	if info.Metadata["key1"] != "value1" {
		t.Errorf("source metadata key1 should be preserved: %v", info.Metadata)
	}
	if info.Metadata["key2"] != "value2" {
		t.Errorf("override metadata key2 should be present: %v", info.Metadata)
	}
}

func TestCopyObjectAt(t *testing.T) {
	bucket := memory.NewBucket(
		&memory.Entry{Key: "source.txt", Value: []byte("custom registry content")},
	)
	registry := storage.SingleBucketRegistry(bucket)

	ctx := t.Context()

	// Use CopyObjectAt with custom registry
	// The URI format must match the bucket location
	bucketLocation := bucket.Location()
	err := storage.CopyObjectAt(ctx, registry,
		bucketLocation+"/source.txt",
		bucketLocation+"/dest.txt",
	)
	if err != nil {
		t.Fatal("unexpected error copying object:", err)
	}

	// Verify destination exists
	r, info, err := bucket.GetObject(ctx, "dest.txt")
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}
	defer r.Close()

	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}
	if string(b) != "custom registry content" {
		t.Fatalf("unexpected object data: %q", b)
	}
	if info.Size != int64(len("custom registry content")) {
		t.Fatalf("unexpected object size: %d", info.Size)
	}
}

func TestCopyObjectContextCancellation(t *testing.T) {
	bucket := memory.NewBucket(
		&memory.Entry{Key: "source.txt", Value: []byte("content")},
	)
	storage.Register("TestCopyObjectContextCancellation", storage.SingleBucketRegistry(bucket))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := storage.CopyObject(ctx,
		"TestCopyObjectContextCancellation://:memory:/source.txt",
		"TestCopyObjectContextCancellation://:memory:/dest.txt",
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
}
