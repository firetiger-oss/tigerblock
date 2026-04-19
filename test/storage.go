package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"iter"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/gs"
	storagehttp "github.com/firetiger-oss/storage/http"
	"github.com/firetiger-oss/storage/internal/sequtil"
	"github.com/firetiger-oss/storage/uri"
)

// assertMetadataContains fails the test unless every key in want is present in
// got with the matching value. Extra keys in got are tolerated — backends with
// native POSIX-permission semantics (notably file://) always surface "mode",
// "uid" and "gid" from the underlying inode.
func assertMetadataContains(t *testing.T, got, want map[string]string, msg string) {
	t.Helper()
	for k, v := range want {
		actual, ok := got[k]
		if !ok {
			t.Fatalf("%s: missing key %q (got %v)", msg, k, got)
		}
		if actual != v {
			t.Fatalf("%s: key %q got %q, want %q (full got=%v)", msg, k, actual, v, got)
		}
	}
}

func TestStorage(t *testing.T, loadBucket func(*testing.T) (storage.Bucket, error)) {
	adapters := []struct {
		name    string
		adapter storage.Adapter
	}{
		{
			name:    "base",
			adapter: storage.AdapterFunc(func(b storage.Bucket) storage.Bucket { return b }),
		},
		{
			name:    "instrumentation",
			adapter: storage.WithInstrumentation(),
		},
		{
			name:    "logging",
			adapter: storage.WithLogger(slog.Default()),
		},
		{
			name:    "prefixed",
			adapter: storage.WithPrefix("test-prefix/"),
		},
		{
			name:    "schemed",
			adapter: storage.WithScheme("testy"),
		},
	}

	tests := []struct {
		scenario string
		function func(*testing.T, storage.Bucket)

		// skipIf will be evaluated against the *unadapted*
		// bucket, to let us skip a few tests which don't work
		// with the GCS mock client.
		skipIf func(storage.Bucket) (skip bool, reason string)
	}{
		{
			scenario: "creating a bucket that already exists returns an error",
			function: testStorageCreateBucketExist,
		},

		{
			scenario: "buckets can be accessed after being created",
			function: testStorageCreateAndAccessBucket,
		},

		{
			scenario: "reading an object that does not exist returns an error",
			function: testStorageGetObjectNotExist,
		},

		{
			scenario: "reading an object range with a negative offset returns an error",
			function: testStorageGetObjectRangeNegativeOffset,
		},

		{
			scenario: "objects written to the bucket can be read",
			function: testStorageWriteAndGetObject,
		},

		{
			scenario: "object deletion is idempotent",
			function: testStorageDeleteObjectIsIdempotent,
		},

		{
			scenario: "deleted objects cannot be read anymore",
			function: testStorageDeleteObjectIsGone,
		},

		{
			scenario: "deleting multiple objects is idempotent",
			function: testStorageDeleteObjectsIsIdempotent,
		},

		{
			scenario: "objects deleted in bulk cannot be read anymore",
			function: testStorageDeleteObjectsIsGone,
		},

		{
			scenario: "GetObject with an invalid path returns an error",
			function: testStorageGetObjectInvalidPath,
		},

		{
			scenario: "PutObject with an invalid path returns an error",
			function: testStoragePutObjectInvalidPath,
		},

		{
			scenario: "PutObject with an io.Reader of unknown size",
			function: testStoragePutObjectStream,
		},

		{
			scenario: "PutObject with a content type",
			function: testStoragePutObjectContentType,
		},

		{
			scenario: "PutObject with cache control",
			function: testStoragePutObjectCacheControl,
			skipIf: func(bucket storage.Bucket) (bool, string) {
				if _, ok := bucket.(*gs.Bucket); ok {
					return true, "GCS mock client does not support Cache-Control properly"
				}
				return false, ""
			},
		},

		{
			scenario: "PutObject with metadata",
			function: testStoragePutObjectMetadata,
		},

		{
			scenario: "PutObjectIfMatch fails when the object does not exist",
			function: testStoragePutObjectIfMatchNotExist,
		},

		{
			scenario: "PutObjectIfMatch succeeds when the object exists and the etag matches",
			function: testStoragePutObjectIfMatchExistAndMatch,
		},

		{
			scenario: "PutObjectIfMatch fails when the object exists and the etag does not match",
			function: testStoragePutObjectIfMatchExistAndNotMatch,
		},

		{
			scenario: "PutObjectIfNoneMatch succeeds when the object does not exist",
			function: testStoragePutObjectIfNoneMatchNotExist,
		},

		{
			scenario: "PutObjectIfNoneMatch fails when the object exist",
			function: testStoragePutObjectIfNoneMatchExist,
		},

		{
			scenario: "PutObjectIfNoneMatch fails when the etag is not '*'",
			function: testStoragePutObjectIfNoneMatchInvalidEtag,
		},

		{
			scenario: "DeleteObject with an invalid path returns an error",
			function: testStorageDeleteObjectInvalidPath,
		},

		{
			scenario: "DeleteObjects with an invalid path returns an error",
			function: testStorageDeleteObjectsInvalidPath,
		},

		{
			scenario: "trailing-slash keys round-trip through Put/Head/Delete",
			function: testStorageDirKeyRoundTrip,
		},

		{
			scenario: "a directory marker and regular children coexist",
			function: testStorageDirKeyThenChild,
		},

		{
			scenario: "ListObjects returns nothing on an empty bucket",
			function: testStorageListObjectsEmpty,
		},

		{
			scenario: "ListObjects returns objects in the bucket",
			function: testStorageListObjectsNotEmpty,
		},

		{
			scenario: "listing a key prefix that does not exist does not error",
			function: testStorageListObjectsPrefixNotExist,
		},

		{
			scenario: "listing objects recurses into subdirectories",
			function: testStorageListObjectsRecursive,
		},

		{
			scenario: "listing objects does not recurse into subdirectories when a delimiter is set",
			function: testStorageListObjectsWithDelimiter,
		},

		{
			scenario: "listing objects respects the start after option",
			function: testStorageListObjectsStartAfter,
		},

		{
			scenario: "listing objects respects the max keys option",
			function: testStorageListObjectsMaxKeys,
		},

		{
			scenario: "watching objects returns initial objects",
			function: testStorageWatchInitialObjects,
		},

		{
			scenario: "watching objects respects context cancellation",
			function: testStorageWatchContextCancellation,
		},

		{
			scenario: "watching objects with prefix filter",
			function: testStorageWatchWithPrefix,
		},

		{
			scenario: "watching objects detects new object creation",
			function: testStorageWatchObjectCreation,
		},

		{
			scenario: "watching objects detects object updates",
			function: testStorageWatchObjectUpdates,
		},

		{
			scenario: "watching objects detects object deletion",
			function: testStorageWatchObjectDeletion,
		},

		{
			scenario: "watching objects detects multiple concurrent changes",
			function: testStorageWatchMultipleChanges,
		},

		{
			scenario: "canceled context returns context canceled",
			function: testStorageContextCanceled,
			skipIf: func(bucket storage.Bucket) (bool, string) {
				if _, ok := bucket.(*gs.Bucket); ok {
					return true, "GCS mock client does not support context cancellation"
				}
				return false, ""
			},
		},
		{
			scenario: "HeadObject on a missing object returns ErrNotFound",
			function: testStorageHeadObjectNotFound,
		},
		{
			scenario: "Location must include a scheme component in its URI",
			function: testStorageLocationHasScheme,
			skipIf: func(bucket storage.Bucket) (bool, string) {
				_, bucketName, _ := uri.Split(bucket.Location())
				if bucketName == ":memory:" {
					// This isn't actually desirable, but it feels
					// like a big refactor to fix.
					return true, "memory bucket does not provide locations with a schema"
				}
				return false, ""
			},
		},
		{
			scenario: "Presign method returns a URL, ErrPresignNotSupported, or ErrObjectNotFound",
			function: testStoragePresign,
		},

		{
			scenario: "CopyObject copies an object within the same bucket",
			function: testStorageCopyObject,
		},

		{
			scenario: "CopyObject returns ErrObjectNotFound when source does not exist",
			function: testStorageCopyObjectNotFound,
		},

		{
			scenario: "CopyObject with an invalid source path returns an error",
			function: testStorageCopyObjectInvalidSourcePath,
		},

		{
			scenario: "CopyObject with an invalid destination path returns an error",
			function: testStorageCopyObjectInvalidDestPath,
		},

		{
			scenario: "CopyObject preserves source metadata by default",
			function: testStorageCopyObjectPreservesMetadata,
			skipIf: func(bucket storage.Bucket) (bool, string) {
				if _, ok := bucket.(*gs.Bucket); ok {
					return true, "GCS mock client does not support Cache-Control properly"
				}
				return false, ""
			},
		},

		{
			scenario: "CopyObject with PutOptions overrides specific metadata",
			function: testStorageCopyObjectOverridesMetadata,
			skipIf: func(bucket storage.Bucket) (bool, string) {
				if _, ok := bucket.(*gs.Bucket); ok {
					return true, "GCS mock client does not support Cache-Control properly"
				}
				return false, ""
			},
		},

		{
			scenario: "CopyObject with canceled context returns context canceled",
			function: testStorageCopyObjectContextCanceled,
			skipIf: func(bucket storage.Bucket) (bool, string) {
				if _, ok := bucket.(*gs.Bucket); ok {
					return true, "GCS mock client does not support context cancellation"
				}
				return false, ""
			},
		},

		{
			scenario: "PutObject with matching SHA-256 checksum succeeds and stores the body",
			function: testStoragePutObjectChecksumSHA256Match,
		},

		{
			scenario: "PutObject with mismatched SHA-256 checksum returns ErrChecksumMismatch and does not store the body",
			function: testStoragePutObjectChecksumSHA256Mismatch,
		},

		{
			scenario: "PutObject without a checksum option behaves as before",
			function: testStoragePutObjectChecksumSHA256NoOption,
		},

		{
			scenario: "PutObject with mismatched ContentLength is rejected and does not store the body",
			function: testStoragePutObjectContentLengthMismatch,
			skipIf: func(bucket storage.Bucket) (bool, string) {
				if _, ok := bucket.(*gs.Bucket); ok {
					// Real GCS enforces Content-Length on the wire;
					// the fake-gcs-server we run tests against is
					// permissive. The HTTP layer doesn't reimplement
					// validation that the destination already does.
					return true, "fake-gcs-server does not enforce Content-Length; real GCS does"
				}
				if _, ok := bucket.(*storagehttp.Bucket); ok {
					// The backend (memory) here CAN validate. What
					// can't happen is the caller's explicit
					// ContentLength(999) reaching it: Go's transport
					// auto-sets the wire Content-Length from the
					// body's Len() (the actual length), so the
					// declared mismatch is erased before the request
					// goes out. A real mismatch is only observable
					// from a misbehaving non-Go client.
					return true, "Go HTTP transport overwrites caller's declared Content-Length with the body's actual length"
				}
				return false, ""
			},
		},
	}

	for _, adapter := range adapters {
		t.Run(adapter.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.scenario, func(t *testing.T) {
					bucket, err := loadBucket(t)
					if err != nil {
						t.Fatal("unexpected error loading bucket:", err)
					}
					if test.skipIf != nil {
						if skip, reason := test.skipIf(bucket); skip {
							t.Skip(reason)
						}
					}
					bucket = adapter.adapter.AdaptBucket(bucket)
					if err := bucket.Create(t.Context()); err != nil {
						if !errors.Is(err, storage.ErrBucketExist) {
							t.Fatal("unexpected error creating bucket:", err)
						}
					}
					test.function(t, bucket)
				})
			}
		})
	}
}

func testStorageCreateBucketExist(t *testing.T, bucket storage.Bucket) {
	if err := bucket.Create(t.Context()); !errors.Is(err, storage.ErrBucketExist) {
		t.Error("unexpected error:", err)
	}
}

func testStorageCreateAndAccessBucket(t *testing.T, bucket storage.Bucket) {
	if err := bucket.Access(t.Context()); err != nil {
		t.Error("unexpected error:", err)
	}
}

func testStorageGetObjectNotExist(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	if _, _, err := bucket.GetObject(ctx, "does-not-exist"); err == nil {
		t.Fatal("expected error reading object that does not exist")
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}

	_, _, err := bucket.GetObject(ctx, "does-not-exist", storage.BytesRange(0, 0))
	if err == nil {
		t.Fatal("expected error reading object that does not exist")
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testStorageGetObjectRangeNegativeOffset(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	object, err := bucket.PutObject(ctx, "test-object", strings.NewReader("hello, world!"))
	if err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 13 {
		t.Fatalf("unexpected object size: %d != 13", object.Size)
	}

	_, _, err = bucket.GetObject(ctx, "test-object", storage.BytesRange(-1, 1))
	if !errors.Is(err, storage.ErrInvalidRange) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testStorageWriteAndGetObject(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const key = "test-object"
	const data = "hello, world!"

	object, err := bucket.PutObject(ctx, key, strings.NewReader(data))
	if err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 13 {
		t.Fatalf("unexpected object size: %d != 13", object.Size)
	}

	r, object, err := bucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	if string(b) != data {
		t.Fatalf("unexpected object data: %q != %q", b, data)
	}
	if object.ETag == "" {
		t.Fatal("unexpected empty object ETag")
	}
	if d := time.Since(object.LastModified); d < 0 || d > 10*time.Second {
		t.Fatalf("unexpected object last modified time: %v (delta=%s)", object.LastModified, d)
	}

	for i := range data {
		r, info, err := bucket.GetObject(ctx, key, storage.BytesRange(int64(i), int64(i)))
		if err != nil {
			t.Fatal("unexpected error reading object:", err)
		} else if info.Size != int64(len(data)) {
			t.Fatalf("unexpected object size: %d != %d", info.Size, len(data))
		} else {
			b, err := io.ReadAll(r)
			r.Close()
			if err != nil {
				t.Fatal("unexpected error reading object:", err)
			} else if string(b) != data[i:i+1] {
				t.Fatalf("unexpected object data at offset %d: %q != %q", i, b, data[i:i+1])
			}
		}
	}
}

func testStorageDeleteObjectIsIdempotent(t *testing.T, bucket storage.Bucket) {
	if err := bucket.DeleteObject(t.Context(), "does-not-exist"); err != nil {
		t.Fatal("unexpected error deleting object:", err)
	}
}

func testStorageDeleteObjectIsGone(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const key = "test-object"

	object, err := bucket.PutObject(ctx, key, strings.NewReader("hello, world!"))
	if err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 13 {
		t.Fatalf("unexpected object size: %d != 13", object.Size)
	}

	if err := bucket.DeleteObject(ctx, key); err != nil {
		t.Fatal("unexpected error deleting object:", err)
	}

	r, _, err := bucket.GetObject(ctx, key)
	if err == nil {
		r.Close()
		t.Fatal("expected error reading deleted object")
	}
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testStorageDeleteObjectsIsIdempotent(t *testing.T, bucket storage.Bucket) {
	results := bucket.DeleteObjects(t.Context(), sequtil.Values([]string{
		"A", "B", "C",
	}))
	// Consume results and check for errors
	for key, err := range results {
		if err != nil {
			t.Fatalf("unexpected error deleting object %s: %v", key, err)
		}
	}
}

func testStorageDeleteObjectsIsGone(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const keyA = "test-object-a"
	const keyB = "test-object-b"
	const keyC = "test-object-c"
	const value = "hello, world!"

	if object, err := bucket.PutObject(ctx, keyA, strings.NewReader(value)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != int64(len(value)) {
		t.Fatalf("unexpected object size: %d != %d", object.Size, len(value))
	}

	if object, err := bucket.PutObject(ctx, keyB, strings.NewReader(value)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != int64(len(value)) {
		t.Fatalf("unexpected object size: %d != %d", object.Size, len(value))
	}

	if object, err := bucket.PutObject(ctx, keyC, strings.NewReader(value)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != int64(len(value)) {
		t.Fatalf("unexpected object size: %d != %d", object.Size, len(value))
	}

	// Delete objects using iterator API
	results := bucket.DeleteObjects(ctx, sequtil.Values([]string{keyA, keyC}))
	for key, err := range results {
		if err != nil {
			t.Fatalf("unexpected error deleting object %s: %v", key, err)
		}
	}

	_, _, err := bucket.GetObject(ctx, keyA)
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected error reading deleted object")
	}

	_, _, err = bucket.GetObject(ctx, keyC)
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatal("expected error reading deleted object")
	}

	r, info, err := bucket.GetObject(ctx, keyB)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	if string(b) != "hello, world!" {
		t.Fatalf("unexpected object data: %q != %q", b, "hello, world!")
	}
	if info.Size != int64(len(b)) {
		t.Fatalf("unexpected object size: %d != %d", info.Size, len(b))
	}
}

var InvalidPaths = []string{
	"",
	"/",
	"//",
	"/a//b",
	"/a/b/",
	"/a/b//",
	"/a/b/c/",
	"/a/b/c//",
	"a//",
	"a/..",
	"a/../",
	"../",
	"./",
	"a/.",
	"a/./b",
}

// ValidDirKeys enumerates keys that are accepted as directory markers: a
// single trailing "/" turns an otherwise valid path into a directory-like
// object (the S3-console "folder" convention).
var ValidDirKeys = []string{
	"a/",
	"a/b/",
	"a/b/c/",
}

func testStorageGetObjectInvalidPath(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	for _, path := range InvalidPaths {
		if _, _, err := bucket.GetObject(ctx, path); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected invalid path error: %s: %v", path, err)
		}
	}
}

func testStoragePutObjectInvalidPath(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	empty := strings.NewReader("")

	for _, path := range InvalidPaths {
		if _, err := bucket.PutObject(ctx, path, empty); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected invalid path error: %s: %v", path, err)
		}
	}
}

func testStoragePutObjectStream(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "multi/part/upload"
	data := strings.Repeat("1234567890", 1e6)

	object, err := bucket.PutObject(ctx, key, struct{ io.Reader }{strings.NewReader(data)})
	if err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != int64(len(data)) {
		t.Fatalf("unexpected object size: %d != %d", object.Size, len(data))
	}

	r, object, err := bucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	defer r.Close()

	if object.ContentType != "application/octet-stream" {
		t.Errorf("default content type must be application/octet-stream but got %q", object.ContentType)
	}

	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	if len(b) != len(data) {
		t.Fatalf("unexpected object size: %d != %d", len(b), len(data))
	}
	if string(b) != data {
		t.Fatalf("unexpected object data: %q != %q", b[:10], data[:10])
	}
}

func testStoragePutObjectContentType(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"
	data := "hello, world!"
	contentType := "text/plain"

	putObject, err := bucket.PutObject(ctx, key, strings.NewReader(data), storage.ContentType(contentType))
	if err != nil {
		t.Fatal("PutObject: unexpected error writing object:", err)
	}
	if putObject.ContentType != contentType {
		t.Fatalf("PutObject: unexpected content type: %q != %q", putObject.ContentType, contentType)
	}

	headObject, err := bucket.HeadObject(ctx, key)
	if err != nil {
		t.Fatal("HeadObject: unexpected error reading object:", err)
	}
	if headObject.ContentType != contentType {
		t.Fatalf("HeadObject: unexpected content type: %q != %q", headObject.ContentType, contentType)
	}
	if headObject.Size != int64(len(data)) {
		t.Fatalf("HeadObject: unexpected object size: %d != %d", headObject.Size, len(data))
	}

	r, getObject, err := bucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("GetObject: unexpected error reading object:", err)
	}
	r.Close()
	if getObject.ContentType != contentType {
		t.Fatalf("GetObject: unexpected content type: %q != %q", getObject.ContentType, contentType)
	}
	if getObject.Size != int64(len(data)) {
		t.Fatalf("GetObject: unexpected object size: %d != %d", getObject.Size, len(data))
	}
}

func testStoragePutObjectIfMatchNotExist(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"
	data := "hello, world!"

	object, err := bucket.PutObject(ctx, key, strings.NewReader(data), storage.IfMatch("7fffffffffffffff"))
	if !errors.Is(err, storage.ErrObjectNotMatch) {
		t.Fatal("unexpected error on failed PutObject:", err)
	}
	if object.Size != 0 {
		t.Fatal("unexpected non-zero size on failed PutObject:", object.Size)
	}
	if object.ETag != "" {
		t.Fatal("unexpected non-empty etag on failed PutObject:", object.ETag)
	}
}

func testStoragePutObjectIfMatchExistAndMatch(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"
	data := "hello, world!"

	if object, err := bucket.PutObject(ctx, key, strings.NewReader(data)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 13 {
		t.Fatalf("unexpected object size: %d != 13", object.Size)
	}

	object, err := bucket.HeadObject(ctx, key)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}

	if put, err := bucket.PutObject(ctx, key, strings.NewReader(data), storage.IfMatch(object.ETag)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if put.Size != 13 {
		t.Fatalf("unexpected object size: %d != 13", put.Size)
	} else if put.ETag == "" {
		t.Fatal("unexpected empty etag")
	}
}

func testStoragePutObjectIfMatchExistAndNotMatch(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"

	if object, err := bucket.PutObject(ctx, key, strings.NewReader("value-1")); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 7 {
		t.Fatalf("unexpected object size: %d != 13", object.Size)
	}

	object, err := bucket.HeadObject(ctx, key)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}

	if object, err := bucket.PutObject(ctx, key, strings.NewReader("value-2")); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 7 {
		t.Fatalf("unexpected object size: %d != 13", object.Size)
	}

	if _, err := bucket.PutObject(ctx, key, strings.NewReader("value-3"), storage.IfMatch(object.ETag)); !errors.Is(err, storage.ErrObjectNotMatch) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testStoragePutObjectIfNoneMatchNotExist(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"
	data := "hello, world!"

	object, err := bucket.PutObject(ctx, key, strings.NewReader(data), storage.IfNoneMatch("*"))
	if err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 13 {
		t.Fatalf("unexpected object size: %d != 13", object.Size)
	} else if object.ETag == "" {
		t.Fatal("unexpected empty etag")
	}

	r, _, err := bucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	if string(b) != data {
		t.Fatalf("unexpected object data: %q != %q", b, data)
	}
}

func testStoragePutObjectIfNoneMatchExist(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"
	data := "hello, world!"

	object, err := bucket.PutObject(ctx, key, strings.NewReader(data))
	if err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != 13 {
		t.Fatalf("unexpected object size: %d != 13", object.Size)
	}

	if _, err := bucket.PutObject(ctx, key, strings.NewReader(data), storage.IfNoneMatch("*")); !errors.Is(err, storage.ErrObjectNotMatch) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testStoragePutObjectIfNoneMatchInvalidEtag(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"
	data := "hello, world!"

	if _, err := bucket.PutObject(ctx, key, strings.NewReader(data), storage.IfNoneMatch("invalid")); !errors.Is(err, storage.ErrInvalidObjectTag) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func testStoragePutObjectChecksumSHA256Match(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object-checksum-match"
	data := []byte("hello, checksum!")
	sum := sha256.Sum256(data)

	info, err := bucket.PutObject(ctx, key, bytes.NewReader(data), storage.ChecksumSHA256(sum))
	if err != nil {
		t.Fatalf("PutObject with matching checksum: %v", err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("ObjectInfo.Size = %d; want %d", info.Size, len(data))
	}

	rc, _, err := bucket.GetObject(ctx, key)
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("body mismatch: got %q want %q", got, data)
	}
}

func testStoragePutObjectChecksumSHA256Mismatch(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object-checksum-mismatch"
	data := []byte("hello, checksum!")
	wrong := sha256.Sum256([]byte("decoy"))

	_, err := bucket.PutObject(ctx, key, bytes.NewReader(data), storage.ChecksumSHA256(wrong))
	if err == nil {
		t.Fatal("expected error from checksum mismatch")
	}
	if !errors.Is(err, storage.ErrChecksumMismatch) {
		t.Fatalf("err = %v; want errors.Is(err, storage.ErrChecksumMismatch)", err)
	}

	if _, err := bucket.HeadObject(ctx, key); err == nil {
		t.Fatal("object stored despite checksum mismatch")
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("HeadObject after mismatch: got %v; want ErrObjectNotFound", err)
	}
}

func testStoragePutObjectContentLengthMismatch(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object-content-length-mismatch"
	data := []byte("twelve bytes")

	_, err := bucket.PutObject(ctx, key, bytes.NewReader(data), storage.ContentLength(999))
	if err == nil {
		t.Fatal("PutObject: expected error from declared ContentLength != streamed body length")
	}

	if _, err := bucket.HeadObject(ctx, key); err == nil {
		t.Fatal("object stored despite content length mismatch")
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("HeadObject after mismatch: got %v; want ErrObjectNotFound", err)
	}
}

func testStoragePutObjectChecksumSHA256NoOption(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object-checksum-noopt"
	data := []byte("plain, no checksum")

	info, err := bucket.PutObject(ctx, key, bytes.NewReader(data))
	if err != nil {
		t.Fatalf("PutObject without checksum: %v", err)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("ObjectInfo.Size = %d; want %d", info.Size, len(data))
	}
}

func testStorageDeleteObjectInvalidPath(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	for _, path := range InvalidPaths {
		if err := bucket.DeleteObject(ctx, path); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected invalid path error: %s: %v", path, err)
		}
	}
}

// testStorageDirKeyRoundTrip exercises the contract that every backend
// accepts trailing-slash keys (directory markers) for all CRUD operations.
// It writes a zero-byte object at a "foo/" key with metadata, reads it back
// via HeadObject and GetObject, and deletes it.
func testStorageDirKeyRoundTrip(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	for _, key := range ValidDirKeys {
		t.Run(key, func(t *testing.T) {
			if _, err := bucket.PutObject(ctx, key, strings.NewReader(""),
				storage.Metadata("custom", "value"),
			); err != nil {
				t.Fatalf("PutObject(%q): %v", key, err)
			}

			info, err := bucket.HeadObject(ctx, key)
			if err != nil {
				t.Fatalf("HeadObject(%q): %v", key, err)
			}
			if info.Size != 0 {
				t.Errorf("size: got %d, want 0", info.Size)
			}
			if info.Metadata["custom"] != "value" {
				t.Errorf("metadata[custom]: got %q, want %q", info.Metadata["custom"], "value")
			}

			rc, _, err := bucket.GetObject(ctx, key)
			if err != nil {
				t.Fatalf("GetObject(%q): %v", key, err)
			}
			rc.Close()

			if err := bucket.DeleteObject(ctx, key); err != nil {
				t.Errorf("DeleteObject(%q): %v", key, err)
			}

			if _, err := bucket.HeadObject(ctx, key); !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("after delete, HeadObject(%q): got %v, want ErrObjectNotFound", key, err)
			}
		})
	}
}

// testStorageDirKeyThenChild verifies that a directory marker and regular
// children under the same prefix can coexist: create "d/", then write "d/a",
// then read "d/a", then delete "d/a", then delete "d/".
func testStorageDirKeyThenChild(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	if _, err := bucket.PutObject(ctx, "d/", strings.NewReader("")); err != nil {
		t.Fatalf("PutObject(d/): %v", err)
	}
	if _, err := bucket.PutObject(ctx, "d/a", strings.NewReader("child")); err != nil {
		t.Fatalf("PutObject(d/a): %v", err)
	}

	rc, info, err := bucket.GetObject(ctx, "d/a")
	if err != nil {
		t.Fatalf("GetObject(d/a): %v", err)
	}
	defer rc.Close()
	if info.Size != int64(len("child")) {
		t.Errorf("child size: got %d, want %d", info.Size, len("child"))
	}

	if _, err := bucket.HeadObject(ctx, "d/"); err != nil {
		t.Errorf("HeadObject(d/) after writing child: %v", err)
	}

	if err := bucket.DeleteObject(ctx, "d/a"); err != nil {
		t.Errorf("DeleteObject(d/a): %v", err)
	}
	if err := bucket.DeleteObject(ctx, "d/"); err != nil {
		t.Errorf("DeleteObject(d/): %v", err)
	}
}

func testStorageDeleteObjectsInvalidPath(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	for _, path := range InvalidPaths {
		results := bucket.DeleteObjects(ctx, sequtil.Values([]string{
			"first/path/is/valid",
			path,
			"third/path/is/valid",
		}))
		// Consume results and look for invalid key error
		foundInvalidError := false
		for _, err := range results {
			if errors.Is(err, storage.ErrInvalidObjectKey) {
				foundInvalidError = true
				break
			}
		}
		if !foundInvalidError {
			t.Errorf("expected invalid path error for: %s", path)
		}
	}
}

func testStorageListObjectsEmpty(t *testing.T, bucket storage.Bucket) {
	for _, err := range bucket.ListObjects(t.Context()) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		} else {
			t.Fatal("expected no objects in the bucket")
		}
	}
}

func testStorageListObjectsNotEmpty(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const keyA = "test-object-a"
	const keyB = "test-object-b"
	const keyC = "oops-object-c"
	const valueA = "1"
	const valueB = "23"
	const valueC = "456"

	if object, err := bucket.PutObject(ctx, keyA, strings.NewReader(valueA)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != int64(len(valueA)) {
		t.Fatalf("unexpected object size: %d != %d", object.Size, len(valueA))
	}

	if object, err := bucket.PutObject(ctx, keyB, strings.NewReader(valueB)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != int64(len(valueB)) {
		t.Fatalf("unexpected object size: %d != %d", object.Size, len(valueB))
	}

	if object, err := bucket.PutObject(ctx, keyC, strings.NewReader(valueC)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	} else if object.Size != int64(len(valueC)) {
		t.Fatalf("unexpected object size: %d != %d", object.Size, len(valueC))
	}

	var objects []storage.Object
	for object, err := range bucket.ListObjects(ctx, storage.KeyPrefix("test-")) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		}
		if object.LastModified.IsZero() {
			t.Fatal("unexpected zero last modified time")
		}
		object.LastModified = time.Time{} // non-deterministic
		objects = append(objects, object)
	}

	expected := []storage.Object{
		{Key: keyA, Size: int64(len(valueA))},
		{Key: keyB, Size: int64(len(valueB))},
	}

	if !slices.Equal(objects, expected) {
		t.Error("unexpected objects:")
		t.Logf("want: %+v", expected)
		t.Logf("got:  %+v", objects)
	}
}

func testStorageListObjectsPrefixNotExist(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	for _, err := range bucket.ListObjects(ctx, storage.KeyPrefix("does-not-exist/")) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		} else {
			t.Fatal("expected no objects in the bucket")
		}
	}
}

func testStorageListObjectsRecursive(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const keyA = "test/object/a"
	const keyB = "test/object/b"
	const keyC = "test/object/sub/c"
	const value = "hello, world!"

	for _, key := range []string{keyA, keyB, keyC} {
		if object, err := bucket.PutObject(ctx, key, strings.NewReader(value)); err != nil {
			t.Fatal("unexpected error writing object:", err)
		} else if object.Size != int64(len(value)) {
			t.Fatalf("unexpected object size: %d != %d", object.Size, len(value))
		}
	}

	var objects []storage.Object
	for object, err := range bucket.ListObjects(ctx, storage.KeyPrefix("test/object/")) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		}
		objects = append(objects, object)
	}

	if len(objects) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(objects))
	}
	if objects[0].Key != keyA {
		t.Errorf("unexpected key: %q != %q", objects[0].Key, keyA)
	}
	if objects[1].Key != keyB {
		t.Errorf("unexpected key: %q != %q", objects[1].Key, keyB)
	}
	if objects[2].Key != keyC {
		t.Errorf("unexpected key: %q != %q", objects[2].Key, keyC)
	}
}

func testStorageListObjectsWithDelimiter(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const keyA = "test/object/a"
	const keyB = "test/object/z"
	const keyC = "test/object/sub/c"
	const keyD = "test/object/sub/d"
	const keyE = "test/object/hello/world"
	const keyF = "test/object/hello/you"

	for _, key := range []string{keyA, keyB, keyC, keyD, keyE, keyF} {
		if object, err := bucket.PutObject(ctx, key, strings.NewReader(key)); err != nil {
			t.Fatal("unexpected error writing object:", err)
		} else if object.Size != int64(len(key)) {
			t.Fatalf("unexpected object size: %d != %d", object.Size, len(key))
		}
	}

	var objects []storage.Object
	for object, err := range bucket.ListObjects(ctx,
		storage.KeyPrefix("test/object/"),
		storage.KeyDelimiter("/")) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		}
		objects = append(objects, object)
	}

	keys := slices.Collect(func(yield func(string) bool) {
		for _, object := range objects {
			yield(object.Key)
		}
	})

	want := []string{
		"test/object/a",
		"test/object/hello/",
		"test/object/sub/",
		"test/object/z",
	}

	if !slices.Equal(keys, want) {
		t.Errorf("unexpected keys:")
		t.Logf("want: %q", want)
		t.Logf("got:  %q", keys)
	}
}

func testStorageListObjectsStartAfter(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const keyA = "test/object/a"
	const keyB = "test/object/b"
	const keyC = "test/object/sub/c"
	const value = "hello, world!"

	for _, key := range []string{keyA, keyB, keyC} {
		if object, err := bucket.PutObject(ctx, key, strings.NewReader(value)); err != nil {
			t.Fatal("unexpected error writing object:", err)
		} else if object.Size != int64(len(value)) {
			t.Fatalf("unexpected object size: %d != %d", object.Size, len(value))
		}
	}

	var objects []storage.Object
	for object, err := range bucket.ListObjects(ctx,
		storage.KeyPrefix("test/object/"),
		storage.StartAfter("test/object/a"),
	) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		}
		objects = append(objects, object)
	}

	if len(objects) != 2 {
		t.Fatalf("expected 2 objects, got %d", len(objects))
	}
	if objects[0].Key != keyB {
		t.Errorf("unexpected key: %q != %q", objects[0].Key, keyB)
	}
	if objects[1].Key != keyC {
		t.Errorf("unexpected key: %q != %q", objects[1].Key, keyC)
	}
}

func testStorageContextCanceled(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	if err := bucket.Access(ctx); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
	if _, err := bucket.HeadObject(ctx, "test-key"); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
	if _, _, err := bucket.GetObject(ctx, "test-key"); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
	if _, err := bucket.PutObject(ctx, "test-key", bytes.NewReader(nil)); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
	if _, err := bucket.PutObject(ctx, "test-key", bytes.NewReader(nil), storage.IfMatch("1234567890")); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
	if _, err := bucket.PutObject(ctx, "test-key", bytes.NewReader(nil), storage.IfNoneMatch("*")); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
	if err := bucket.DeleteObject(ctx, "test-key"); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
	// Test DeleteObjects with cancelled context
	results := bucket.DeleteObjects(ctx, sequtil.Values([]string{"test-key"}))
	for _, err := range results {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context canceled error, got %v", err)
		}
		break // Only check first result
	}
	next, stop := iter.Pull2(bucket.ListObjects(ctx))
	defer stop()
	if _, err, _ := next(); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
}

func testStoragePutObjectCacheControl(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"
	data := "hello, world!"
	cacheControl := "max-age=3600, public"

	putObject, err := bucket.PutObject(ctx, key, strings.NewReader(data), storage.CacheControl(cacheControl))
	if err != nil {
		t.Fatal("PutObject: unexpected error writing object:", err)
	}
	if putObject.CacheControl != cacheControl {
		t.Fatalf("PutObject: unexpected cache control: %q != %q", putObject.CacheControl, cacheControl)
	}

	headObject, err := bucket.HeadObject(ctx, key)
	if err != nil {
		t.Fatal("HeadObject: unexpected error reading object:", err)
	}
	if headObject.CacheControl != cacheControl {
		t.Fatalf("HeadObject: unexpected cache control: %q != %q", headObject.CacheControl, cacheControl)
	}

	r, getObject, err := bucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("GetObject: unexpected error reading object:", err)
	}
	defer r.Close()
	if getObject.CacheControl != cacheControl {
		t.Fatalf("GetObject: unexpected cache control: %q != %q", getObject.CacheControl, cacheControl)
	}
}

func testStoragePutObjectMetadata(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-object"
	data := "hello, world!"
	metadata := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"time": "2023-01-15-10",
	}

	putObject, err := bucket.PutObject(ctx, key, strings.NewReader(data),
		storage.Metadata("key1", "value1"),
		storage.Metadata("key2", "value2"),
		storage.Metadata("time", "2023-01-15-10"))
	if err != nil {
		t.Fatal("unexpected error writing object:", err)
	}
	assertMetadataContains(t, putObject.Metadata, metadata, "PutObject metadata")

	headObject, err := bucket.HeadObject(ctx, key)
	if err != nil {
		t.Fatal("unexpected error reading object metadata:", err)
	}
	assertMetadataContains(t, headObject.Metadata, metadata, "HeadObject metadata")

	r, getObject, err := bucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("unexpected error reading object:", err)
	}
	defer r.Close()
	assertMetadataContains(t, getObject.Metadata, metadata, "GetObject metadata")
}

func testStorageHeadObjectNotFound(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	_, err := bucket.HeadObject(ctx, "missing")
	if err == nil {
		t.Error("unexpected nil error")
	}
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Errorf("unexpected error type, have=%v (type=%T)", err, err)
	}
	_, err = bucket.HeadObject(ctx, "missing/in/directory")
	if err == nil {
		t.Error("unexpected nil error")
	}
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Errorf("unexpected error type, have=%v (type=%T)", err, err)
	}
}

func testStorageLocationHasScheme(t *testing.T, bucket storage.Bucket) {
	loc := bucket.Location()
	scheme, location, path := uri.Split(loc)
	if scheme == "" {
		t.Error("missing scheme in bucket.Location()")
		t.Logf("  location=%q", location)
		t.Logf("  path=%q", path)
	}
}

func testStorageListObjectsMaxKeys(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const keyA = "test/object/a"
	const keyB = "test/object/b"
	const keyC = "test/object/c"
	const keyD = "test/object/d"
	const keyE = "test/object/e"
	const value = "hello, world!"

	// Create 5 objects
	for _, key := range []string{keyA, keyB, keyC, keyD, keyE} {
		if object, err := bucket.PutObject(ctx, key, strings.NewReader(value)); err != nil {
			t.Fatal("unexpected error writing object:", err)
		} else if object.Size != int64(len(value)) {
			t.Fatalf("unexpected object size: %d != %d", object.Size, len(value))
		}
	}

	// Test MaxKeys(3) - should return only 3 objects
	var objects []storage.Object
	for object, err := range bucket.ListObjects(ctx,
		storage.KeyPrefix("test/object/"),
		storage.MaxKeys(3),
	) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		}
		objects = append(objects, object)
	}

	if len(objects) != 3 {
		t.Fatalf("expected 3 objects with MaxKeys(3), got %d", len(objects))
	}

	// Verify we got the first 3 objects (alphabetically sorted)
	expectedKeys := []string{keyA, keyB, keyC}
	for i, expected := range expectedKeys {
		if objects[i].Key != expected {
			t.Errorf("unexpected key at position %d: %q != %q", i, objects[i].Key, expected)
		}
	}

	// Test MaxKeys(0) - should return all objects (no limit)
	objects = nil
	for object, err := range bucket.ListObjects(ctx,
		storage.KeyPrefix("test/object/"),
		storage.MaxKeys(0),
	) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		}
		objects = append(objects, object)
	}

	if len(objects) != 5 {
		t.Fatalf("expected 5 objects with MaxKeys(0), got %d", len(objects))
	}

	// Test MaxKeys(1) - should return only 1 object
	objects = nil
	for object, err := range bucket.ListObjects(ctx,
		storage.KeyPrefix("test/object/"),
		storage.MaxKeys(1),
	) {
		if err != nil {
			t.Fatal("unexpected error listing objects:", err)
		}
		objects = append(objects, object)
	}

	if len(objects) != 1 {
		t.Fatalf("expected 1 object with MaxKeys(1), got %d", len(objects))
	}
	if objects[0].Key != keyA {
		t.Errorf("unexpected key: %q != %q", objects[0].Key, keyA)
	}
}

func testStorageWatchInitialObjects(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	// Create some initial objects
	const keyA = "file1.txt"
	const keyB = "file2.txt"
	const valueA = "content1"
	const valueB = "content2"

	if _, err := bucket.PutObject(ctx, keyA, strings.NewReader(valueA)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	}
	if _, err := bucket.PutObject(ctx, keyB, strings.NewReader(valueB)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	}

	// Start watching and collect objects
	seenObjects := make(map[string]bool)
	for obj, err := range bucket.WatchObjects(ctx) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break // Expected timeout
			}
			t.Fatal("unexpected error:", err)
		}

		seenObjects[obj.Key] = true

		// Stop after seeing both objects to prevent hanging
		if len(seenObjects) >= 2 {
			break
		}
	}

	if !seenObjects[keyA] {
		t.Error("expected to see file1.txt")
	}
	if !seenObjects[keyB] {
		t.Error("expected to see file2.txt")
	}
}

func testStorageWatchContextCancellation(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithCancel(t.Context())

	// Create a test object
	const key = "test.txt"
	const value = "content"

	if _, err := bucket.PutObject(ctx, key, strings.NewReader(value)); err != nil {
		t.Fatal("unexpected error writing object:", err)
	}

	// Start watching
	watchSeq := bucket.WatchObjects(ctx)

	// Cancel the context immediately
	cancel()

	// Should stop watching due to context cancellation
	watchStopped := true
	for _, err := range watchSeq {
		if err == nil {
			watchStopped = false
			t.Error("expected watch to stop immediately after context cancellation")
			break
		}
		// Expected error due to context cancellation
		break
	}

	if !watchStopped {
		t.Error("expected watch to be stopped after context cancellation")
	}
}

func testStorageWatchWithPrefix(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	// Create objects with different prefixes
	const keyA = "logs/app.log"
	const keyB = "logs/error.log"
	const keyC = "data/file.dat"
	const value = "content"

	for _, key := range []string{keyA, keyB, keyC} {
		if _, err := bucket.PutObject(ctx, key, strings.NewReader(value)); err != nil {
			t.Fatal("unexpected error writing object:", err)
		}
	}

	// Watch with prefix filter for logs only
	seenObjects := make(map[string]bool)
	for obj, err := range bucket.WatchObjects(ctx, storage.KeyPrefix("logs/")) {
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break // Expected timeout
			}
			t.Fatal("unexpected error:", err)
		}

		seenObjects[obj.Key] = true

		// Stop after seeing expected objects
		if len(seenObjects) >= 2 {
			break
		}
	}

	// Verify we only saw log files
	expectedLogs := []string{keyA, keyB}
	for _, expectedKey := range expectedLogs {
		if !seenObjects[expectedKey] {
			t.Errorf("expected to see %s", expectedKey)
		}
	}

	// Verify we didn't see data files
	if seenObjects[keyC] {
		t.Error("should not have seen data/file.dat with logs/ prefix filter")
	}
}

func testStorageWatchObjectCreation(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	// Start watching before creating objects
	next, stop := iter.Pull2(bucket.WatchObjects(ctx))
	defer stop()

	// Create new objects while watching
	const keyA = "created-file-a.txt"
	const keyB = "created-file-b.txt"
	const valueA = "content A"
	const valueB = "content B"

	if _, err := bucket.PutObject(ctx, keyA, strings.NewReader(valueA)); err != nil {
		t.Fatal("unexpected error creating object A:", err)
	}

	if _, err := bucket.PutObject(ctx, keyB, strings.NewReader(valueB)); err != nil {
		t.Fatal("unexpected error creating object B:", err)
	}

	// Pull events from the iterator to verify we see the created objects
	seenObjects := make(map[string]bool)

	for range 10 {
		obj, err, more := next()
		if !more {
			break
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatal("unexpected watch error:", err)
		}

		seenObjects[obj.Key] = true
		// If we've seen both objects, we're done
		if seenObjects[keyA] && seenObjects[keyB] {
			break
		}
	}

	if !seenObjects[keyA] {
		t.Error("expected to see created file A")
	}
	if !seenObjects[keyB] {
		t.Error("expected to see created file B")
	}
}

func testStorageWatchObjectUpdates(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	// Create initial object
	const key = "update-test.txt"
	const initialValue = "initial content"
	const updatedValue = "content after update"

	if _, err := bucket.PutObject(ctx, key, strings.NewReader(initialValue)); err != nil {
		t.Fatal("unexpected error creating initial object:", err)
	}

	// Start watching
	next, stop := iter.Pull2(bucket.WatchObjects(ctx))
	defer stop()

	// Pull the initial object from the watch
	seenInitial := false
	for range 5 {
		obj, err, more := next()
		if !more {
			break
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatal("unexpected watch error:", err)
		}

		if obj.Key == key && obj.Size == int64(len(initialValue)) {
			seenInitial = true
			break
		}
	}

	if !seenInitial {
		t.Fatal("expected to see initial object before update")
	}

	// Now update the object
	if _, err := bucket.PutObject(ctx, key, strings.NewReader(updatedValue)); err != nil {
		t.Fatal("unexpected error updating object:", err)
	}

	// Pull events to find the updated version
	hasUpdate := false
	for range 10 {
		obj, err, more := next()
		if !more {
			break
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatal("unexpected watch error:", err)
		}

		// Check if this is the updated version (different size indicates updated content)
		if obj.Key == key && obj.Size == int64(len(updatedValue)) {
			hasUpdate = true
			break
		}
	}

	if !hasUpdate {
		t.Error("expected to see the updated version of the object")
	}
}

func testStorageWatchObjectDeletion(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()

	// Create initial object
	const key = "delete-test.txt"
	const value = "content to be deleted"

	if _, err := bucket.PutObject(ctx, key, strings.NewReader(value)); err != nil {
		t.Fatal("unexpected error creating object:", err)
	}

	// Start watching
	next, stop := iter.Pull2(bucket.WatchObjects(ctx))
	defer stop()

	// Pull the initial object from the watch
	seenInitial := false
	for range 5 {
		obj, err, more := next()
		if !more {
			break
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatal("unexpected watch error:", err)
		}

		if obj.Key == key && obj.Size == int64(len(value)) {
			seenInitial = true
			break
		}
	}

	if !seenInitial {
		t.Fatal("expected to see initial object before deletion")
	}

	// Delete the object
	if err := bucket.DeleteObject(ctx, key); err != nil {
		t.Fatal("unexpected error deleting object:", err)
	}

	// Pull events to find the deletion marker
	hasDeleteMarker := false
	for range 10 {
		obj, err, more := next()
		if !more {
			break
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatal("unexpected watch error:", err)
		}

		// Check for deletion marker (Size: -1)
		if obj.Key == key && obj.Size == -1 {
			hasDeleteMarker = true
			break
		}
	}

	if !hasDeleteMarker {
		t.Error("expected to see deletion marker (Size: -1) for deleted object")
	}
}

func testStorageWatchMultipleChanges(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	// Start watching before making any changes
	next, stop := iter.Pull2(bucket.WatchObjects(ctx))
	defer stop()

	// Perform multiple operations sequentially
	const keyA = "multi-a.txt"
	const keyB = "multi-b.txt"
	const keyC = "multi-c.txt"
	const valueA = "content A"
	const valueB = "content B"
	const valueC = "content C"
	const updatedValueA = "updated content A"

	// Create objects
	if _, err := bucket.PutObject(ctx, keyA, strings.NewReader(valueA)); err != nil {
		t.Fatal("unexpected error creating object A:", err)
	}
	if _, err := bucket.PutObject(ctx, keyB, strings.NewReader(valueB)); err != nil {
		t.Fatal("unexpected error creating object B:", err)
	}
	if _, err := bucket.PutObject(ctx, keyC, strings.NewReader(valueC)); err != nil {
		t.Fatal("unexpected error creating object C:", err)
	}

	// Update one object
	if _, err := bucket.PutObject(ctx, keyA, strings.NewReader(updatedValueA)); err != nil {
		t.Fatal("unexpected error updating object A:", err)
	}

	// Pull events to verify all changes
	seenObjects := make(map[string]bool)
	hasUpdate := false
	hasDeleteMarker := false

	for len(seenObjects) < 3 || !hasUpdate || !hasDeleteMarker {
		obj, err, more := next()
		if !more {
			break
		}
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			t.Fatal("unexpected watch error:", err)
		}

		seenObjects[obj.Key] = true

		// Check for deletion marker
		if obj.Key == keyC {
			if obj.Size == -1 {
				hasDeleteMarker = true
			} else {
				// Delete one object
				if err := bucket.DeleteObject(ctx, keyC); err != nil {
					t.Fatal("unexpected error deleting object C:", err)
				}
			}
		}

		// Check for update (larger size indicates updated content)
		if obj.Key == keyA && obj.Size == int64(len(updatedValueA)) {
			hasUpdate = true
		}
	}

	// Verify we saw all the objects
	if !seenObjects[keyA] {
		t.Error("expected to see object A")
	}
	if !seenObjects[keyB] {
		t.Error("expected to see object B")
	}
	if !seenObjects[keyC] {
		t.Error("expected to see object C")
	}

	if !hasUpdate {
		t.Error("expected to see updated version of object A")
	}
	if !hasDeleteMarker {
		t.Error("expected to see deletion marker for object C")
	}
}

func testStoragePresign(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	key := "test-presign-object"

	t.Run("GetObject", func(t *testing.T) {
		url, err := bucket.PresignGetObject(ctx, key, time.Hour)
		if err != nil {
			if !errors.Is(err, storage.ErrPresignNotSupported) && !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("expected ErrPresignNotSupported, ErrObjectNotFound, or nil, got: %v", err)
			}
		} else {
			if url == "" {
				t.Error("expected non-empty URL when no error is returned")
			}
		}
	})

	t.Run("PutObject", func(t *testing.T) {
		url, err := bucket.PresignPutObject(ctx, key, time.Hour)
		if err != nil {
			if !errors.Is(err, storage.ErrPresignNotSupported) && !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("expected ErrPresignNotSupported, ErrObjectNotFound, or nil, got: %v", err)
			}
		} else {
			if url == "" {
				t.Error("expected non-empty URL when no error is returned")
			}
		}
	})

	t.Run("HeadObject", func(t *testing.T) {
		url, err := bucket.PresignHeadObject(ctx, key, time.Hour)
		if err != nil {
			if !errors.Is(err, storage.ErrPresignNotSupported) && !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("expected ErrPresignNotSupported, ErrObjectNotFound, or nil, got: %v", err)
			}
		} else {
			if url == "" {
				t.Error("expected non-empty URL when no error is returned")
			}
		}
	})

	t.Run("DeleteObject", func(t *testing.T) {
		url, err := bucket.PresignDeleteObject(ctx, key, time.Hour)
		if err != nil {
			if !errors.Is(err, storage.ErrPresignNotSupported) && !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("expected ErrPresignNotSupported, ErrObjectNotFound, or nil, got: %v", err)
			}
		} else {
			if url == "" {
				t.Error("expected non-empty URL when no error is returned")
			}
		}
	})

	t.Run("invalid key", func(t *testing.T) {
		_, err := bucket.PresignGetObject(ctx, "", time.Hour)
		if !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected ErrInvalidObjectKey, got: %v", err)
		}
	})

	t.Run("presign with options", func(t *testing.T) {
		_, err := bucket.PresignGetObject(ctx, key, time.Hour, storage.BytesRange(0, 100))
		if err != nil && !errors.Is(err, storage.ErrPresignNotSupported) && !errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("PresignGetObject with options failed with unexpected error: %v", err)
		}

		_, err = bucket.PresignPutObject(ctx, key, time.Hour, storage.ContentType("text/plain"))
		if err != nil && !errors.Is(err, storage.ErrPresignNotSupported) && !errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("PresignPutObject with options failed with unexpected error: %v", err)
		}
	})
}

func testStorageCopyObject(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const srcKey = "copy-source.txt"
	const dstKey = "copy-dest.txt"
	const data = "hello, world!"

	// Create source object
	if _, err := bucket.PutObject(ctx, srcKey, strings.NewReader(data)); err != nil {
		t.Fatal("unexpected error writing source object:", err)
	}

	// Copy the object
	if err := bucket.CopyObject(ctx, srcKey, dstKey); err != nil {
		t.Fatal("unexpected error copying object:", err)
	}

	// Verify destination object exists with correct content
	r, info, err := bucket.GetObject(ctx, dstKey)
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}
	defer r.Close()

	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}
	if string(b) != data {
		t.Fatalf("unexpected object data: %q != %q", b, data)
	}
	if info.Size != int64(len(data)) {
		t.Fatalf("unexpected object size: %d != %d", info.Size, len(data))
	}

	// Verify source object still exists
	r, _, err = bucket.GetObject(ctx, srcKey)
	if err != nil {
		t.Fatal("source object should still exist after copy:", err)
	}
	r.Close()
}

func testStorageCopyObjectNotFound(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	err := bucket.CopyObject(ctx, "does-not-exist", "destination")
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("expected ErrObjectNotFound, got: %v", err)
	}
}

func testStorageCopyObjectInvalidSourcePath(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	for _, path := range InvalidPaths {
		if err := bucket.CopyObject(ctx, path, "valid-dest"); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected invalid path error for source %q: %v", path, err)
		}
	}
}

func testStorageCopyObjectInvalidDestPath(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()

	// First create a valid source object
	const srcKey = "valid-source.txt"
	if _, err := bucket.PutObject(ctx, srcKey, strings.NewReader("content")); err != nil {
		t.Fatal("unexpected error writing source object:", err)
	}

	for _, path := range InvalidPaths {
		if err := bucket.CopyObject(ctx, srcKey, path); !errors.Is(err, storage.ErrInvalidObjectKey) {
			t.Errorf("expected invalid path error for dest %q: %v", path, err)
		}
	}
}

func testStorageCopyObjectPreservesMetadata(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const srcKey = "copy-metadata-source.txt"
	const dstKey = "copy-metadata-dest.txt"
	const data = "hello, world!"
	const contentType = "text/plain"
	const cacheControl = "max-age=3600"
	metadata := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	// Create source object with metadata
	if _, err := bucket.PutObject(ctx, srcKey, strings.NewReader(data),
		storage.ContentType(contentType),
		storage.CacheControl(cacheControl),
		storage.Metadata("key1", "value1"),
		storage.Metadata("key2", "value2"),
	); err != nil {
		t.Fatal("unexpected error writing source object:", err)
	}

	// Copy without any options - should preserve metadata
	if err := bucket.CopyObject(ctx, srcKey, dstKey); err != nil {
		t.Fatal("unexpected error copying object:", err)
	}

	// Verify destination has same metadata
	info, err := bucket.HeadObject(ctx, dstKey)
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}

	if info.ContentType != contentType {
		t.Errorf("content type not preserved: %q != %q", info.ContentType, contentType)
	}
	if info.CacheControl != cacheControl {
		t.Errorf("cache control not preserved: %q != %q", info.CacheControl, cacheControl)
	}
	assertMetadataContains(t, info.Metadata, metadata, "CopyObject preserved metadata")
}

func testStorageCopyObjectOverridesMetadata(t *testing.T, bucket storage.Bucket) {
	ctx := t.Context()
	const srcKey = "copy-override-source.txt"
	const dstKey = "copy-override-dest.txt"
	const data = "hello, world!"
	const srcContentType = "text/plain"
	const dstContentType = "application/json"
	const cacheControl = "max-age=3600"

	// Create source object with metadata
	if _, err := bucket.PutObject(ctx, srcKey, strings.NewReader(data),
		storage.ContentType(srcContentType),
		storage.CacheControl(cacheControl),
		storage.Metadata("key1", "value1"),
	); err != nil {
		t.Fatal("unexpected error writing source object:", err)
	}

	// Copy with override options
	if err := bucket.CopyObject(ctx, srcKey, dstKey,
		storage.ContentType(dstContentType),
		storage.Metadata("key2", "value2"),
	); err != nil {
		t.Fatal("unexpected error copying object:", err)
	}

	// Verify destination has overridden metadata
	info, err := bucket.HeadObject(ctx, dstKey)
	if err != nil {
		t.Fatal("unexpected error reading destination object:", err)
	}

	// Content-Type should be overridden
	if info.ContentType != dstContentType {
		t.Errorf("content type not overridden: %q != %q", info.ContentType, dstContentType)
	}

	// Cache-Control should be preserved from source
	if info.CacheControl != cacheControl {
		t.Errorf("cache control should be preserved: %q != %q", info.CacheControl, cacheControl)
	}

	// Metadata should be merged (key1 from source, key2 from options)
	if info.Metadata["key1"] != "value1" {
		t.Errorf("source metadata key1 should be preserved: %v", info.Metadata)
	}
	if info.Metadata["key2"] != "value2" {
		t.Errorf("override metadata key2 should be present: %v", info.Metadata)
	}
}

func testStorageCopyObjectContextCanceled(t *testing.T, bucket storage.Bucket) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	// Create a source object first (with non-canceled context)
	const srcKey = "copy-cancel-source.txt"
	if _, err := bucket.PutObject(t.Context(), srcKey, strings.NewReader("content")); err != nil {
		t.Fatal("unexpected error writing source object:", err)
	}

	// CopyObject with canceled context should fail
	if err := bucket.CopyObject(ctx, srcKey, "copy-cancel-dest.txt"); !errors.Is(err, context.Canceled) {
		t.Errorf("expected context canceled error, got %v", err)
	}
}
