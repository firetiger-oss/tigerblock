package oras_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	storageoras "github.com/firetiger-oss/tigerblock/oras"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	oras "oras.land/oras-go/v2"
	"oras.land/oras-go/v2/errdef"
)

// pushArtifact pushes a manifest containing the given (title, body) pairs
// plus an optional untitled blob, and tags it as ref. Returns the manifest
// descriptor.
func pushArtifact(t *testing.T, target oras.Target, ref string, files map[string]string, untitled []byte) ocispec.Descriptor {
	t.Helper()
	ctx := t.Context()

	layers := make([]ocispec.Descriptor, 0, len(files)+1)

	keys := slices.Sorted(maps.Keys(files))
	for _, title := range keys {
		body := []byte(files[title])
		desc := ocispec.Descriptor{
			MediaType:   "application/octet-stream",
			Digest:      digest.FromBytes(body),
			Size:        int64(len(body)),
			Annotations: map[string]string{ocispec.AnnotationTitle: title},
		}
		if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
			t.Fatalf("push %q: %v", title, err)
		}
		layers = append(layers, desc)
	}

	if untitled != nil {
		desc := ocispec.Descriptor{
			MediaType: "application/octet-stream",
			Digest:    digest.FromBytes(untitled),
			Size:      int64(len(untitled)),
		}
		if err := target.Push(ctx, desc, bytes.NewReader(untitled)); err != nil {
			t.Fatalf("push untitled: %v", err)
		}
		layers = append(layers, desc)
	}

	configBody := []byte(`{}`)
	configDesc := ocispec.Descriptor{
		MediaType: "application/vnd.oci.empty.v1+json",
		Digest:    digest.FromBytes(configBody),
		Size:      int64(len(configBody)),
	}
	if err := target.Push(ctx, configDesc, bytes.NewReader(configBody)); err != nil {
		t.Fatalf("push config: %v", err)
	}

	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers:    layers,
	}
	manifest.SchemaVersion = 2
	manifestBody, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestBody),
		Size:      int64(len(manifestBody)),
	}
	if err := target.Push(ctx, manifestDesc, bytes.NewReader(manifestBody)); err != nil {
		t.Fatalf("push manifest: %v", err)
	}
	if err := target.Tag(ctx, manifestDesc, ref); err != nil {
		t.Fatalf("tag: %v", err)
	}
	return manifestDesc
}

func TestReadOnlyBucketRoundTrip(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	files := map[string]string{
		"alpha.txt": "alpha contents",
		"beta.json": `{"hello":"beta"}`,
	}
	pushArtifact(t, target, "v1", files, nil)

	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	for title, want := range files {
		t.Run("get/"+title, func(t *testing.T) {
			rc, info, err := bucket.GetObject(ctx, title)
			if err != nil {
				t.Fatalf("GetObject: %v", err)
			}
			got, err := io.ReadAll(rc)
			rc.Close()
			if err != nil {
				t.Fatalf("ReadAll: %v", err)
			}
			if string(got) != want {
				t.Fatalf("body = %q; want %q", got, want)
			}
			if info.Size != int64(len(want)) {
				t.Fatalf("Size = %d; want %d", info.Size, len(want))
			}
			if info.ContentType != "application/octet-stream" {
				t.Fatalf("ContentType = %q; want application/octet-stream", info.ContentType)
			}
			if info.ETag == "" {
				t.Fatal("ETag empty")
			}
		})
	}
}

func TestReadOnlyBucketHeadObjectMetadata(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	body := []byte("annotated")
	desc := ocispec.Descriptor{
		MediaType: "text/plain",
		Digest:    digest.FromBytes(body),
		Size:      int64(len(body)),
		Annotations: map[string]string{
			ocispec.AnnotationTitle:       "doc.txt",
			"org.example.custom":          "yes",
			"org.opencontainers.image.os": "linux",
		},
	}
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	configBody := []byte(`{}`)
	configDesc := ocispec.Descriptor{
		MediaType: "application/vnd.oci.empty.v1+json",
		Digest:    digest.FromBytes(configBody),
		Size:      int64(len(configBody)),
	}
	if err := target.Push(ctx, configDesc, bytes.NewReader(configBody)); err != nil {
		t.Fatalf("push config: %v", err)
	}

	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers:    []ocispec.Descriptor{desc},
	}
	manifest.SchemaVersion = 2
	manifestBody, _ := json.Marshal(manifest)
	manifestDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest,
		Digest:    digest.FromBytes(manifestBody),
		Size:      int64(len(manifestBody)),
	}
	if err := target.Push(ctx, manifestDesc, bytes.NewReader(manifestBody)); err != nil {
		t.Fatalf("push manifest: %v", err)
	}
	if err := target.Tag(ctx, manifestDesc, "v1"); err != nil {
		t.Fatalf("tag: %v", err)
	}

	bucket := storageoras.NewReadOnlyBucket(target, "v1")
	info, err := bucket.HeadObject(ctx, "doc.txt")
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if info.ContentType != "text/plain" {
		t.Errorf("ContentType = %q; want text/plain", info.ContentType)
	}
	if info.Size != int64(len(body)) {
		t.Errorf("Size = %d; want %d", info.Size, len(body))
	}
	if info.ETag != desc.Digest.String() {
		t.Errorf("ETag = %q; want %q", info.ETag, desc.Digest.String())
	}
	if info.Metadata["org.example.custom"] != "yes" || info.Metadata["org.opencontainers.image.os"] != "linux" {
		t.Errorf("annotations not surfaced as metadata: %v", info.Metadata)
	}
}

func TestReadOnlyBucketUntitledLayersHidden(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	pushArtifact(t, target, "v1", map[string]string{"named.txt": "named"}, []byte("anonymous"))

	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	if _, err := bucket.HeadObject(ctx, "anonymous"); !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("HeadObject(untitled) = %v; want ErrObjectNotFound", err)
	}

	var keys []string
	for obj, err := range bucket.ListObjects(ctx) {
		if err != nil {
			t.Fatalf("ListObjects: %v", err)
		}
		keys = append(keys, obj.Key)
	}
	if !slices.Equal(keys, []string{"named.txt"}) {
		t.Fatalf("keys = %v; want [named.txt]", keys)
	}
}

func TestReadOnlyBucketListPaginationAndDelimiter(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	files := map[string]string{
		"a/x.txt": "ax",
		"a/y.txt": "ay",
		"b/x.txt": "bx",
		"top.txt": "top",
	}
	pushArtifact(t, target, "v1", files, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	t.Run("sorted", func(t *testing.T) {
		var got []string
		for obj, err := range bucket.ListObjects(ctx) {
			if err != nil {
				t.Fatalf("ListObjects: %v", err)
			}
			got = append(got, obj.Key)
		}
		want := []string{"a/x.txt", "a/y.txt", "b/x.txt", "top.txt"}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("KeyPrefix", func(t *testing.T) {
		var got []string
		for obj, err := range bucket.ListObjects(ctx, storage.KeyPrefix("a/")) {
			if err != nil {
				t.Fatalf("ListObjects: %v", err)
			}
			got = append(got, obj.Key)
		}
		want := []string{"a/x.txt", "a/y.txt"}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("StartAfter", func(t *testing.T) {
		var got []string
		for obj, err := range bucket.ListObjects(ctx, storage.StartAfter("a/y.txt")) {
			if err != nil {
				t.Fatalf("ListObjects: %v", err)
			}
			got = append(got, obj.Key)
		}
		want := []string{"b/x.txt", "top.txt"}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("MaxKeys", func(t *testing.T) {
		var got []string
		for obj, err := range bucket.ListObjects(ctx, storage.MaxKeys(2)) {
			if err != nil {
				t.Fatalf("ListObjects: %v", err)
			}
			got = append(got, obj.Key)
		}
		want := []string{"a/x.txt", "a/y.txt"}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})

	t.Run("KeyDelimiter", func(t *testing.T) {
		var got []string
		for obj, err := range bucket.ListObjects(ctx, storage.KeyDelimiter("/")) {
			if err != nil {
				t.Fatalf("ListObjects: %v", err)
			}
			got = append(got, obj.Key)
		}
		want := []string{"a/", "b/", "top.txt"}
		if !slices.Equal(got, want) {
			t.Fatalf("got %v; want %v", got, want)
		}
	})
}

func TestReadOnlyBucketRejectsWrites(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	pushArtifact(t, target, "v1", map[string]string{"x": "x"}, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	if err := bucket.Create(ctx); !errors.Is(err, storage.ErrBucketReadOnly) {
		t.Errorf("Create = %v; want ErrBucketReadOnly", err)
	}
	if _, err := bucket.PutObject(ctx, "y", bytes.NewReader([]byte("y"))); !errors.Is(err, storage.ErrBucketReadOnly) {
		t.Errorf("PutObject = %v; want ErrBucketReadOnly", err)
	}
	if err := bucket.DeleteObject(ctx, "x"); !errors.Is(err, storage.ErrBucketReadOnly) {
		t.Errorf("DeleteObject = %v; want ErrBucketReadOnly", err)
	}
	if err := bucket.CopyObject(ctx, "x", "z"); !errors.Is(err, storage.ErrBucketReadOnly) {
		t.Errorf("CopyObject = %v; want ErrBucketReadOnly", err)
	}

	keys := func(yield func(string, error) bool) {
		yield("x", nil)
		yield("y", nil)
	}
	for _, err := range bucket.DeleteObjects(ctx, keys) {
		if !errors.Is(err, storage.ErrBucketReadOnly) {
			t.Errorf("DeleteObjects element = %v; want ErrBucketReadOnly", err)
		}
	}
}

func TestReadOnlyBucketPresignNotSupported(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	pushArtifact(t, target, "v1", map[string]string{"x": "x"}, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	if _, err := bucket.PresignGetObject(ctx, "x", time.Minute); !errors.Is(err, storage.ErrPresignNotSupported) {
		t.Errorf("PresignGetObject = %v; want ErrPresignNotSupported", err)
	}
	if _, err := bucket.PresignPutObject(ctx, "x", time.Minute); !errors.Is(err, storage.ErrPresignNotSupported) {
		t.Errorf("PresignPutObject = %v; want ErrPresignNotSupported", err)
	}
	if _, err := bucket.PresignHeadObject(ctx, "x", time.Minute); !errors.Is(err, storage.ErrPresignNotSupported) {
		t.Errorf("PresignHeadObject = %v; want ErrPresignNotSupported", err)
	}
	if _, err := bucket.PresignDeleteObject(ctx, "x", time.Minute); !errors.Is(err, storage.ErrPresignNotSupported) {
		t.Errorf("PresignDeleteObject = %v; want ErrPresignNotSupported", err)
	}
}

func TestReadOnlyBucketWatchObjectsExitsOnContextCancel(t *testing.T) {
	target := storageoras.New(memory.NewBucket())
	pushArtifact(t, target, "v1", map[string]string{"x": "x"}, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	var seen []string
	go func() {
		defer close(done)
		for obj, err := range bucket.WatchObjects(ctx) {
			if err != nil {
				return
			}
			seen = append(seen, obj.Key)
		}
	}()

	// Give the goroutine a chance to emit the initial listing then
	// block on ctx.Done.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WatchObjects did not exit after context cancel")
	}
	if !slices.Equal(seen, []string{"x"}) {
		t.Fatalf("seen = %v; want [x]", seen)
	}
}

// Regression: only the standard image manifest media type is
// supported. Other shapes (image index, OCI artifact manifest with
// `blobs` instead of `layers`, Docker manifest list) cannot be decoded
// as ocispec.Manifest and would silently look like an empty bucket if
// we let them through. They must surface errdef.ErrUnsupported.
func TestReadOnlyBucketRejectsNonImageManifestMediaTypes(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	for _, mediaType := range []string{
		ocispec.MediaTypeImageIndex,
		"application/vnd.oci.artifact.manifest.v1+json",
		"application/vnd.docker.distribution.manifest.list.v2+json",
		"application/vnd.docker.distribution.manifest.v2+json",
	} {
		t.Run(mediaType, func(t *testing.T) {
			body := []byte(`{}`)
			desc := ocispec.Descriptor{
				MediaType: mediaType,
				Digest:    digest.FromBytes(body),
				Size:      int64(len(body)),
			}
			if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
				t.Fatalf("push: %v", err)
			}
			tag := "tag-" + digest.FromBytes([]byte(mediaType)).Encoded()[:8]
			if err := target.Tag(ctx, desc, tag); err != nil {
				t.Fatalf("tag: %v", err)
			}

			bucket := storageoras.NewReadOnlyBucket(target, tag)
			if err := bucket.Access(ctx); !errors.Is(err, errdef.ErrUnsupported) {
				t.Fatalf("Access on %s = %v; want errdef.ErrUnsupported", mediaType, err)
			}
			// And lazy I/O paths must agree.
			if _, err := bucket.HeadObject(ctx, "anything"); !errors.Is(err, errdef.ErrUnsupported) {
				t.Fatalf("HeadObject on %s = %v; want errdef.ErrUnsupported", mediaType, err)
			}
		})
	}
}

func TestReadOnlyBucketUnresolvedReferenceTranslatesError(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	bucket := storageoras.NewReadOnlyBucket(target, "ghost")
	err := bucket.Access(ctx)
	if !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("Access(ghost): errors.Is(err, errdef.ErrNotFound) = false; err = %v", err)
	}
	if !errors.Is(err, storage.ErrObjectNotFound) {
		t.Fatalf("Access(ghost): errors.Is(err, storage.ErrObjectNotFound) = false; err = %v", err)
	}
}

func TestReadOnlyBucketGetObjectBytesRange(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	files := map[string]string{"file": "0123456789"}
	pushArtifact(t, target, "v1", files, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	rc, _, err := bucket.GetObject(ctx, "file", storage.BytesRange(2, 5))
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "2345" {
		t.Fatalf("got %q; want 2345", got)
	}
}

// Regression: callers like storage.File.ReadAt and the FUSE wrapper
// build BytesRange(off, off+len(buf)-1) without clamping to the blob
// size, so the bucket must clamp end to size-1 instead of returning
// ErrInvalidRange. Matches memory backend behaviour.
func TestReadOnlyBucketGetObjectBytesRangeClampsPastEOF(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	files := map[string]string{"file": "0123456789"} // 10 bytes
	pushArtifact(t, target, "v1", files, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	// Range extends past EOF: should return last 2 bytes ("89"), not error.
	rc, _, err := bucket.GetObject(ctx, "file", storage.BytesRange(8, 99))
	if err != nil {
		t.Fatalf("GetObject: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "89" {
		t.Fatalf("got %q; want 89", got)
	}
}

// Regression: when KeyDelimiter is set, StartAfter must be compared
// against the emitted key (e.g. "a/"), not the underlying layer title
// ("a/x.txt"). Otherwise paginated directory listings re-emit the
// previous page's prefix and a naive caller feeding the last key back
// loops forever.
func TestReadOnlyBucketListStartAfterRespectsCollapsedPrefixes(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	files := map[string]string{
		"a/x.txt": "ax",
		"a/y.txt": "ay",
		"b/x.txt": "bx",
	}
	pushArtifact(t, target, "v1", files, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	// First page: list with delimiter only. Expect ["a/", "b/"].
	var page1 []string
	for obj, err := range bucket.ListObjects(ctx, storage.KeyDelimiter("/")) {
		if err != nil {
			t.Fatalf("ListObjects page 1: %v", err)
		}
		page1 = append(page1, obj.Key)
	}
	if !slices.Equal(page1, []string{"a/", "b/"}) {
		t.Fatalf("page 1 = %v; want [a/ b/]", page1)
	}

	// Second page: feed the last key back. Must NOT re-emit "a/".
	var page2 []string
	for obj, err := range bucket.ListObjects(ctx, storage.KeyDelimiter("/"), storage.StartAfter("a/")) {
		if err != nil {
			t.Fatalf("ListObjects page 2: %v", err)
		}
		page2 = append(page2, obj.Key)
	}
	if !slices.Equal(page2, []string{"b/"}) {
		t.Fatalf("page 2 = %v; want [b/] (must not repeat a/)", page2)
	}
}

// Regression: every bucket method must honour a cancelled context
// before short-circuiting with ErrBucketReadOnly or
// ErrPresignNotSupported, otherwise upper layers retry/log the wrong
// failure mode for aborted requests. Matches the cancellation
// contract enforced by other backends in this repo.
func TestReadOnlyBucketHonoursCanceledContext(t *testing.T) {
	target := storageoras.New(memory.NewBucket())
	pushArtifact(t, target, "v1", map[string]string{"x": "x"}, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	canceledErr := errors.New("test-cancel")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(canceledErr)

	if err := bucket.Create(ctx); !errors.Is(err, canceledErr) {
		t.Errorf("Create = %v; want canceledErr", err)
	}
	if _, err := bucket.PutObject(ctx, "x", bytes.NewReader(nil)); !errors.Is(err, canceledErr) {
		t.Errorf("PutObject = %v; want canceledErr", err)
	}
	if err := bucket.DeleteObject(ctx, "x"); !errors.Is(err, canceledErr) {
		t.Errorf("DeleteObject = %v; want canceledErr", err)
	}
	if err := bucket.CopyObject(ctx, "x", "y"); !errors.Is(err, canceledErr) {
		t.Errorf("CopyObject = %v; want canceledErr", err)
	}

	for _, err := range bucket.DeleteObjects(ctx, func(yield func(string, error) bool) { yield("x", nil) }) {
		if !errors.Is(err, canceledErr) {
			t.Errorf("DeleteObjects element = %v; want canceledErr", err)
		}
	}

	if _, err := bucket.PresignGetObject(ctx, "x", time.Minute); !errors.Is(err, canceledErr) {
		t.Errorf("PresignGetObject = %v; want canceledErr", err)
	}
	if _, err := bucket.PresignPutObject(ctx, "x", time.Minute); !errors.Is(err, canceledErr) {
		t.Errorf("PresignPutObject = %v; want canceledErr", err)
	}
	if _, err := bucket.PresignHeadObject(ctx, "x", time.Minute); !errors.Is(err, canceledErr) {
		t.Errorf("PresignHeadObject = %v; want canceledErr", err)
	}
	if _, err := bucket.PresignDeleteObject(ctx, "x", time.Minute); !errors.Is(err, canceledErr) {
		t.Errorf("PresignDeleteObject = %v; want canceledErr", err)
	}
}

// Regression: HeadObject and GetObject must validate the key before
// touching the manifest, matching the contract in
// testStorageGetObjectInvalidPath / testStorageHeadObjectInvalidPath.
func TestReadOnlyBucketReadInvalidKeyReturnsErrInvalidObjectKey(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	pushArtifact(t, target, "v1", map[string]string{"x": "x"}, nil)
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	for _, bad := range []string{"a/../b", "//", "a/.."} {
		t.Run(bad, func(t *testing.T) {
			if _, err := bucket.HeadObject(ctx, bad); !errors.Is(err, storage.ErrInvalidObjectKey) {
				t.Errorf("HeadObject(%q) = %v; want ErrInvalidObjectKey", bad, err)
			}
			if _, _, err := bucket.GetObject(ctx, bad); !errors.Is(err, storage.ErrInvalidObjectKey) {
				t.Errorf("GetObject(%q) = %v; want ErrInvalidObjectKey", bad, err)
			}
		})
	}
}

func TestReadOnlyBucketLazyConstruction(t *testing.T) {
	// NewReadOnlyBucket must not touch the target. Use a counter wrapper
	// to assert zero calls until a method that needs the index runs.
	target := &countingTarget{inner: storageoras.New(memory.NewBucket())}
	bucket := storageoras.NewReadOnlyBucket(target, "any")

	_ = bucket.Location()
	if _, err := bucket.PutObject(t.Context(), "x", bytes.NewReader(nil)); !errors.Is(err, storage.ErrBucketReadOnly) {
		t.Fatalf("PutObject = %v; want ErrBucketReadOnly", err)
	}
	if _, err := bucket.PresignGetObject(t.Context(), "x", time.Minute); !errors.Is(err, storage.ErrPresignNotSupported) {
		t.Fatalf("PresignGetObject = %v; want ErrPresignNotSupported", err)
	}
	if got := target.calls.Load(); got != 0 {
		t.Fatalf("target was called %d times; want 0", got)
	}
}

func TestReadOnlyBucketLazyRetriesOnTransientFailure(t *testing.T) {
	ctx := t.Context()
	memTarget := storageoras.New(memory.NewBucket())
	pushArtifact(t, memTarget, "v1", map[string]string{"x": "xv"}, nil)

	target := &flakyResolveTarget{inner: memTarget, failTimes: 1, want: "v1"}
	bucket := storageoras.NewReadOnlyBucket(target, "v1")

	if err := bucket.Access(ctx); err == nil {
		t.Fatal("Access: expected injected resolve failure")
	}
	if err := bucket.Access(ctx); err != nil {
		t.Fatalf("retry Access: %v", err)
	}
	if got := target.resolveAttempts.Load(); got != 2 {
		t.Fatalf("resolve attempts = %d; want 2", got)
	}
}

// countingTarget records every call to its three methods.
type countingTarget struct {
	inner oras.ReadOnlyTarget
	calls atomic.Int64
}

func (c *countingTarget) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	c.calls.Add(1)
	return c.inner.Fetch(ctx, target)
}

func (c *countingTarget) Exists(ctx context.Context, target ocispec.Descriptor) (bool, error) {
	c.calls.Add(1)
	return c.inner.Exists(ctx, target)
}

func (c *countingTarget) Resolve(ctx context.Context, ref string) (ocispec.Descriptor, error) {
	c.calls.Add(1)
	return c.inner.Resolve(ctx, ref)
}

// flakyResolveTarget makes the first failTimes Resolve calls fail with a
// generic error, then forwards subsequent calls to inner.
type flakyResolveTarget struct {
	inner           oras.ReadOnlyTarget
	failTimes       int64
	want            string
	resolveAttempts atomic.Int64
}

func (f *flakyResolveTarget) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	return f.inner.Fetch(ctx, target)
}

func (f *flakyResolveTarget) Exists(ctx context.Context, target ocispec.Descriptor) (bool, error) {
	return f.inner.Exists(ctx, target)
}

func (f *flakyResolveTarget) Resolve(ctx context.Context, ref string) (ocispec.Descriptor, error) {
	attempt := f.resolveAttempts.Add(1)
	if attempt <= f.failTimes && ref == f.want {
		return ocispec.Descriptor{}, errors.New("injected resolve failure")
	}
	return f.inner.Resolve(ctx, ref)
}
