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

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/memory"
	storageoras "github.com/firetiger-oss/storage/oras"
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

func TestReadOnlyBucketImageIndexRejected(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	indexBody := []byte(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[]}`)
	indexDesc := ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageIndex,
		Digest:    digest.FromBytes(indexBody),
		Size:      int64(len(indexBody)),
	}
	if err := target.Push(ctx, indexDesc, bytes.NewReader(indexBody)); err != nil {
		t.Fatalf("push index: %v", err)
	}
	if err := target.Tag(ctx, indexDesc, "multi"); err != nil {
		t.Fatalf("tag: %v", err)
	}

	bucket := storageoras.NewReadOnlyBucket(target, "multi")
	err := bucket.Access(ctx)
	if !errors.Is(err, errdef.ErrUnsupported) {
		t.Fatalf("Access on image index = %v; want errdef.ErrUnsupported", err)
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
