package oras_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	storageoras "github.com/firetiger-oss/tigerblock/oras"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	oras "oras.land/oras-go/v2"
	orascontent "oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry"
)

func descriptorFor(data []byte, mediaType string) ocispec.Descriptor {
	return ocispec.Descriptor{
		MediaType: mediaType,
		Digest:    digest.FromBytes(data),
		Size:      int64(len(data)),
	}
}

func TestPushFetchRoundTrip(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	body := []byte("hello blob")
	desc := descriptorFor(body, "application/octet-stream")

	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	rc, err := target.Fetch(ctx, desc)
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("body mismatch: got %q want %q", got, body)
	}
}

// Blobs are content-addressable and therefore immutable; the bridge
// must mark them as such so CDNs and HTTP caches treat them as
// permanent. Tag refs are deliberately left without a cache directive
// because they're mutable.
func TestPushSetsImmutableCacheControlOnBlobs(t *testing.T) {
	ctx := t.Context()
	bucket := memory.NewBucket()
	target := storageoras.New(bucket)

	body := []byte("immutable")
	desc := descriptorFor(body, "application/octet-stream")
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	blobKey := "blobs/" + desc.Digest.Algorithm().String() + "/" + desc.Digest.Encoded()
	info, err := bucket.HeadObject(ctx, blobKey)
	if err != nil {
		t.Fatalf("HeadObject(blob): %v", err)
	}
	if info.CacheControl != storage.CacheControlImmutable {
		t.Fatalf("blob CacheControl = %q; want %q", info.CacheControl, storage.CacheControlImmutable)
	}

	if err := target.Tag(ctx, desc, "v1"); err != nil {
		t.Fatalf("Tag: %v", err)
	}
	refInfo, err := bucket.HeadObject(ctx, "refs/v1")
	if err != nil {
		t.Fatalf("HeadObject(ref): %v", err)
	}
	if refInfo.CacheControl != "" {
		t.Fatalf("ref CacheControl = %q; want empty (refs are mutable)", refInfo.CacheControl)
	}
}

func TestExistsReportsPresence(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	body := []byte("present")
	desc := descriptorFor(body, "application/octet-stream")

	ok, err := target.Exists(ctx, desc)
	if err != nil || ok {
		t.Fatalf("Exists before push = (%v, %v); want (false, nil)", ok, err)
	}

	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	ok, err = target.Exists(ctx, desc)
	if err != nil || !ok {
		t.Fatalf("Exists after push = (%v, %v); want (true, nil)", ok, err)
	}
}

func TestFetchMissingReturnsNotFound(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	body := []byte("missing")
	desc := descriptorFor(body, "application/octet-stream")

	if _, err := target.Fetch(ctx, desc); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("Fetch missing = %v; want errdef.ErrNotFound", err)
	}
}

func TestPushDigestMismatchCleansUp(t *testing.T) {
	ctx := t.Context()
	bucket := memory.NewBucket()
	target := storageoras.New(bucket)

	body := []byte("real bytes")
	desc := descriptorFor([]byte("other bytes"), "application/octet-stream")
	desc.Size = int64(len(body))

	err := target.Push(ctx, desc, bytes.NewReader(body))
	if !errors.Is(err, errdef.ErrInvalidDigest) {
		t.Fatalf("Push with bad digest = %v; want errdef.ErrInvalidDigest", err)
	}

	ok, err := target.Exists(ctx, desc)
	if err != nil {
		t.Fatalf("Exists after failed push: %v", err)
	}
	if ok {
		t.Fatalf("blob should have been cleaned up after digest mismatch")
	}
}

// Regression for codex review P1: a Push with mismatched bytes must never
// publish at the final blob key — even briefly. Previously Push wrote
// directly to blobs/<algo>/<digest> with IfNoneMatch and only ran
// verification afterwards, so concurrent readers/pushers could observe
// or "succeed against" unverified data.
func TestPushDigestMismatchNeverPublishes(t *testing.T) {
	ctx := t.Context()
	bucket := &recordingBucket{Bucket: memory.NewBucket()}
	target := storageoras.New(bucket)

	body := []byte("real bytes")
	desc := descriptorFor([]byte("other bytes"), "application/octet-stream")
	desc.Size = int64(len(body))

	if err := target.Push(ctx, desc, bytes.NewReader(body)); !errors.Is(err, errdef.ErrInvalidDigest) {
		t.Fatalf("Push = %v; want errdef.ErrInvalidDigest", err)
	}

	finalKey := "blobs/" + desc.Digest.Algorithm().String() + "/" + desc.Digest.Encoded()
	if bucket.seenAt(finalKey) {
		t.Fatalf("final blob key %q must never receive bytes from a failed verification", finalKey)
	}
}

func TestPushSizeMismatchNeverPublishes(t *testing.T) {
	ctx := t.Context()
	bucket := &recordingBucket{Bucket: memory.NewBucket()}
	target := storageoras.New(bucket)

	body := []byte("twelve bytes")
	desc := descriptorFor(body, "application/octet-stream")
	desc.Size = 999

	if err := target.Push(ctx, desc, bytes.NewReader(body)); err == nil {
		t.Fatal("Push: expected error from size mismatch")
	}

	finalKey := "blobs/" + desc.Digest.Algorithm().String() + "/" + desc.Digest.Encoded()
	if bucket.seenAt(finalKey) {
		t.Fatalf("final blob key %q must never receive bytes from a failed verification", finalKey)
	}
}

func TestPushSizeMismatchCleansUp(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	body := []byte("twelve bytes")
	desc := descriptorFor(body, "application/octet-stream")
	desc.Size = 999 // lie about size

	if err := target.Push(ctx, desc, bytes.NewReader(body)); err == nil {
		t.Fatal("Push with bad size: expected error")
	}

	ok, err := target.Exists(ctx, desc)
	if err != nil || ok {
		t.Fatalf("blob should have been cleaned up after size mismatch (exists=%v err=%v)", ok, err)
	}
}

func TestPushDuplicateIsNoOp(t *testing.T) {
	ctx := t.Context()
	bucket := &countingBucket{Bucket: memory.NewBucket()}
	target := storageoras.New(bucket)

	body := []byte("dup")
	desc := descriptorFor(body, "application/octet-stream")

	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("first Push: %v", err)
	}
	puts := bucket.puts.Load()

	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("second Push: %v", err)
	}
	// The blob is already present — Push must short-circuit without any
	// further uploads or staging copies.
	if got := bucket.puts.Load(); got != puts {
		t.Fatalf("duplicate Push triggered %d extra puts; want 0", got-puts)
	}
}

// A duplicate Push must drain the reader so the caller never sees a
// half-consumed stream when the blob is already present.
func TestPushDuplicateDrainsReader(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	body := []byte("drain me on dup")
	desc := descriptorFor(body, "application/octet-stream")
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("first Push: %v", err)
	}

	r := bytes.NewReader(body)
	if err := target.Push(ctx, desc, r); err != nil {
		t.Fatalf("second Push: %v", err)
	}
	if r.Len() != 0 {
		t.Fatalf("duplicate Push left %d bytes unread", r.Len())
	}
}

func TestTagAndResolve(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	body := []byte("tag me")
	desc := descriptorFor(body, ocispec.MediaTypeImageManifest)
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	if err := target.Tag(ctx, desc, "v1"); err != nil {
		t.Fatalf("Tag: %v", err)
	}

	got, err := target.Resolve(ctx, "v1")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got.Digest != desc.Digest || got.Size != desc.Size || got.MediaType != desc.MediaType {
		t.Fatalf("Resolve mismatch: got %+v want %+v", got, desc)
	}
}

func TestTagRejectsUnknownBlob(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	desc := descriptorFor([]byte("not pushed"), ocispec.MediaTypeImageManifest)

	err := target.Tag(ctx, desc, "ghost")
	if !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("Tag of missing blob = %v; want errdef.ErrNotFound", err)
	}
}

// Regression for codex review P2: oras targets are expected to resolve
// digest references too, not only named tags. oras.Copy and oras.Fetch
// hand the target a digest string when the caller only has a digest.
// Push must reject non-SHA256 digests as unsupported. SHA-384 and SHA-512
// are valid OCI algorithms but are not used in practice and we intentionally
// don't carry a fallback verification path for them.
// Regression: Push's preflight (ensureLayout, HeadObject for dedup)
// used to leak raw storage sentinels. Callers that errors.Is against
// errdef sentinels would silently miss those failure modes even though
// the post-preflight upload path mapped them correctly.
func TestPushPreflightErrorsTranslated(t *testing.T) {
	body := []byte("preflight")
	desc := descriptorFor(body, "application/octet-stream")

	t.Run("ensureLayout failure → errdef.ErrUnsupported", func(t *testing.T) {
		// Read-only on every PutObject — ensureLayout is the first
		// PutObject in Push.
		bucket := &readOnlyOnPutBucket{Bucket: memory.NewBucket()}
		target := storageoras.New(bucket)

		err := target.Push(t.Context(), desc, bytes.NewReader(body))
		if !errors.Is(err, errdef.ErrUnsupported) {
			t.Fatalf("Push = %v; want errors.Is(err, errdef.ErrUnsupported)", err)
		}
	})

	t.Run("HeadObject failure → errdef.ErrNotFound", func(t *testing.T) {
		// Non-NotFound HeadObject error (here: ErrBucketNotFound)
		// — Push's dedup check was returning the raw storage error.
		bucket := &headFailingBucket{Bucket: memory.NewBucket(), err: storage.ErrBucketNotFound}
		target := storageoras.New(bucket)

		err := target.Push(t.Context(), desc, bytes.NewReader(body))
		if !errors.Is(err, errdef.ErrNotFound) {
			t.Fatalf("Push = %v; want errors.Is(err, errdef.ErrNotFound)", err)
		}
	})
}

func TestPushNonSHA256Unsupported(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	body := []byte("non-sha256 body")
	desc := ocispec.Descriptor{
		MediaType: "application/octet-stream",
		Digest:    digest.SHA512.FromBytes(body),
		Size:      int64(len(body)),
	}
	if err := target.Push(ctx, desc, bytes.NewReader(body)); !errors.Is(err, errdef.ErrUnsupported) {
		t.Fatalf("Push with sha512 = %v; want errdef.ErrUnsupported", err)
	}
}

func TestResolveByDigest(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	body := []byte("digest-resolvable")
	desc := descriptorFor(body, ocispec.MediaTypeImageLayer)
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	got, err := target.Resolve(ctx, desc.Digest.String())
	if err != nil {
		t.Fatalf("Resolve(digest): %v", err)
	}
	if got.Digest != desc.Digest {
		t.Fatalf("Resolve(digest).Digest = %s; want %s", got.Digest, desc.Digest)
	}
	if got.Size != desc.Size {
		t.Fatalf("Resolve(digest).Size = %d; want %d", got.Size, desc.Size)
	}
	// Regression: digest-addressed resolves used to drop MediaType,
	// which broke oras.Copy on manifest digests (oras-go dispatches on
	// MediaType to walk a manifest's config and layers).
	if got.MediaType != desc.MediaType {
		t.Fatalf("Resolve(digest).MediaType = %q; want %q", got.MediaType, desc.MediaType)
	}
}

// Regression: oras.Copy from a digest reference must work end-to-end.
// Prior to the fix, resolveDigest returned an empty MediaType, so
// oras-go treated the manifest as an opaque blob and skipped the
// config/layer copies, leaving the destination incomplete.
func TestCopyByDigestPreservesGraph(t *testing.T) {
	ctx := t.Context()
	src := storageoras.New(memory.NewBucket())
	dst := storageoras.New(memory.NewBucket())

	layer := []byte("layer payload for digest copy")
	layerDesc := descriptorFor(layer, ocispec.MediaTypeImageLayer)
	if err := src.Push(ctx, layerDesc, bytes.NewReader(layer)); err != nil {
		t.Fatalf("push layer: %v", err)
	}

	configBody := []byte(`{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}`)
	configDesc := descriptorFor(configBody, ocispec.MediaTypeImageConfig)
	if err := src.Push(ctx, configDesc, bytes.NewReader(configBody)); err != nil {
		t.Fatalf("push config: %v", err)
	}

	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers:    []ocispec.Descriptor{layerDesc},
	}
	manifest.SchemaVersion = 2
	manifestBody := mustMarshalJSON(t, manifest)
	manifestDesc := descriptorFor(manifestBody, ocispec.MediaTypeImageManifest)
	if err := src.Push(ctx, manifestDesc, bytes.NewReader(manifestBody)); err != nil {
		t.Fatalf("push manifest: %v", err)
	}

	// Copy by digest reference, NOT by tag — this is the path that
	// breaks if resolveDigest drops MediaType.
	digestRef := manifestDesc.Digest.String()
	if _, err := oras.Copy(ctx, src, digestRef, dst, digestRef, oras.DefaultCopyOptions); err != nil {
		t.Fatalf("oras.Copy by digest: %v", err)
	}

	// Both the config and the layer must have made the trip.
	if ok, err := dst.Exists(ctx, configDesc); err != nil || !ok {
		t.Fatalf("config not copied: exists=%v err=%v", ok, err)
	}
	if ok, err := dst.Exists(ctx, layerDesc); err != nil || !ok {
		t.Fatalf("layer not copied: exists=%v err=%v", ok, err)
	}
}

func TestResolveMissingDigestReturnsNotFound(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	missing := digest.FromBytes([]byte("never pushed"))

	if _, err := target.Resolve(ctx, missing.String()); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("Resolve(missing digest) = %v; want errdef.ErrNotFound", err)
	}
}

func TestResolveMissingReturnsNotFound(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	if _, err := target.Resolve(ctx, "nope"); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("Resolve missing = %v; want errdef.ErrNotFound", err)
	}
}

func TestResolveEmptyReferenceErrors(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())

	if _, err := target.Resolve(ctx, ""); !errors.Is(err, errdef.ErrMissingReference) {
		t.Fatalf("Resolve empty = %v; want errdef.ErrMissingReference", err)
	}
}

func TestUntagRemovesReference(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	body := []byte("u")
	desc := descriptorFor(body, ocispec.MediaTypeImageManifest)
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := target.Tag(ctx, desc, "to-remove"); err != nil {
		t.Fatalf("Tag: %v", err)
	}

	untagger := target.(orascontent.Untagger)
	if err := untagger.Untag(ctx, "to-remove"); err != nil {
		t.Fatalf("Untag: %v", err)
	}

	if _, err := target.Resolve(ctx, "to-remove"); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("Resolve after Untag = %v; want errdef.ErrNotFound", err)
	}

	// Idempotent: untagging again is a no-op.
	if err := untagger.Untag(ctx, "to-remove"); err != nil {
		t.Fatalf("second Untag: %v", err)
	}
}

func TestDeleteRemovesBlob(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	body := []byte("delete me")
	desc := descriptorFor(body, "application/octet-stream")
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	deleter := target.(orascontent.Deleter)
	if err := deleter.Delete(ctx, desc); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := target.Fetch(ctx, desc); !errors.Is(err, errdef.ErrNotFound) {
		t.Fatalf("Fetch after Delete = %v; want errdef.ErrNotFound", err)
	}
}

func TestTagsListsAndPaginates(t *testing.T) {
	ctx := t.Context()
	target := storageoras.New(memory.NewBucket())
	body := []byte("for tags")
	desc := descriptorFor(body, ocispec.MediaTypeImageManifest)
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}

	want := []string{"alpha", "beta", "gamma", "omega"}
	for _, ref := range want {
		if err := target.Tag(ctx, desc, ref); err != nil {
			t.Fatalf("Tag %s: %v", ref, err)
		}
	}

	lister := target.(registry.TagLister)

	var got []string
	if err := lister.Tags(ctx, "", func(tags []string) error {
		got = append(got, tags...)
		return nil
	}); err != nil {
		t.Fatalf("Tags: %v", err)
	}
	if !slicesEqual(got, want) {
		t.Fatalf("Tags = %v; want %v", got, want)
	}

	got = got[:0]
	if err := lister.Tags(ctx, "beta", func(tags []string) error {
		got = append(got, tags...)
		return nil
	}); err != nil {
		t.Fatalf("Tags(beta): %v", err)
	}
	if !slicesEqual(got, []string{"gamma", "omega"}) {
		t.Fatalf("Tags(beta) = %v; want [gamma omega]", got)
	}
}

func TestLazyLayoutWritesOnceAcrossOperations(t *testing.T) {
	ctx := t.Context()
	bucket := &countingBucket{Bucket: memory.NewBucket()}
	target := storageoras.New(bucket)
	body := []byte("layout once")
	desc := descriptorFor(body, ocispec.MediaTypeImageManifest)

	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if got := bucket.layoutPuts.Load(); got != 1 {
		t.Fatalf("after first Push, oci-layout puts = %d; want 1", got)
	}
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("second Push: %v", err)
	}
	if err := target.Tag(ctx, desc, "v1"); err != nil {
		t.Fatalf("Tag: %v", err)
	}
	if got := bucket.layoutPuts.Load(); got != 1 {
		t.Fatalf("oci-layout written %d times; want 1 across multiple operations", got)
	}
}

func TestLazyLayoutRetriesAfterTransientFailure(t *testing.T) {
	ctx := t.Context()
	bucket := &flakyLayoutBucket{Bucket: memory.NewBucket(), failTimes: 1}
	target := storageoras.New(bucket)
	body := []byte("retry layout")
	desc := descriptorFor(body, ocispec.MediaTypeImageManifest)

	err := target.Push(ctx, desc, bytes.NewReader(body))
	if err == nil {
		t.Fatalf("Push: expected error from injected layout failure")
	}
	if !strings.Contains(err.Error(), "injected") {
		t.Fatalf("Push error = %v; want injected", err)
	}

	// Next push must retry the layout PUT and succeed.
	if err := target.Push(ctx, desc, bytes.NewReader(body)); err != nil {
		t.Fatalf("retry Push: %v", err)
	}
	if got := bucket.layoutAttempts.Load(); got != 2 {
		t.Fatalf("oci-layout attempts = %d; want 2 (one fail + one success)", got)
	}
}

func TestCopyAcrossStores(t *testing.T) {
	ctx := t.Context()
	src := storageoras.New(memory.NewBucket())
	dst := storageoras.New(memory.NewBucket())

	layer := []byte("layer payload")
	layerDesc := descriptorFor(layer, ocispec.MediaTypeImageLayer)
	if err := src.Push(ctx, layerDesc, bytes.NewReader(layer)); err != nil {
		t.Fatalf("push layer: %v", err)
	}

	configBody := []byte(`{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}`)
	configDesc := descriptorFor(configBody, ocispec.MediaTypeImageConfig)
	if err := src.Push(ctx, configDesc, bytes.NewReader(configBody)); err != nil {
		t.Fatalf("push config: %v", err)
	}

	manifest := ocispec.Manifest{
		MediaType: ocispec.MediaTypeImageManifest,
		Config:    configDesc,
		Layers:    []ocispec.Descriptor{layerDesc},
	}
	manifest.SchemaVersion = 2
	manifestBody := mustMarshalJSON(t, manifest)
	manifestDesc := descriptorFor(manifestBody, ocispec.MediaTypeImageManifest)
	if err := src.Push(ctx, manifestDesc, bytes.NewReader(manifestBody)); err != nil {
		t.Fatalf("push manifest: %v", err)
	}
	if err := src.Tag(ctx, manifestDesc, "v1"); err != nil {
		t.Fatalf("tag src: %v", err)
	}

	got, err := oras.Copy(ctx, src, "v1", dst, "v1", oras.DefaultCopyOptions)
	if err != nil {
		t.Fatalf("oras.Copy: %v", err)
	}
	if got.Digest != manifestDesc.Digest {
		t.Fatalf("copied descriptor digest = %s; want %s", got.Digest, manifestDesc.Digest)
	}

	resolved, err := dst.Resolve(ctx, "v1")
	if err != nil {
		t.Fatalf("dst.Resolve: %v", err)
	}
	if resolved.Digest != manifestDesc.Digest {
		t.Fatalf("dst tag = %s; want %s", resolved.Digest, manifestDesc.Digest)
	}

	rc, err := dst.Fetch(ctx, layerDesc)
	if err != nil {
		t.Fatalf("dst.Fetch layer: %v", err)
	}
	gotLayer, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read layer: %v", err)
	}
	if !bytes.Equal(gotLayer, layer) {
		t.Fatalf("dst layer mismatch")
	}
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

// readOnlyOnPutBucket fails every PutObject with ErrBucketReadOnly,
// used to drive ensureLayout's preflight error path.
type readOnlyOnPutBucket struct {
	storage.Bucket
}

func (b *readOnlyOnPutBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	_, _ = io.Copy(io.Discard, value)
	return storage.ObjectInfo{}, storage.ErrBucketReadOnly
}

// headFailingBucket returns a fixed error from HeadObject, used to
// drive Push's dedup-check error path.
type headFailingBucket struct {
	storage.Bucket
	err error
}

func (b *headFailingBucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{}, b.err
}

// countingBucket counts PutObject calls (and per-key for the layout marker).
type countingBucket struct {
	storage.Bucket
	puts       atomic.Int64
	layoutPuts atomic.Int64
}

func (b *countingBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	b.puts.Add(1)
	if key == ocispec.ImageLayoutFile {
		b.layoutPuts.Add(1)
	}
	return b.Bucket.PutObject(ctx, key, value, options...)
}

// recordingBucket records every successful PUT so tests can assert
// that unverified bytes never land at the final blob key.
type recordingBucket struct {
	storage.Bucket
	mu      sync.Mutex
	written []string
}

func (b *recordingBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	info, err := b.Bucket.PutObject(ctx, key, value, options...)
	if err == nil {
		b.mu.Lock()
		b.written = append(b.written, key)
		b.mu.Unlock()
	}
	return info, err
}

func (b *recordingBucket) seenAt(key string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, k := range b.written {
		if k == key {
			return true
		}
	}
	return false
}

// flakyLayoutBucket fails the first failTimes PUTs of the oci-layout marker.
type flakyLayoutBucket struct {
	storage.Bucket
	failTimes      int64
	layoutAttempts atomic.Int64
}

func (b *flakyLayoutBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	if key == ocispec.ImageLayoutFile {
		attempt := b.layoutAttempts.Add(1)
		if attempt <= b.failTimes {
			// Drain the reader so any retry semantics are honored.
			_, _ = io.Copy(io.Discard, value)
			return storage.ObjectInfo{}, fmt.Errorf("injected layout failure #%d", attempt)
		}
	}
	return b.Bucket.PutObject(ctx, key, value, options...)
}

func slicesEqual[T comparable](a, b []T) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
