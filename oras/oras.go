// Package oras bridges a [storage.Bucket] to the [oras.land/oras-go/v2] client,
// allowing any backend implemented by storage to act as a content-addressable
// store and tag resolver for OCI artifacts.
//
// One [Store] corresponds to one logical OCI repository. To host multiple
// repositories on the same bucket, wrap with [storage.Prefix] before calling
// [New].
//
// Only SHA-256 digests are supported on Push; other algorithms return
// [errdef.ErrUnsupported]. SHA-384 and SHA-512 are theoretically valid in
// the OCI image spec but absent in practice; rejecting them lets the bridge
// rely on the storage layer's native checksum facilities (e.g. S3's
// x-amz-checksum-sha256) instead of a local digest verifier.
//
// On-bucket layout:
//
//	oci-layout                       written lazily on first push/tag
//	blobs/<algo>/<encoded-digest>    raw blob bytes (immutable)
//	refs/<reference>                 JSON-encoded ocispec.Descriptor
package oras

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"

	"github.com/firetiger-oss/storage"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	oras "oras.land/oras-go/v2"
	orascontent "oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry"
)

// New wraps a [storage.Bucket] as an [oras.Target]. The constructor performs
// no I/O; the OCI image-layout marker is written lazily on the first
// push or tag.
func New(bucket storage.Bucket) oras.Target {
	return &store{bucket: bucket}
}

type store struct {
	bucket      storage.Bucket
	initialized atomic.Bool
}

var (
	_ oras.Target          = (*store)(nil)
	_ orascontent.Deleter  = (*store)(nil)
	_ orascontent.Untagger = (*store)(nil)
	_ registry.TagLister   = (*store)(nil)
)

const (
	blobsPrefix = "blobs/"
	refsPrefix  = "refs/"
)

func (s *store) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	key, err := blobKey(target.Digest)
	if err != nil {
		return nil, err
	}
	body, _, err := s.bucket.GetObject(ctx, key)
	if err != nil {
		return nil, makeError(err)
	}
	return body, nil
}

func (s *store) Push(ctx context.Context, expected ocispec.Descriptor, content io.Reader) error {
	if expected.Digest.Algorithm() != digest.SHA256 {
		return fmt.Errorf("%s: only sha256 is supported: %w",
			expected.Digest.Algorithm(), errdef.ErrUnsupported)
	}
	finalKey, err := blobKey(expected.Digest)
	if err != nil {
		return err
	}
	if err := s.ensureLayout(ctx); err != nil {
		return makeError(err)
	}

	// Content-addressable: same digest ⇒ same bytes. If the blob is already
	// present, drain the reader so the caller doesn't see a half-consumed
	// stream and return success.
	if _, err := s.bucket.HeadObject(ctx, finalKey); err == nil {
		_, _ = io.Copy(io.Discard, content)
		return nil
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return makeError(err)
	}

	raw, err := hex.DecodeString(expected.Digest.Encoded())
	if err != nil {
		return fmt.Errorf("%s: %w", expected.Digest, errdef.ErrInvalidDigest)
	}
	var sum [sha256.Size]byte
	copy(sum[:], raw)

	// Hand verification to the storage layer: native server-side for
	// S3/S3-compatible backends, streaming fallback for the rest. On any
	// digest or size mismatch the bucket guarantees the bytes are not
	// durable at finalKey, so we don't need a staging key or a post-PUT
	// rollback. Every backend enforces ContentLength.
	if _, err := s.bucket.PutObject(ctx, finalKey, content,
		storage.ContentLength(expected.Size),
		storage.ContentType(cmp.Or(expected.MediaType, "application/octet-stream")),
		storage.ChecksumSHA256(sum),
		// Blobs are content-addressable: their bytes never change at
		// this key, so they're safe to cache permanently. Other
		// descriptor-level properties (Annotations, URLs, ArtifactType,
		// Platform) are intentionally NOT mirrored onto the blob —
		// the OCI spec scopes them to the descriptor, and we already
		// preserve the full descriptor JSON in refs/<reference>.
		storage.CacheControl(storage.CacheControlImmutable),
	); err != nil {
		return fmt.Errorf("%s: %w", expected.Digest, makeError(err))
	}
	return nil
}

func (s *store) Exists(ctx context.Context, target ocispec.Descriptor) (bool, error) {
	key, err := blobKey(target.Digest)
	if err != nil {
		return false, err
	}
	if _, err := s.bucket.HeadObject(ctx, key); err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return false, nil
		}
		return false, makeError(err)
	}
	return true, nil
}

func (s *store) Delete(ctx context.Context, target ocispec.Descriptor) error {
	key, err := blobKey(target.Digest)
	if err != nil {
		return err
	}
	return makeError(s.bucket.DeleteObject(ctx, key))
}

func (s *store) Resolve(ctx context.Context, reference string) (ocispec.Descriptor, error) {
	if reference == "" {
		return ocispec.Descriptor{}, errdef.ErrMissingReference
	}

	// A reference of the form "<algo>:<encoded>" is a digest pointing
	// directly into the blob CAS. oras.Copy and oras.Fetch rely on this
	// resolution path when the caller only has a digest.
	if d, err := digest.Parse(reference); err == nil {
		return s.resolveDigest(ctx, d)
	}

	key, err := refKey(reference)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	body, _, err := s.bucket.GetObject(ctx, key)
	if err != nil {
		return ocispec.Descriptor{}, makeError(err)
	}
	defer body.Close()

	var desc ocispec.Descriptor
	if err := json.NewDecoder(body).Decode(&desc); err != nil {
		return ocispec.Descriptor{}, fmt.Errorf("decoding descriptor for %q: %w", reference, err)
	}
	return desc, nil
}

func (s *store) resolveDigest(ctx context.Context, d digest.Digest) (ocispec.Descriptor, error) {
	key, err := blobKey(d)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	info, err := s.bucket.HeadObject(ctx, key)
	if err != nil {
		return ocispec.Descriptor{}, makeError(err)
	}
	// ContentType was stored on Push from desc.MediaType; recover it
	// here so digest-addressed manifests resolve to a descriptor that
	// oras-go recognises (oras.Copy, oras.Resolve, etc. dispatch on
	// MediaType to walk the manifest's config and layers).
	return ocispec.Descriptor{
		MediaType: info.ContentType,
		Digest:    d,
		Size:      info.Size,
	}, nil
}

func (s *store) Tag(ctx context.Context, desc ocispec.Descriptor, reference string) error {
	if reference == "" {
		return errdef.ErrMissingReference
	}
	key, err := refKey(reference)
	if err != nil {
		return err
	}
	blob, err := blobKey(desc.Digest)
	if err != nil {
		return err
	}
	if err := s.ensureLayout(ctx); err != nil {
		return err
	}

	if _, err := s.bucket.HeadObject(ctx, blob); err != nil {
		return fmt.Errorf("%s: %w", desc.Digest, makeError(err))
	}

	body, err := json.Marshal(desc)
	if err != nil {
		return err
	}
	if _, err := s.bucket.PutObject(ctx, key, strings.NewReader(string(body)),
		storage.ContentType(storage.ContentTypeJSON),
	); err != nil {
		return makeError(err)
	}
	return nil
}

func (s *store) Untag(ctx context.Context, reference string) error {
	if reference == "" {
		return errdef.ErrMissingReference
	}
	key, err := refKey(reference)
	if err != nil {
		return err
	}
	if err := s.bucket.DeleteObject(ctx, key); err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil
		}
		return makeError(err)
	}
	return nil
}

func (s *store) Tags(ctx context.Context, last string, fn func(tags []string) error) error {
	const pageSize = 1024

	options := []storage.ListOption{storage.KeyPrefix(refsPrefix)}
	if last != "" {
		options = append(options, storage.StartAfter(refsPrefix+last))
	}

	page := make([]string, 0, pageSize)
	for obj, err := range s.bucket.ListObjects(ctx, options...) {
		if err != nil {
			return err
		}
		tag := strings.TrimPrefix(obj.Key, refsPrefix)
		if tag == "" {
			continue
		}
		page = append(page, tag)
		if len(page) == pageSize {
			if err := fn(page); err != nil {
				return err
			}
			page = page[:0]
		}
	}
	if len(page) > 0 {
		return fn(page)
	}
	return nil
}

func (s *store) ensureLayout(ctx context.Context) error {
	if s.initialized.Load() {
		return nil
	}
	_, err := s.bucket.PutObject(ctx, ocispec.ImageLayoutFile,
		strings.NewReader(`{"imageLayoutVersion":"`+ocispec.ImageLayoutVersion+`"}`),
		storage.IfNoneMatch("*"),
		storage.ContentType(storage.ContentTypeJSON),
	)
	if err != nil && !errors.Is(err, storage.ErrObjectNotMatch) {
		return err
	}
	s.initialized.Store(true)
	return nil
}

func blobKey(d digest.Digest) (string, error) {
	if err := d.Validate(); err != nil {
		return "", fmt.Errorf("%s: %w", d, errdef.ErrInvalidDigest)
	}
	return blobsPrefix + d.Algorithm().String() + "/" + d.Encoded(), nil
}

func refKey(ref string) (string, error) {
	if ref == "." || ref == ".." || strings.ContainsAny(ref, "/\\\x00\r\n") {
		return "", fmt.Errorf("%q: %w", ref, errdef.ErrInvalidReference)
	}
	return refsPrefix + ref, nil
}

// makeError translates errors returned by the storage package into the
// oras-go errdef counterpart so callers can use errors.Is against the
// oras-go sentinels. The original storage error is preserved in the
// joined chain. Errors with no clean counterpart are wrapped as-is so
// errors.Is still matches every sentinel they carry.
func makeError(err error) error {
	var sentinel error
	switch {
	case err == nil:
		// leave sentinel nil; errors.Join(nil, nil) returns nil
	case errors.Is(err, storage.ErrObjectNotFound),
		errors.Is(err, storage.ErrBucketNotFound):
		sentinel = errdef.ErrNotFound
	case errors.Is(err, storage.ErrObjectNotMatch):
		// IfNoneMatch="*" failures surface here; the semantic is
		// "the object already exists at this key", which lines up
		// with errdef.ErrAlreadyExists.
		sentinel = errdef.ErrAlreadyExists
	case errors.Is(err, storage.ErrChecksumMismatch):
		sentinel = errdef.ErrInvalidDigest
	case errors.Is(err, storage.ErrBucketReadOnly):
		sentinel = errdef.ErrUnsupported
	}
	return errors.Join(sentinel, err)
}
