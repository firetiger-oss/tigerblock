package oras

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/firetiger-oss/tigerblock/storage"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	oras "oras.land/oras-go/v2"
	"oras.land/oras-go/v2/errdef"
)

// NewReadOnlyBucket wraps an [oras.ReadOnlyTarget] as a read-only
// [storage.Bucket] presenting each layer of the artifact at reference
// whose descriptor carries an [ocispec.AnnotationTitle] annotation as
// one object at that key. Layers without a title annotation are not
// addressable through this bucket.
//
// reference may be a tag or a digest. The constructor performs no I/O;
// the manifest is resolved, fetched, and parsed on every call that
// needs the layer index. Image-index manifests are rejected with
// [errdef.ErrUnsupported] when first encountered — callers must pick a
// platform manifest first.
//
// All write operations return [storage.ErrBucketReadOnly]. All Presign
// operations return [storage.ErrPresignNotSupported]. WatchObjects
// emits the initial listing and then blocks on context cancellation,
// since OCI artifacts are immutable.
func NewReadOnlyBucket(target oras.ReadOnlyTarget, reference string) storage.Bucket {
	return &readOnlyBucket{target: target, reference: reference}
}

type readOnlyBucket struct {
	target    oras.ReadOnlyTarget
	reference string
}

// loadIndex resolves the bucket's reference, fetches the manifest, and
// returns a title→descriptor map plus the sorted list of titles.
// Called fresh on every method that needs the layer index — no cache.
func (b *readOnlyBucket) loadIndex(ctx context.Context) (map[string]ocispec.Descriptor, []string, error) {
	desc, err := b.target.Resolve(ctx, b.reference)
	if err != nil {
		return nil, nil, makeBucketError(err)
	}
	// Be explicit about what we accept. ocispec.Manifest only models
	// the standard image-manifest layout (config + layers); decoding a
	// different shape (image index, OCI artifact manifest with `blobs`,
	// Docker manifest list) silently leaves Layers empty and the
	// bucket would look like an empty artifact. Reject everything that
	// isn't an image manifest with errdef.ErrUnsupported so callers
	// know to pick a single platform manifest first.
	if desc.MediaType != ocispec.MediaTypeImageManifest {
		return nil, nil, fmt.Errorf("%s: media type %q is not supported (only %s); resolve to a single platform manifest first: %w",
			b.reference, desc.MediaType, ocispec.MediaTypeImageManifest, errdef.ErrUnsupported)
	}

	body, err := b.target.Fetch(ctx, desc)
	if err != nil {
		return nil, nil, makeBucketError(err)
	}
	defer body.Close()

	var manifest ocispec.Manifest
	if err := json.NewDecoder(body).Decode(&manifest); err != nil {
		return nil, nil, fmt.Errorf("decoding manifest %s: %w", desc.Digest, err)
	}

	index := make(map[string]ocispec.Descriptor, len(manifest.Layers))
	for _, layer := range manifest.Layers {
		title := layer.Annotations[ocispec.AnnotationTitle]
		if title == "" {
			continue
		}
		if _, dup := index[title]; dup {
			return nil, nil, fmt.Errorf("manifest %s has duplicate title %q", desc.Digest, title)
		}
		index[title] = layer
	}
	return index, slices.Sorted(maps.Keys(index)), nil
}

func (b *readOnlyBucket) Location() string {
	return "oras://" + b.reference
}

func (b *readOnlyBucket) Access(ctx context.Context) error {
	_, _, err := b.loadIndex(ctx)
	return err
}

func (b *readOnlyBucket) Create(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return storage.ErrBucketReadOnly
}

func (b *readOnlyBucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}
	index, _, err := b.loadIndex(ctx)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	desc, ok := index[key]
	if !ok {
		return storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrObjectNotFound)
	}
	return objectInfoFromDescriptor(desc), nil
}

func (b *readOnlyBucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return nil, storage.ObjectInfo{}, err
	}
	index, _, err := b.loadIndex(ctx)
	if err != nil {
		return nil, storage.ObjectInfo{}, err
	}
	desc, ok := index[key]
	if !ok {
		return nil, storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrObjectNotFound)
	}

	body, err := b.target.Fetch(ctx, desc)
	if err != nil {
		return nil, storage.ObjectInfo{}, makeBucketError(err)
	}

	getOptions := storage.NewGetOptions(options...)
	if start, end, ok := getOptions.BytesRange(); ok {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			body.Close()
			return nil, storage.ObjectInfo{}, err
		}
		// oras's Fetcher only returns the full stream; honour the
		// range by skipping head bytes and limiting the tail. Clamp
		// to the blob size so callers that deliberately over-read the
		// tail (e.g. storage.File.ReadAt, FUSE) get a short read
		// instead of ErrInvalidRange — matches the memory backend.
		if end < 0 {
			end = desc.Size - 1
		}
		skip := min(desc.Size, start)
		take := min(desc.Size, end+1) - skip
		if take < 0 {
			take = 0
		}
		if _, err := io.CopyN(io.Discard, body, skip); err != nil {
			body.Close()
			return nil, storage.ObjectInfo{}, err
		}
		body = struct {
			io.Reader
			io.Closer
		}{io.LimitReader(body, take), body}
	}

	return body, objectInfoFromDescriptor(desc), nil
}

func (b *readOnlyBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return storage.ObjectInfo{}, err
	}
	return storage.ObjectInfo{}, storage.ErrBucketReadOnly
}

func (b *readOnlyBucket) DeleteObject(ctx context.Context, key string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return storage.ErrBucketReadOnly
}

func (b *readOnlyBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for key, err := range objects {
			if cancelErr := context.Cause(ctx); cancelErr != nil {
				yield(key, cancelErr)
				return
			}
			if err != nil {
				if !yield(key, err) {
					return
				}
				continue
			}
			if !yield(key, storage.ErrBucketReadOnly) {
				return
			}
		}
	}
}

func (b *readOnlyBucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return storage.ErrBucketReadOnly
}

func (b *readOnlyBucket) ListObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		index, titles, err := b.loadIndex(ctx)
		if err != nil {
			yield(storage.Object{}, err)
			return
		}
		listOptions := storage.NewListOptions(options...)
		emitListing(yield, index, titles, listOptions)
	}
}

func (b *readOnlyBucket) WatchObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		index, titles, err := b.loadIndex(ctx)
		if err != nil {
			yield(storage.Object{}, err)
			return
		}
		listOptions := storage.NewListOptions(options...)
		if !emitListing(yield, index, titles, listOptions) {
			return
		}
		// OCI artifacts are immutable — there is nothing to watch.
		// Block until the context is cancelled, then exit cleanly.
		<-ctx.Done()
	}
}

func (b *readOnlyBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	if err := context.Cause(ctx); err != nil {
		return "", err
	}
	return "", storage.ErrPresignNotSupported
}

func (b *readOnlyBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...storage.PutOption) (string, error) {
	if err := context.Cause(ctx); err != nil {
		return "", err
	}
	return "", storage.ErrPresignNotSupported
}

func (b *readOnlyBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := context.Cause(ctx); err != nil {
		return "", err
	}
	return "", storage.ErrPresignNotSupported
}

func (b *readOnlyBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := context.Cause(ctx); err != nil {
		return "", err
	}
	return "", storage.ErrPresignNotSupported
}

// emitListing yields entries to yield in title-sorted order, respecting
// KeyPrefix, StartAfter, MaxKeys, and KeyDelimiter. Returns false if
// the consumer asked to stop. StartAfter is compared against the
// emitted key (post-delimiter collapse), not the underlying title, so
// paginated directory listings advance correctly when the caller feeds
// the last returned key back in.
func emitListing(yield func(storage.Object, error) bool, index map[string]ocispec.Descriptor, titles []string, opts *storage.ListOptions) bool {
	prefix := opts.KeyPrefix()
	startAfter := opts.StartAfter()
	delimiter := opts.KeyDelimiter()
	maxKeys := opts.MaxKeys()

	emitted := 0
	seenPrefix := make(map[string]struct{})
	for _, title := range titles {
		if !strings.HasPrefix(title, prefix) {
			continue
		}

		// Resolve the emitted key first — it may be a common prefix
		// rather than the raw title — so StartAfter is compared
		// against what the consumer actually saw on the previous page.
		key := title
		isPrefix := false
		if delimiter != "" {
			rest := title[len(prefix):]
			if i := strings.Index(rest, delimiter); i >= 0 {
				key = prefix + rest[:i+len(delimiter)]
				isPrefix = true
			}
		}

		if startAfter != "" && key <= startAfter {
			continue
		}
		if _, dup := seenPrefix[key]; dup {
			continue
		}
		seenPrefix[key] = struct{}{}

		if maxKeys > 0 && emitted >= maxKeys {
			return true
		}
		emitted++

		obj := storage.Object{Key: key}
		if !isPrefix {
			obj.Size = index[title].Size
		}
		if !yield(obj, nil) {
			return false
		}
	}
	return true
}

func objectInfoFromDescriptor(desc ocispec.Descriptor) storage.ObjectInfo {
	return storage.ObjectInfo{
		ContentType: desc.MediaType,
		Size:        desc.Size,
		ETag:        desc.Digest.String(),
		Metadata:    maps.Clone(desc.Annotations),
	}
}

// makeBucketError translates oras-go errdef sentinels into their
// storage counterparts so callers using errors.Is against the storage
// package match. Original error is preserved in the joined chain.
func makeBucketError(err error) error {
	var sentinel error
	switch {
	case err == nil:
	case errors.Is(err, errdef.ErrNotFound):
		sentinel = storage.ErrObjectNotFound
	case errors.Is(err, errdef.ErrAlreadyExists):
		sentinel = storage.ErrObjectNotMatch
	case errors.Is(err, errdef.ErrInvalidDigest):
		sentinel = storage.ErrChecksumMismatch
	}
	return errors.Join(sentinel, err)
}
