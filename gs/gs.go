package gs

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"iter"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	gcloud "cloud.google.com/go/storage"
	"github.com/firetiger-oss/concurrent"
	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/backoff"
	"github.com/firetiger-oss/storage/cache"
	"github.com/firetiger-oss/storage/gs/gsclient"
	"github.com/firetiger-oss/storage/internal/sequtil"
	"github.com/firetiger-oss/storage/uri"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	storage.Register("gs", NewRegistry())
}

type RegistryOption func(*registryOptions)

type registryOptions struct {
	httpClient *http.Client
}

func WithHTTPClient(httpClient *http.Client) RegistryOption {
	return func(opts *registryOptions) {
		opts.httpClient = httpClient
	}
}

func NewRegistry(options ...RegistryOption) storage.Registry {
	opts := &registryOptions{}
	for _, option := range options {
		option(opts)
	}

	var cachedGoogleClient cache.Value[*gcloud.Client]
	var cachedPutClient cache.Value[*gsclient.Client]
	return storage.RegistryFunc(func(ctx context.Context, bucket string) (storage.Bucket, error) {
		bucketName, prefix, _ := strings.Cut(bucket, "/")

		var httpClient *http.Client
		if opts.httpClient != nil {
			httpClient = opts.httpClient
		} else {
			httpClient = &http.Client{Transport: &http.Transport{}}
		}

		var err error
		httpClient, err = gsclient.NewHTTPClientWithDefaultCredentials(ctx, httpClient)
		if err != nil {
			return nil, err
		}

		googleClient, err := cachedGoogleClient.Load(func() (*gcloud.Client, error) {
			return gcloud.NewClient(context.Background(), option.WithHTTPClient(httpClient))
		})
		if err != nil {
			return nil, err
		}

		putClient, err := cachedPutClient.Load(func() (*gsclient.Client, error) {
			return gsclient.NewGoogleCloudStorageClient(context.Background(), bucketName, gsclient.WithHTTPClient(httpClient))
		})
		if err != nil {
			return nil, err
		}

		b := NewBucket(googleClient, putClient, bucketName)
		if prefix != "" {
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
			return storage.Prefix(b, prefix), nil
		}
		return b, nil
	})
}

func NewBucket(client *gcloud.Client, putClient *gsclient.Client, bucket string, options ...BucketOption) *Bucket {
	b := &Bucket{
		client:    client,
		putClient: putClient,
		bucket:    bucket,
	}
	for _, opt := range options {
		opt(b)
	}
	return b
}

type BucketOption func(*Bucket)

type Bucket struct {
	client    *gcloud.Client
	putClient *gsclient.Client
	bucket    string
}

func (b *Bucket) Location() string {
	return uri.Join("gs", b.bucket)
}

func (b *Bucket) Access(ctx context.Context) error {
	_, err := b.client.Bucket(b.bucket).Attrs(ctx)
	if err != nil {
		return makeIcebergError(err)
	}
	return nil
}

func (b *Bucket) Create(ctx context.Context) error {
	err := b.client.Bucket(b.bucket).Create(ctx, "", nil)
	if err != nil {
		if gerr, ok := err.(*googleapi.Error); ok {
			if gerr.Code == http.StatusConflict {
				return errors.Join(storage.ErrBucketExist, err)
			}
		}
		return makeIcebergError(err)
	}
	return nil
}

func (b *Bucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	obj := b.client.Bucket(b.bucket).Object(key)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(err)
	}

	object := storage.ObjectInfo{
		CacheControl:    attrs.CacheControl,
		ContentType:     attrs.ContentType,
		ContentEncoding: attrs.ContentEncoding,
		ETag:            makeETag(attrs.Generation),
		Size:            attrs.Size,
		LastModified:    attrs.Updated,
		Metadata:        attrs.Metadata,
	}
	return object, nil
}

func (b *Bucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return nil, storage.ObjectInfo{}, err
	}

	// ReadCompressed(true) opts out of GCS's decompressive transcoding.
	// Without it, objects uploaded with Content-Encoding: gzip are
	// silently decompressed on read: attrs.Size is the stored
	// compressed length but the body the caller reads is longer, so
	// range offsets and Content-Length/Content-Range arithmetic
	// downstream (cache adapters, HTTP adapter) break. With it, GCS
	// serves the stored bytes as-is with Content-Encoding: gzip on
	// the wire, attrs.Size matches the body, and callers that want
	// the decompressed content apply their own gzip.Reader — the
	// same contract every other backend in this package already uses.
	// https://cloud.google.com/storage/docs/transcoding
	obj := b.client.Bucket(b.bucket).Object(key).ReadCompressed(true)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, storage.ObjectInfo{}, makeIcebergError(err)
	}

	getOptions := storage.NewGetOptions(options...)
	start, end, hasRange := getOptions.BytesRange()

	object := storage.ObjectInfo{
		CacheControl:    attrs.CacheControl,
		ContentType:     attrs.ContentType,
		ContentEncoding: attrs.ContentEncoding,
		ETag:            makeETag(attrs.Generation),
		Size:            attrs.Size,
		LastModified:    attrs.Updated,
		Metadata:        attrs.Metadata,
	}

	var reader *gcloud.Reader
	if hasRange {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			return nil, storage.ObjectInfo{}, err
		}
		length := int64(-1)
		if end >= 0 {
			length = (end + 1) - start
		}
		reader, err = obj.NewRangeReader(ctx, start, length)
		if err != nil {
			// GCS returns 416 Range Not Satisfiable when the start
			// offset is past the end of the stored object. Translate
			// to an empty reader so the BytesRange(offset, -1)
			// contract (empty body + nil error past end) holds for
			// gs the same as it does for s3/http.
			if isRangeNotSatisfiable(err) {
				return io.NopCloser(strings.NewReader("")), object, nil
			}
			return nil, storage.ObjectInfo{}, makeIcebergError(err)
		}
	} else {
		reader, err = obj.NewReader(ctx)
		if err != nil {
			return nil, storage.ObjectInfo{}, makeIcebergError(err)
		}
	}

	return reader, object, nil
}

func isRangeNotSatisfiable(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == http.StatusRequestedRangeNotSatisfiable
}

func (b *Bucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}
	putOptions := storage.NewPutOptions(options...)
	valueContentLength, err := putOptions.ContentLength(value)
	if err != nil {
		return storage.ObjectInfo{}, err
	}

	// GCS does not natively expose caller-supplied SHA-256 verification
	// (it carries CRC32C/MD5 instead). Fall back to wrapping the body with
	// a reader that hashes on the fly and substitutes ErrChecksumMismatch
	// for io.EOF on the final Read when the running hash doesn't match.
	// The upload library sees a non-EOF error and aborts the request, so
	// nothing is ever committed — no post-hoc DeleteObject required.
	var verifier *sha256VerifyReader
	if sum, ok := putOptions.ChecksumSHA256(); ok {
		verifier = &sha256VerifyReader{r: value, want: sum, hasher: sha256.New()}
		value = verifier
	}

	var object storage.ObjectInfo
	if valueContentLength < 0 {
		object, err = b.putClient.PutObjectStreaming(ctx, key, value, putOptions)
	} else {
		object, err = b.putClient.PutObjectSingleRequest(ctx, key, value, valueContentLength, putOptions)
	}
	// If the verifier saw a mismatch, surface it as ErrChecksumMismatch
	// regardless of how the upload library reported the aborted request
	// (resumable-upload error messages get repackaged on the way back).
	if verifier != nil && verifier.mismatched {
		return storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrChecksumMismatch)
	}
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(err)
	}
	return object, nil
}

// sha256VerifyReader hashes the bytes flowing through it and replaces
// io.EOF on the final Read with [storage.ErrChecksumMismatch] when the
// running hash doesn't match the expected sum. Setting `mismatched`
// lets the caller identify the failure even when the upload library
// repackages the underlying error.
type sha256VerifyReader struct {
	r          io.Reader
	want       [sha256.Size]byte
	hasher     hash.Hash
	mismatched bool
}

func (v *sha256VerifyReader) Read(p []byte) (int, error) {
	n, err := v.r.Read(p)
	if n > 0 {
		v.hasher.Write(p[:n])
	}
	if err == io.EOF {
		var got [sha256.Size]byte
		copy(got[:], v.hasher.Sum(nil))
		if got != v.want {
			v.mismatched = true
			return n, fmt.Errorf("%w", storage.ErrChecksumMismatch)
		}
	}
	return n, err
}

func (b *Bucket) DeleteObject(ctx context.Context, key string) error {
	if err := storage.ValidObjectKey(key); err != nil {
		return err
	}
	err := b.client.Bucket(b.bucket).Object(key).Delete(ctx)
	if err != nil && !errors.Is(err, gcloud.ErrObjectNotExist) {
		return makeIcebergError(err)
	}
	return nil
}

func (b *Bucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		concurrent.Pipeline(ctx, sequtil.Transform(objects, func(key string) (string, error) {
			err := storage.ValidObjectKey(key)
			return key, err
		}), func(ctx context.Context, key string) (string, error) {
			return key, b.DeleteObject(ctx, key)
		})(yield)
	}
}

func (b *Bucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	if err := storage.ValidObjectKey(from); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(to); err != nil {
		return err
	}

	putOptions := storage.NewPutOptions(options...)

	src := b.client.Bucket(b.bucket).Object(from)
	dst := b.client.Bucket(b.bucket).Object(to)

	// Get source attributes for metadata merging
	srcAttrs, err := src.Attrs(ctx)
	if err != nil {
		return makeIcebergError(err)
	}

	copier := dst.CopierFrom(src)

	// Merge metadata: source metadata with overrides from options
	copier.ObjectAttrs = gcloud.ObjectAttrs{
		CacheControl:    srcAttrs.CacheControl,
		ContentType:     srcAttrs.ContentType,
		ContentEncoding: srcAttrs.ContentEncoding,
		Metadata:        srcAttrs.Metadata,
	}

	// Apply overrides
	if cc := putOptions.CacheControl(); cc != "" {
		copier.CacheControl = cc
	}

	if ct := putOptions.ContentType(); ct != "application/octet-stream" {
		copier.ContentType = ct
	}

	if ce := putOptions.ContentEncoding(); ce != "" {
		copier.ContentEncoding = ce
	}

	// Merge metadata maps (overrides win)
	if copier.Metadata == nil {
		copier.Metadata = make(map[string]string)
	}
	for k, v := range putOptions.Metadata() {
		copier.Metadata[k] = v
	}

	_, err = copier.Run(ctx)
	if err != nil {
		return makeIcebergError(err)
	}
	return nil
}

type listedObject struct {
	key          string
	generation   int64
	size         int64
	lastModified time.Time
}

func (b *Bucket) listObjects(ctx context.Context, listOptions *storage.ListOptions) iter.Seq2[listedObject, error] {
	return func(yield func(listedObject, error) bool) {
		delimiter := listOptions.KeyDelimiter()
		prefix := listOptions.KeyPrefix()
		startAfter := listOptions.StartAfter()

		it := b.client.Bucket(b.bucket).Objects(ctx, &gcloud.Query{
			Delimiter:   delimiter,
			Prefix:      prefix,
			StartOffset: startAfter, // inclusive
		})

		objects := make([]listedObject, 0, 100)
		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				return
			}
			if err != nil {
				yield(listedObject{}, makeIcebergError(err))
				return
			}

			if attrs.Prefix != "" {
				if attrs.Prefix > startAfter {
					objects = append(objects, listedObject{
						key: attrs.Prefix,
					})
				}
			} else {
				if attrs.Name > startAfter {
					objects = append(objects, listedObject{
						key:          attrs.Name,
						generation:   attrs.Generation,
						size:         attrs.Size,
						lastModified: attrs.Updated,
					})
				}
			}

			if it.PageInfo().Remaining() == 0 {
				slices.SortFunc(objects, func(a, b listedObject) int {
					return strings.Compare(a.key, b.key)
				})

				for _, object := range objects {
					if !yield(object, nil) {
						return
					}
				}

				objects = objects[:0]
			}
		}
	}
}

func (b *Bucket) ListObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	listOptions := storage.NewListOptions(options...)
	listObjects := func(yield func(storage.Object, error) bool) {
		for object, err := range b.listObjects(ctx, listOptions) {
			if !yield(storage.Object{
				Key:          object.key,
				Size:         object.size,
				LastModified: object.lastModified,
			}, err) {
				return
			}
		}
	}
	return sequtil.Limit(listObjects, listOptions.MaxKeys())
}

func (b *Bucket) WatchObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		type versionedObject struct {
			object  listedObject
			version int
		}

		currentObjects := make(map[string]versionedObject)
		currentVersion := 0
		listOptions := storage.NewListOptions(options...)

		for {
		backoffLoop:
			for _, err := range backoff.Seq(ctx) {
				if err != nil { // context canceled
					return
				}

				var changeCount int
				for object, err := range b.listObjects(ctx, listOptions) {
					if err != nil {
						if !yield(storage.Object{}, err) {
							return
						}
						continue backoffLoop
					}

					current, exists := currentObjects[object.key]
					if !exists ||
						object.generation != current.object.generation ||
						object.lastModified.After(current.object.lastModified) {
						if !yield(storage.Object{
							Key:          object.key,
							Size:         object.size,
							LastModified: object.lastModified,
						}, nil) {
							return
						}
						changeCount++
					}

					currentObjects[object.key] = versionedObject{
						object:  object,
						version: currentVersion,
					}
				}

				var deletedObjects []listedObject
				for key, object := range currentObjects {
					if object.version < currentVersion {
						deletedObjects = append(deletedObjects, object.object)
						delete(currentObjects, key)
					}
				}

				if len(deletedObjects) > 0 {
					deletionTime := time.Now()

					slices.SortFunc(deletedObjects, func(a, b listedObject) int {
						return -strings.Compare(a.key, b.key)
					})

					for _, object := range deletedObjects {
						if !yield(storage.Object{
							Key:          object.key,
							Size:         -1, // deletion marker
							LastModified: deletionTime,
						}, nil) {
							return
						}
						changeCount++
					}
				}

				currentVersion++
				if changeCount > 0 {
					break // continue to outer loop to reset backoff delay
				}
			}
		}
	}
}

func (b *Bucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}

	opts, err := signedGetOptions(key, expiration, options...)
	if err != nil {
		return "", err
	}

	url, err := b.client.Bucket(b.bucket).SignedURL(key, opts)
	if err != nil {
		return "", makeIcebergError(err)
	}
	return url, nil
}

// signedGetOptions builds the gcloud.SignedURLOptions for a GET
// presigned URL. Signing Accept-Encoding to opt out of GCS
// transcoding isn't safe: browsers, curl, and proxies don't all send
// that header value verbatim, so the resulting URL would fail
// signature verification for ordinary consumers. That means presigned
// GETs on gzip-encoded objects still go through GCS's default
// decompressive transcoding — this PR scopes its transcoding opt-out
// to the direct GetObject path.
func signedGetOptions(key string, expiration time.Duration, options ...storage.GetOption) (*gcloud.SignedURLOptions, error) {
	opts := &gcloud.SignedURLOptions{
		Scheme:  gcloud.SigningSchemeV4,
		Method:  "GET",
		Expires: time.Now().Add(expiration),
	}
	getOptions := storage.NewGetOptions(options...)
	if start, end, ok := getOptions.BytesRange(); ok {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			return nil, err
		}
		header := "Range:bytes=" + strconv.FormatInt(start, 10) + "-"
		if end >= 0 {
			header += strconv.FormatInt(end, 10)
		}
		opts.Headers = append(opts.Headers, header)
	}
	return opts, nil
}

func (b *Bucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...storage.PutOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}

	putOptions := storage.NewPutOptions(options...)
	opts := &gcloud.SignedURLOptions{
		Scheme:      gcloud.SigningSchemeV4,
		Method:      "PUT",
		Expires:     time.Now().Add(expiration),
		ContentType: putOptions.ContentType(),
	}

	// Add other headers if specified
	if cacheControl := putOptions.CacheControl(); cacheControl != "" {
		opts.Headers = append(opts.Headers, "Cache-Control:"+cacheControl)
	}
	if contentEncoding := putOptions.ContentEncoding(); contentEncoding != "" {
		opts.Headers = append(opts.Headers, "Content-Encoding:"+contentEncoding)
	}
	if ifMatch := putOptions.IfMatch(); ifMatch != "" {
		opts.Headers = append(opts.Headers, "If-Match:"+ifMatch)
	}
	if ifNoneMatch := putOptions.IfNoneMatch(); ifNoneMatch != "" {
		opts.Headers = append(opts.Headers, "If-None-Match:"+ifNoneMatch)
	}

	// Add custom metadata headers
	for k, v := range putOptions.Metadata() {
		opts.Headers = append(opts.Headers, "x-goog-meta-"+k+":"+v)
	}

	url, err := b.client.Bucket(b.bucket).SignedURL(key, opts)
	if err != nil {
		return "", makeIcebergError(err)
	}
	return url, nil
}

func (b *Bucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}

	opts := &gcloud.SignedURLOptions{
		Scheme:  gcloud.SigningSchemeV4,
		Method:  "HEAD",
		Expires: time.Now().Add(expiration),
	}

	url, err := b.client.Bucket(b.bucket).SignedURL(key, opts)
	if err != nil {
		return "", makeIcebergError(err)
	}
	return url, nil
}

func (b *Bucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}

	opts := &gcloud.SignedURLOptions{
		Scheme:  gcloud.SigningSchemeV4,
		Method:  "DELETE",
		Expires: time.Now().Add(expiration),
	}

	url, err := b.client.Bucket(b.bucket).SignedURL(key, opts)
	if err != nil {
		return "", makeIcebergError(err)
	}
	return url, nil
}

func makeETag(generation int64) string {
	return fmt.Sprintf("%016x", generation)
}

func makeIcebergError(err error) error {
	if errors.Is(err, gcloud.ErrObjectNotExist) {
		return errors.Join(storage.ErrObjectNotFound, err)
	}
	if errors.Is(err, gcloud.ErrBucketNotExist) {
		return errors.Join(storage.ErrBucketNotFound, err)
	}
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == http.StatusPreconditionFailed {
		return errors.Join(storage.ErrObjectNotMatch, err)
	}

	// Handle signing-related errors using proper error type checking
	var gcsErr *googleapi.Error
	if errors.As(err, &gcsErr) {
		// Handle authentication/authorization errors that indicate signing not supported
		if gcsErr.Code == http.StatusUnauthorized || gcsErr.Code == http.StatusForbidden {
			return errors.Join(storage.ErrPresignNotSupported, err)
		}
		if gcsErr.Code == http.StatusTooManyRequests {
			return errors.Join(storage.ErrTooManyRequests, err)
		}
	}

	// Handle specific GCS signing validation errors (these are plain errors.New() calls)
	// These validation errors are NOT wrapped in googleapi.Error, so string checking is necessary
	errMsg := err.Error()
	if strings.Contains(errMsg, "missing required GoogleAccessID") ||
		strings.Contains(errMsg, "missing required SignedURLOptions") ||
		strings.Contains(errMsg, "exactly one of PrivateKey or SignedBytes must be set") ||
		strings.Contains(errMsg, "unable to detect default GoogleAccessID") {
		return errors.Join(storage.ErrPresignNotSupported, err)
	}

	return err
}
