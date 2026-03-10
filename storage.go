package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/firetiger-oss/storage/cache"
	"github.com/firetiger-oss/storage/uri"
)

type temporaryError struct{ err error }

func (e *temporaryError) Error() string   { return e.err.Error() }
func (e *temporaryError) Unwrap() error   { return e.err }
func (e *temporaryError) Temporary() bool { return true }

func makeTemporary(err error) error { return &temporaryError{err: err} }

var (
	ErrBucketExist         = errors.New("bucket exist")
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBucketReadOnly      = errors.New("read-only bucket")
	ErrObjectNotFound      = errors.New("object not found")
	ErrObjectNotMatch      = makeTemporary(errors.New("object mismatch"))
	ErrInvalidObjectKey    = errors.New("invalid object key")
	ErrInvalidObjectTag    = errors.New("invalid object tag")
	ErrInvalidRange        = errors.New("offset out of range")
	ErrPresignNotSupported = errors.New("presigned URLs not supported")
	ErrPresignRedirect     = errors.New("redirect to presigned URL")
	ErrTooManyRequests     = makeTemporary(errors.New("too many requests"))
)

const (
	ContentTypeJSON       = "application/json"
	ContentTypeAvro       = "application/avro"
	ContentTypeParquet    = "application/vnd.apache.parquet"
	CacheControlImmutable = "public, max-age=31536000, immutable"
)

// Object is the type of values returned by the ListObjects method.
//
// This type contains the minimal set of information available about each
// object key when iterating through a prefix of the object store.
type Object struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last-modified,omitzero"`
}

// ObjectInfo represent detailed metadata about an object.
//
// This type differs from Object by not including the key, which is always
// known to the application when obtaining an ObjectInfo, and by including
// more metadata that are not available when iterating through a prefix of the
// object store.
type ObjectInfo struct {
	CacheControl    string            `json:"cache-control,omitempty"`
	ContentType     string            `json:"content-type,omitempty"`
	ContentEncoding string            `json:"content-encoding,omitempty"`
	ETag            string            `json:"etag,omitempty"`
	Size            int64             `json:"size"`
	LastModified    time.Time         `json:"last-modified,omitzero"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

// Bucket is an interface describing an object storage bucket. It is
// modeled off of the S3 object storage API. While it has many
// implementations, it uses S3's interface as the common denominator,
// because that is standard in the industry for object storage.
type Bucket interface {
	// Location returns a URI for the bucket. It always includes a
	// scheme component, and may include a path component.
	//
	// Some example location values:
	//
	//   s3://some-bucket
	//   gcs://another-one/with-prefix
	//
	// As a special exception, the "memory" bucket implementation
	// does _not_ contain a scheme prefix, and instead has the
	// special hostname of ":memory:", for historical reasons.
	Location() string

	// Access verifies that the bucket is accessible. It returns
	// nil error only if the bucket can be reached. This can be
	// used to test bucket existence and authentication.
	Access(ctx context.Context) error

	// Create instantiates a new bucket at Location().
	Create(ctx context.Context) error

	// HeadObject retrieves metadata about the object stored at
	// key.
	HeadObject(ctx context.Context, key string) (ObjectInfo, error)

	// GetObject retrieves the contents of the object stored at
	// key, as well as its metadata.
	GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error)

	// PutObject stores bytes at key.
	PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error)

	// DeleteObject removes whatever is found at key. It returns
	// an error if there is nothing there.
	DeleteObject(ctx context.Context, key string) error

	// DeleteObjects deletes multiple objects. It consumes the input sequence
	// of object keys and yields results for each deletion. The output sequence
	// yields (key, nil) for successful deletions and (key, error) for failures.
	// Input errors are propagated immediately. The stream must be consumed to
	// drive the deletion process.
	DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error]

	// CopyObject copies an object from one key to another within the same bucket.
	// Source object metadata is preserved by default; any PutOptions provided will
	// override specific fields (merge semantics).
	CopyObject(ctx context.Context, from, to string, options ...PutOption) error

	// ListObjects gathers a list of abbreviated metadata for all
	// objects in a bucket, or under a key prefix (set through a
	// ListOption).
	ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error]

	// WatchObjects is list ListObjects but the sequence doesn't end.
	// After listing the objects, it watches for any changes to the
	// prefix and yields new objects as they are added, modified, or
	// removed. The removal of objects is indicated by yielding an
	// Object with a negative Size.
	WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error]

	// PresignGetObject generates a presigned URL for getting an object.
	// The expiration parameter specifies how long the presigned URL will remain valid.
	PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error)

	// PresignPutObject generates a presigned URL for putting an object.
	// The expiration parameter specifies how long the presigned URL will remain valid.
	PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error)

	// PresignHeadObject generates a presigned URL for getting object metadata.
	PresignHeadObject(ctx context.Context, key string) (string, error)

	// PresignDeleteObject generates a presigned URL for deleting an object.
	PresignDeleteObject(ctx context.Context, key string) (string, error)
}

type Adapter interface {
	AdaptBucket(Bucket) Bucket
}

type AdapterFunc func(Bucket) Bucket

func (a AdapterFunc) AdaptBucket(b Bucket) Bucket { return a(b) }

func AdaptBucket(bucket Bucket, adapters ...Adapter) Bucket {
	for _, adapter := range adapters {
		bucket = adapter.AdaptBucket(bucket)
	}
	return bucket
}

type Registry interface {
	LoadBucket(ctx context.Context, bucketURI string) (Bucket, error)
}

// registryFunc is a caching implementation of Registry.
type registryFunc struct {
	load  func(context.Context, string) (Bucket, error)
	cache cache.Cache[string, Bucket]
}

// RegistryFunc creates a Registry that caches bucket instances by URI.
// The load function is called to create buckets on cache miss.
// Bucket URIs are normalized before lookup to ensure consistent caching.
func RegistryFunc(load func(context.Context, string) (Bucket, error)) Registry {
	return &registryFunc{
		load:  load,
		cache: *cache.New[string, Bucket](1024),
	}
}

func (r *registryFunc) LoadBucket(ctx context.Context, bucketURI string) (Bucket, error) {
	// Normalize URI for consistent cache keys
	scheme, location, path := uri.Split(bucketURI)
	normalizedURI := uri.Join(scheme, location, path)
	normalizedURI = strings.TrimSuffix(normalizedURI, "/")

	return r.cache.Load(normalizedURI, func() (Bucket, error) {
		return r.load(ctx, bucketURI)
	})
}

func SingleBucketRegistry(bucket Bucket) Registry {
	return RegistryFunc(func(ctx context.Context, bucketURI string) (Bucket, error) {
		bucketType, bucketName, objectURI := uri.Split(bucketURI)
		bucketLocation := uri.Join(bucketType, bucketName)
		if bucketLocation != bucket.Location() {
			return nil, fmt.Errorf("%s: %w (only has %s)", bucketURI, ErrBucketNotFound, bucket.Location())
		}
		return normalizeBucket(bucket, bucketType, objectURI), nil
	})
}

// WithAdapters returns a Registry that applies the given adapters to all
// buckets loaded from the wrapped registry. This allows custom registries
// to have adapters applied, similar to how Install() works for the global
// registry.
func WithAdapters(registry Registry, adapters ...Adapter) Registry {
	if len(adapters) == 0 {
		return registry
	}
	return &adaptedRegistry{
		registry: registry,
		adapters: slices.Clone(adapters),
	}
}

type adaptedRegistry struct {
	registry Registry
	adapters []Adapter
}

func (r *adaptedRegistry) LoadBucket(ctx context.Context, bucketURI string) (Bucket, error) {
	bucket, err := r.registry.LoadBucket(ctx, bucketURI)
	if err != nil {
		return nil, err
	}
	return AdaptBucket(bucket, r.adapters...), nil
}

var (
	globalMutex    sync.RWMutex
	globalAdapters []Adapter
	globalRegistry = map[string]Registry{}
)

func WithScheme(scheme string) Adapter {
	return AdapterFunc(func(b Bucket) Bucket {
		return &typedBucket{
			Bucket:     b,
			bucketType: scheme,
		}
	})
}

type typedBucket struct {
	Bucket
	bucketType string
}

func (b *typedBucket) Location() string {
	_, location, prefix := uri.Split(b.Bucket.Location())
	return uri.Join(b.bucketType, location, prefix)
}

func Register(typ string, reg Registry) {
	globalMutex.Lock()
	globalRegistry[typ] = reg
	globalMutex.Unlock()
}

func Install(adapters ...Adapter) {
	globalMutex.Lock()
	globalAdapters = append(globalAdapters, adapters...)
	globalMutex.Unlock()
}

var defaultRegistry = RegistryFunc(loadBucket)

// DefaultRegistry returns the default caching registry that loads buckets
// from the global registry and applies global adapters.
func DefaultRegistry() Registry {
	return defaultRegistry
}

// LoadBucket loads a bucket using the default caching registry.
// Bucket instances are cached by normalized URI to avoid recreating
// adapters on each call.
func LoadBucket(ctx context.Context, bucketURI string) (Bucket, error) {
	return defaultRegistry.LoadBucket(ctx, bucketURI)
}

// loadBucket is the internal bucket loading logic (not cached).
// It looks up the bucket type in the global registry, loads the bucket,
// normalizes it, and applies global adapters.
func loadBucket(ctx context.Context, bucketURI string) (Bucket, error) {
	bucketType, bucketName, objectKey := uri.Split(bucketURI)
	globalMutex.RLock()
	bucketAdapters := globalAdapters
	bucketRegistry, ok := globalRegistry[bucketType]
	globalMutex.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%s: %w (did you forget the import?)", bucketURI, ErrBucketNotFound)
	}
	bucket, err := bucketRegistry.LoadBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	bucket = normalizeBucket(bucket, bucketType, objectKey)
	bucket = AdaptBucket(bucket, bucketAdapters...)
	return bucket, nil
}

func normalizeBucket(bucket Bucket, bucketType, objectKey string) Bucket {
	if objectKey != "" {
		if !strings.HasSuffix(objectKey, "/") {
			objectKey += "/"
		}
		bucket = WithPrefix(objectKey).AdaptBucket(bucket)
	}
	if bucketType != "" {
		bucket = WithScheme(bucketType).AdaptBucket(bucket)
	}
	return bucket
}

func HeadObject(ctx context.Context, objectURI string) (ObjectInfo, error) {
	return HeadObjectAt(ctx, DefaultRegistry(), objectURI)
}

func HeadObjectAt(ctx context.Context, registry Registry, objectURI string) (ObjectInfo, error) {
	bucketType, bucketName, objectKey := uri.Split(objectURI)
	bucket, err := registry.LoadBucket(ctx, uri.Join(bucketType, bucketName))
	if err != nil {
		return ObjectInfo{}, err
	}
	return bucket.HeadObject(ctx, objectKey)
}

func GetObject(ctx context.Context, objectURI string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	return GetObjectAt(ctx, DefaultRegistry(), objectURI, options...)
}

func GetObjectAt(ctx context.Context, registry Registry, objectURI string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	bucketType, bucketName, objectKey := uri.Split(objectURI)
	bucket, err := registry.LoadBucket(ctx, uri.Join(bucketType, bucketName))
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	return bucket.GetObject(ctx, objectKey, options...)
}

func PutObject(ctx context.Context, objectURI string, object io.Reader, options ...PutOption) (ObjectInfo, error) {
	return PutObjectAt(ctx, DefaultRegistry(), objectURI, object, options...)
}

func PutObjectAt(ctx context.Context, registry Registry, objectURI string, object io.Reader, options ...PutOption) (ObjectInfo, error) {
	bucketType, bucketName, objectKey := uri.Split(objectURI)
	bucket, err := registry.LoadBucket(ctx, uri.Join(bucketType, bucketName))
	if err != nil {
		return ObjectInfo{}, err
	}
	return bucket.PutObject(ctx, objectKey, object, options...)
}

func PutObjectWriter(ctx context.Context, objectURI string, options ...PutOption) io.WriteCloser {
	return PutObjectAtWriter(ctx, DefaultRegistry(), objectURI, options...)
}

// PutObjectAtWriter wraps a storage.PutObjectAt call in a WriteCloser.
// Errors returned by PutObjectAt are passed through the WriteCloser methods.
func PutObjectAtWriter(ctx context.Context, registry Registry, objectURI string, options ...PutOption) io.WriteCloser {
	pr, pw := io.Pipe()
	ow := &objectWriter{
		pw:    pw,
		errCh: make(chan error, 1),
	}

	go func() {
		defer close(ow.errCh)

		_, err := PutObjectAt(ctx, registry, objectURI, pr, options...)
		_ = pr.CloseWithError(err)
		ow.errCh <- err
	}()

	return ow
}

type objectWriter struct {
	pw    *io.PipeWriter
	errCh chan error
	err   error
	once  sync.Once
}

func (ow *objectWriter) Close() error {
	_ = ow.pw.Close()
	ow.once.Do(func() {
		ow.err = <-ow.errCh
	})
	return ow.err
}

func (ow *objectWriter) Write(p []byte) (int, error) {
	return ow.pw.Write(p)
}

func DeleteObject(ctx context.Context, objectURI string) error {
	return DeleteObjectAt(ctx, DefaultRegistry(), objectURI)
}

func DeleteObjectAt(ctx context.Context, registry Registry, objectURI string) error {
	bucketType, bucketName, objectKey := uri.Split(objectURI)
	bucket, err := registry.LoadBucket(ctx, uri.Join(bucketType, bucketName))
	if err != nil {
		return err
	}
	return bucket.DeleteObject(ctx, objectKey)
}

func CopyObject(ctx context.Context, sourceURI, destURI string, options ...PutOption) error {
	return CopyObjectAt(ctx, DefaultRegistry(), sourceURI, destURI, options...)
}

func CopyObjectAt(ctx context.Context, registry Registry, sourceURI, destURI string, options ...PutOption) error {
	srcScheme, srcBucket, srcKey := uri.Split(sourceURI)
	dstScheme, dstBucket, dstKey := uri.Split(destURI)

	srcBucketURI := uri.Join(srcScheme, srcBucket)
	dstBucketURI := uri.Join(dstScheme, dstBucket)

	// Same bucket URI: only load once
	if srcBucketURI == dstBucketURI {
		bucket, err := registry.LoadBucket(ctx, srcBucketURI)
		if err != nil {
			return err
		}
		return bucket.CopyObject(ctx, srcKey, dstKey, options...)
	}

	// Different bucket URIs: load both to check if they resolve to the same backend
	srcBucketObj, err := registry.LoadBucket(ctx, srcBucketURI)
	if err != nil {
		return err
	}

	dstBucketObj, err := registry.LoadBucket(ctx, dstBucketURI)
	if err != nil {
		return err
	}

	// Check if they resolve to the same underlying bucket
	if srcBucketObj.Location() == dstBucketObj.Location() {
		return srcBucketObj.CopyObject(ctx, srcKey, dstKey, options...)
	}

	// Cross-bucket: streaming fallback
	reader, srcInfo, err := GetObjectAt(ctx, registry, sourceURI)
	if err != nil {
		return err
	}
	defer reader.Close()

	mergedOpts := mergePutOptions(srcInfo, options...)
	_, err = PutObjectAt(ctx, registry, destURI, reader, mergedOpts...)
	return err
}

// copyObjectStreaming performs a cross-bucket copy via GetObject -> PutObject.
func copyObjectStreaming(ctx context.Context, srcBucket Bucket, srcKey string, dstBucket Bucket, dstKey string, options ...PutOption) error {
	reader, srcInfo, err := srcBucket.GetObject(ctx, srcKey)
	if err != nil {
		return err
	}
	defer reader.Close()

	mergedOpts := mergePutOptions(srcInfo, options...)
	_, err = dstBucket.PutObject(ctx, dstKey, reader, mergedOpts...)
	return err
}

// mergePutOptions creates PutOptions that preserve source metadata with overrides.
func mergePutOptions(srcInfo ObjectInfo, overrides ...PutOption) []PutOption {
	override := NewPutOptions(overrides...)

	var opts []PutOption

	// CacheControl
	if cc := override.CacheControl(); cc != "" {
		opts = append(opts, CacheControl(cc))
	} else if srcInfo.CacheControl != "" {
		opts = append(opts, CacheControl(srcInfo.CacheControl))
	}

	// ContentType
	if ct := override.ContentType(); ct != "application/octet-stream" {
		opts = append(opts, ContentType(ct))
	} else if srcInfo.ContentType != "" {
		opts = append(opts, ContentType(srcInfo.ContentType))
	}

	// ContentEncoding
	if ce := override.ContentEncoding(); ce != "" {
		opts = append(opts, ContentEncoding(ce))
	} else if srcInfo.ContentEncoding != "" {
		opts = append(opts, ContentEncoding(srcInfo.ContentEncoding))
	}

	// Metadata - merge maps (override wins)
	metadata := make(map[string]string)
	for k, v := range srcInfo.Metadata {
		metadata[k] = v
	}
	for k, v := range override.Metadata() {
		metadata[k] = v
	}
	for k, v := range metadata {
		opts = append(opts, Metadata(k, v))
	}

	return opts
}

func DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return DeleteObjectsAt(ctx, DefaultRegistry(), objects)
}

func DeleteObjectsAt(ctx context.Context, registry Registry, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		type result struct {
			key string
			err error
		}

		resch := make(chan result)
		group := new(sync.WaitGroup)
		group.Add(1)

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		go func() {
			defer group.Done()

			type bucketURI struct {
				bucketType, bucketName string
			}

			type bucketChannel struct {
				keys chan<- string
				done <-chan struct{}
			}

			buckets := make(map[bucketURI]bucketChannel)
			defer func() {
				for _, ch := range buckets {
					close(ch.keys)
				}
			}()

			for key, err := range objects {
				if err != nil {
					resch <- result{key, err}
					continue
				}

				bucketType, bucketName, objectKey := uri.Split(key)
				bucketKey := bucketURI{bucketType, bucketName}
				bucketChan, ok := buckets[bucketKey]
				if !ok {
					bucket, err := registry.LoadBucket(ctx, uri.Join(bucketType, bucketName))
					if err != nil {
						resch <- result{key, err}
						continue
					}
					keys := make(chan string, 10)
					done := make(chan struct{})
					bucketChan = bucketChannel{keys, done}
					buckets[bucketKey] = bucketChan

					group.Add(1)
					go func() {
						defer group.Done()
						defer close(done)

						for key, err := range bucket.DeleteObjects(ctx, func(yield func(string, error) bool) {
							for key := range keys {
								if !yield(key, nil) {
									return
								}
							}
						}) {
							if key != "" {
								key = uri.Join(bucketType, bucketName, key)
							}
							resch <- result{key, err}
						}
					}()
				}

				select {
				case bucketChan.keys <- objectKey:
				case <-bucketChan.done:
				case <-ctx.Done():
					resch <- result{key, context.Cause(ctx)}
					return
				}
			}
		}()

		go func() {
			group.Wait()
			close(resch)
		}()

		defer func() {
			for range resch {
			}
		}()

		for r := range resch {
			if !yield(r.key, r.err) {
				cancel()
				return
			}
		}
	}
}

func Location(location, path string) string {
	scheme, location, base := uri.Split(location)
	return uri.Join(scheme, location, base, path)
}

func ValidObjectKey(key string) error {
	if !fs.ValidPath(key) {
		return fmt.Errorf("%w (%s)", ErrInvalidObjectKey, key)
	}
	return nil
}

func ValidObjectRange(key string, start, end int64) error {
	if start < 0 || end < 0 || end < start {
		return fmt.Errorf("%s: %w (start=%d, end=%d)", key, ErrInvalidRange, start, end)
	}
	return nil
}

func ListObjects(ctx context.Context, prefixURI string, options ...ListOption) iter.Seq2[Object, error] {
	return ListObjectsAt(ctx, DefaultRegistry(), prefixURI, options...)
}

func ListObjectsAt(ctx context.Context, registry Registry, prefixURI string, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		bucketType, bucketName, objectPrefix := uri.Split(prefixURI)
		bucket, err := registry.LoadBucket(ctx, uri.Join(bucketType, bucketName))
		if err != nil {
			yield(Object{}, err)
			return
		}

		listOptions := slices.Clip(options)
		if objectPrefix != "" {
			listOptions = append(listOptions, KeyPrefix(objectPrefix))
		}

		for object, err := range bucket.ListObjects(ctx, listOptions...) {
			if err != nil {
				yield(Object{}, err)
				return
			}
			object.Key = uri.Join(bucketType, bucketName, object.Key)
			if !yield(object, nil) {
				return
			}
		}
	}
}
