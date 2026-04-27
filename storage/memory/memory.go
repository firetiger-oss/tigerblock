package memory

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/internal/sequtil"
)

func init() {
	storage.Register("memory", NewRegistry())
}

func NewRegistry() storage.Registry {
	return storage.RegistryFunc(func(ctx context.Context, bucket string) (storage.Bucket, error) {
		b := new(Bucket)
		if bucket != "" {
			if !strings.HasSuffix(bucket, "/") {
				bucket += "/"
			}
			return storage.Prefix(b, bucket), nil
		}
		return b, nil
	})
}

func NewBucket(entries ...*Entry) *Bucket {
	return NewBucketFrom(func(yield func(string, []byte) bool) {
		for _, entry := range entries {
			if !yield(entry.Key, entry.Value) {
				break
			}
		}
	})
}

func NewBucketFrom(entries iter.Seq2[string, []byte]) *Bucket {
	bucket := &Bucket{
		objects: make(map[string]object),
	}
	lastModified := time.Now()
	for key, value := range entries {
		bucket.objects[key] = object{
			value:        value,
			lastModified: lastModified,
		}
	}
	return bucket
}

type Entry struct {
	Key   string
	Value []byte
}

type Bucket struct {
	mutex     sync.RWMutex
	objects   map[string]object
	listeners map[*listener]struct{}
}

type object struct {
	value           []byte
	etag            string
	cacheControl    string
	contentType     string
	contentEncoding string
	lastModified    time.Time
	metadata        map[string]string
}

type listener struct {
	prefix     string
	delimiter  string
	startAfter string
	wakeChan   chan<- struct{}
}

func (l *listener) notify(keys ...string) {
	for _, key := range keys {
		if !strings.HasPrefix(key, l.prefix) {
			continue
		}
		if l.startAfter > key {
			continue
		}
		// No delimiter filtering here: a write to a/b/c.json must wake a
		// listener on prefix="a/" delimiter="/" because the write may create
		// a new common-prefix entry "a/b/" that wasn't in the previous listing.
		// WatchObjects deduplicates via currentObjects, so spurious wakes are
		// harmless (they just trigger a cheap re-list).
		select {
		case l.wakeChan <- struct{}{}:
		default:
		}
		break
	}
}

func (b *Bucket) Location() string {
	return ":memory:"
}

func (b *Bucket) Access(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return nil
}

func (b *Bucket) Create(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return storage.ErrBucketExist
}

func (b *Bucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}
	b.mutex.RLock()
	object, ok := b.objects[key]
	b.mutex.RUnlock()
	if !ok {
		return storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrObjectNotFound)
	}
	return storage.ObjectInfo{
		CacheControl:    object.cacheControl,
		ContentType:     object.contentType,
		ContentEncoding: object.contentEncoding,
		ETag:            object.etag,
		Size:            int64(len(object.value)),
		LastModified:    object.lastModified,
		Metadata:        maps.Clone(object.metadata),
	}, nil
}

func (b *Bucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return nil, storage.ObjectInfo{}, err
	}
	b.mutex.RLock()
	object, ok := b.objects[key]
	b.mutex.RUnlock()
	if !ok {
		return nil, storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrObjectNotFound)
	}

	getOptions := storage.NewGetOptions(options...)
	content := object.value
	start, end, ok := getOptions.BytesRange()
	if ok {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			return nil, storage.ObjectInfo{}, err
		}
		size := int64(len(content))
		if end < 0 {
			end = size - 1
		}
		i := min(size, start)
		j := min(size, end+1)
		content = content[i:j]
	}

	reader := new(objectReader)
	reader.Reset(content)

	return reader, storage.ObjectInfo{
		CacheControl:    object.cacheControl,
		ContentType:     object.contentType,
		ContentEncoding: object.contentEncoding,
		ETag:            object.etag,
		Size:            int64(len(object.value)),
		LastModified:    object.lastModified,
		Metadata:        maps.Clone(object.metadata),
	}, nil
}

type objectReader struct{ bytes.Reader }

func (r *objectReader) Close() error {
	r.Reset(nil)
	return nil
}

func (b *Bucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	putOptions := storage.NewPutOptions(options...)
	contentLength, err := putOptions.ContentLength(value)
	if err != nil {
		return storage.ObjectInfo{}, err
	}

	buffer := new(bytes.Buffer)
	_, err = buffer.ReadFrom(value)
	if err != nil {
		return storage.ObjectInfo{}, err
	}

	lastModified := time.Now()
	data := buffer.Bytes()
	if contentLength >= 0 && int64(len(data)) != contentLength {
		return storage.ObjectInfo{}, fmt.Errorf("%s: declared content length %d does not match streamed body of %d bytes",
			key, contentLength, len(data))
	}
	if want, ok := putOptions.ChecksumSHA256(); ok {
		if got := sha256.Sum256(data); got != want {
			return storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrChecksumMismatch)
		}
	}
	etag := hashObject(data)

	cacheControl := putOptions.CacheControl()
	contentType := putOptions.ContentType()
	contentEncoding := putOptions.ContentEncoding()
	metadata := putOptions.Metadata()

	b.mutex.Lock()
	defer b.mutex.Unlock()

	if ifMatch := putOptions.IfMatch(); ifMatch != "" {
		if object, ok := b.objects[key]; !ok || hashObject(object.value) != ifMatch {
			return storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrObjectNotMatch)
		}
	}

	if ifNoneMatch := putOptions.IfNoneMatch(); ifNoneMatch == "*" {
		if _, exist := b.objects[key]; exist {
			return storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrObjectNotMatch)
		}
	} else if ifNoneMatch != "" {
		return storage.ObjectInfo{}, fmt.Errorf("%s: %w", key, storage.ErrInvalidObjectTag)
	}

	if b.objects == nil {
		b.objects = make(map[string]object)
	}

	b.objects[key] = object{
		value:           data,
		etag:            etag,
		cacheControl:    cacheControl,
		contentType:     contentType,
		contentEncoding: contentEncoding,
		lastModified:    lastModified,
		metadata:        maps.Clone(metadata),
	}

	object := storage.ObjectInfo{
		CacheControl:    cacheControl,
		ContentType:     contentType,
		ContentEncoding: contentEncoding,
		ETag:            etag,
		Size:            int64(len(data)),
		LastModified:    lastModified,
		Metadata:        metadata,
	}

	b.notify(key)
	return object, nil
}

func (b *Bucket) DeleteObject(ctx context.Context, key string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return err
	}
	b.mutex.Lock()
	delete(b.objects, key)
	b.notify(key)
	b.mutex.Unlock()
	return nil
}

func (b *Bucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for key, err := range objects {
			err = cmp.Or(err, context.Cause(ctx), storage.ValidObjectKey(key))
			if err == nil {
				b.mutex.Lock()
				delete(b.objects, key)
				b.notify(key)
				b.mutex.Unlock()
			}
			if !yield(key, err) {
				return
			}
		}
	}
}

func (b *Bucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(from); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(to); err != nil {
		return err
	}

	putOptions := storage.NewPutOptions(options...)

	b.mutex.Lock()
	defer b.mutex.Unlock()

	srcObj, ok := b.objects[from]
	if !ok {
		return fmt.Errorf("%s: %w", from, storage.ErrObjectNotFound)
	}

	// Create new object with copied data
	newObj := object{
		value:        slices.Clone(srcObj.value),
		lastModified: time.Now(),
	}

	// Merge metadata: source metadata with overrides from options
	if cc := putOptions.CacheControl(); cc != "" {
		newObj.cacheControl = cc
	} else {
		newObj.cacheControl = srcObj.cacheControl
	}

	if ct := putOptions.ContentType(); ct != "application/octet-stream" {
		newObj.contentType = ct
	} else {
		newObj.contentType = srcObj.contentType
	}

	if ce := putOptions.ContentEncoding(); ce != "" {
		newObj.contentEncoding = ce
	} else {
		newObj.contentEncoding = srcObj.contentEncoding
	}

	// Merge metadata maps (overrides win)
	newObj.metadata = maps.Clone(srcObj.metadata)
	if newObj.metadata == nil {
		newObj.metadata = make(map[string]string)
	}
	for k, v := range putOptions.Metadata() {
		newObj.metadata[k] = v
	}

	newObj.etag = hashObject(newObj.value)

	if b.objects == nil {
		b.objects = make(map[string]object)
	}
	b.objects[to] = newObj
	b.notify(to)

	return nil
}

func (b *Bucket) notify(keys ...string) {
	for l := range b.listeners {
		l.notify(keys...)
	}
}

func (b *Bucket) listObjects(ctx context.Context, listOptions *storage.ListOptions) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		if err := context.Cause(ctx); err != nil {
			yield(storage.Object{}, err)
			return
		}

		delimiter := listOptions.KeyDelimiter()
		prefix := listOptions.KeyPrefix()
		startAfter := listOptions.StartAfter()

		b.mutex.RLock()
		var objects []storage.Object
		for key, object := range b.objects {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			size := int64(len(object.value))
			lastModified := object.lastModified
			if delimiter != "" {
				if i := strings.Index(key[len(prefix):], delimiter); i >= 0 {
					key = key[:len(prefix)+len(delimiter)+i]
					size = 0
					lastModified = time.Time{}
				}
			}
			if key <= startAfter {
				continue
			}
			objects = append(objects, storage.Object{
				Key:          key,
				Size:         size,
				LastModified: lastModified,
			})
		}
		b.mutex.RUnlock()

		slices.SortFunc(objects, func(a, b storage.Object) int {
			return strings.Compare(a.Key, b.Key)
		})

		objects = slices.CompactFunc(objects, func(a, b storage.Object) bool {
			return a.Key == b.Key
		})

		for _, object := range objects {
			if !yield(object, nil) {
				return
			}
		}
	}
}

func (b *Bucket) ListObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	listOptions := storage.NewListOptions(options...)
	listObjects := b.listObjects(ctx, listOptions)
	return sequtil.Limit(listObjects, listOptions.MaxKeys())
}

func (b *Bucket) WatchObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		listOptions := storage.NewListOptions(options...)

		wakeChan := make(chan struct{}, 1)
		thisLstn := &listener{
			prefix:     listOptions.KeyPrefix(),
			delimiter:  listOptions.KeyDelimiter(),
			startAfter: listOptions.StartAfter(),
			wakeChan:   wakeChan,
		}

		b.mutex.Lock()
		if b.listeners == nil {
			b.listeners = make(map[*listener]struct{})
		}
		b.listeners[thisLstn] = struct{}{}
		b.mutex.Unlock()

		defer func() {
			b.mutex.Lock()
			delete(b.listeners, thisLstn)
			b.mutex.Unlock()
		}()

		type versionedObject struct {
			storage.Object
			version int
		}

		currentObjects := make(map[string]versionedObject)
		currentVersion := 0

		for {
			for object, err := range b.listObjects(ctx, listOptions) {
				if err != nil {
					return // context canceled
				}

				current, exists := currentObjects[object.Key]
				if !exists || object.LastModified.After(current.LastModified) {
					if !yield(object, nil) {
						return
					}
				}

				currentObjects[object.Key] = versionedObject{
					Object:  object,
					version: currentVersion,
				}
			}

			var deletedObjects []string
			for key, object := range currentObjects {
				if object.version < currentVersion {
					deletedObjects = append(deletedObjects, key)
				}
			}

			if len(deletedObjects) > 0 {
				deletionTime := time.Now()

				slices.SortFunc(deletedObjects, func(a, b string) int {
					return -strings.Compare(a, b)
				})

				for _, key := range deletedObjects {
					if !yield(storage.Object{
						Key:          key,
						Size:         -1, // deletion marker
						LastModified: deletionTime,
					}, nil) {
						return
					}
				}
			}

			currentVersion++
			select {
			case <-wakeChan:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (b *Bucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}

func (b *Bucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...storage.PutOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}

func (b *Bucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}

func (b *Bucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}

func hashObject(data []byte) string {
	hash := md5.Sum(data)
	return hex.EncodeToString(hash[:])
}
