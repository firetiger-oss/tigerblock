package storage

import (
	"bytes"
	"cmp"
	"context"
	"io"
	"iter"
	"time"

	"github.com/firetiger-oss/concurrent"
	"github.com/firetiger-oss/storage/cache"
)

const (
	DefaultCachePageSize       = 256 * 1024       // 256 KiB
	DefaultObjectInfoCacheSize = 512 * 1024       // 512 KiB
	DefaultObjectPageCacheSize = 64 * 1024 * 1024 // 64 MiB
	DefaultObjectCacheSize     = 64 * 1024 * 1024 // 64 MiB
	DefaultCacheTTL            = time.Minute
)

// CacheOption is a function type that can be used to configure new Cache
// instances created by calling NewCache.
type CacheOption func(*Cache)

// CachePageSize sets the size of each page in the cache. This is used when
// fetching byte ranges from objects. If the page size is zero or negative,
// no caching is done for byte ranges.
func CachePageSize(size int64) CacheOption {
	return func(cache *Cache) { cache.pageSize = size }
}

// ObjectCacheSize sets the maximum size of the cache ofr full objects.
func ObjectCacheSize(size int64) CacheOption {
	return func(cache *Cache) { cache.objects.Limit = size }
}

// ObjectInfoCacheSize sets the maximum size of the cache for ObjectInfo
// values (e.g., from HeadObject calls).
func ObjectInfoCacheSize(size int64) CacheOption {
	return func(cache *Cache) { cache.infos.Limit = size }
}

// ObjectPageCacheSize sets the maximum size of the cache for object pages
// stored from calls to GetObject with a byte range.
func ObjectPageCacheSize(size int64) CacheOption {
	return func(cache *Cache) { cache.pages.Limit = size }
}

// CacheTTL sets the time-to-live for cached entries. After the TTL expires,
// entries will be re-fetched on the next access. A TTL of 0 disables expiration.
// The default TTL is 1 minute.
func CacheTTL(d time.Duration) CacheOption {
	return func(cache *Cache) { cache.ttl = d }
}

// Cache is an in-memory cache for objects read from a Bucket.
type Cache struct {
	pages    cache.TTL[objectRange, cachedObject]
	infos    cache.TTL[string, ObjectInfo]
	objects  cache.TTL[string, cachedObject]
	pageSize int64
	ttl      time.Duration
}

type objectRange struct {
	object string
	page   int
}

// NewCache constructs a new Cache instance configured with the options passed
// as arguments.
//
// By default, the page and object caches are 64MiB each, and the object info
// cache is 512KiB. The page size for byte ranges is set to 256KiB. The default
// TTL is 1 minute.
func NewCache(options ...CacheOption) *Cache {
	cache := &Cache{
		pages: cache.TTL[objectRange, cachedObject]{
			Limit: DefaultObjectPageCacheSize,
		},
		infos: cache.TTL[string, ObjectInfo]{
			Limit: DefaultObjectInfoCacheSize,
		},
		objects: cache.TTL[string, cachedObject]{
			Limit: DefaultObjectCacheSize,
		},
		pageSize: DefaultCachePageSize,
		ttl:      DefaultCacheTTL,
	}
	for _, option := range options {
		option(cache)
	}
	return cache
}

// AdaptBucket returns a Bucket that caches the results of calls to HeadObject, and
// GetObject.
func (c *Cache) AdaptBucket(bucket Bucket) Bucket {
	return &cachedBucket{Cache: c, bucket: bucket}
}

func (c *Cache) expireAt() time.Time {
	if c.ttl > 0 {
		return time.Now().Add(c.ttl)
	}
	return time.Time{}
}

// PageSize returns the size of each page in the cache.
func (c *Cache) PageSize() int64 {
	return c.pageSize
}

// CacheStat contains statistics about the cache configuration and utilization.
type CacheStat struct {
	Limit     int64 // Maximum size of the cache in bytes.
	Size      int64 // Current size of the cache in bytes.
	Hits      int64 // Total number of cache hits.
	Misses    int64 // Total number of cache misses.
	Evictions int64 // Total number of evictions from the cache.
}

// Stat returns statistics about the cache, including the page size, number of
func (c *Cache) Stat() (objects, infos, pages CacheStat) {
	return CacheStat(c.objects.Stat()), CacheStat(c.infos.Stat()), CacheStat(c.pages.Stat())
}

var _ Adapter = (*Cache)(nil)

type cachedObject struct {
	info ObjectInfo
	body []byte
}

type cachedBucket struct {
	*Cache
	bucket Bucket
}

func (c *cachedBucket) Location() string {
	return c.bucket.Location()
}

func (c *cachedBucket) Access(ctx context.Context) error {
	return c.bucket.Access(ctx)
}

func (c *cachedBucket) Create(ctx context.Context) error {
	return c.bucket.Create(ctx)
}

func (c *cachedBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	info, _, err := c.infos.Load(key, time.Now(), false, func() (int64, ObjectInfo, time.Time, error) {
		object, err := c.bucket.HeadObject(ctx, key)
		size := int64(0)
		size += int64(len(key))
		size += sizeOfObjectInfo(object)
		return size, object, c.expireAt(), err
	})
	return info, err
}

func (c *cachedBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	var getOptions = NewGetOptions(options...)
	var object cachedObject
	var err error

	if getOptions.byteRange {
		pageSize := c.pageSize
		if pageSize <= 0 {
			return c.bucket.GetObject(ctx, key, options...)
		}
		if err := ValidObjectRange(key, getOptions.start, getOptions.end); err != nil {
			return nil, ObjectInfo{}, err
		}

		ctx, cancel := context.WithCancel(ctx)
		pages := make(chan cachedObject)
		errch := make(chan error, 1)

		go func() {
			defer close(errch)
			defer close(pages)

			for page, err := range concurrent.Pipeline(ctx,
				func(yield func(objectRange, error) bool) {
					startPageIndex := int(getOptions.start / pageSize)
					endPageIndex := int(getOptions.end / pageSize)

					for i := startPageIndex; i <= endPageIndex; i++ {
						if !yield(objectRange{object: key, page: i}, nil) {
							return
						}
						if i == startPageIndex && i+1 < endPageIndex {
							info, err := c.HeadObject(ctx, key)
							if err != nil {
								yield(objectRange{}, err)
								return
							}
							maxPageIndex := max(0, int((info.Size-1)/pageSize))
							endPageIndex = min(endPageIndex, maxPageIndex)
						}
					}
				},

				func(ctx context.Context, thisPageKey objectRange) (cachedObject, error) {
					obj, _, err := c.pages.Load(thisPageKey, time.Now(), false, func() (int64, cachedObject, time.Time, error) {
						thisPageStart := int64(thisPageKey.page) * pageSize
						thisPageEnd := thisPageStart + pageSize - 1
						body, info, err := c.bucket.GetObject(ctx, thisPageKey.object,
							BytesRange(thisPageStart, thisPageEnd))
						if err != nil {
							return 0, cachedObject{}, time.Time{}, err
						}
						defer body.Close()
						pageLength := thisPageEnd - thisPageStart + 1
						page := make([]byte, min(pageLength, info.Size-thisPageStart))
						if _, err := io.ReadFull(body, page); err != nil {
							return 0, cachedObject{}, time.Time{}, err
						}
						object := cachedObject{
							info: info,
							body: page,
						}
						size := int64(0)
						size += int64(len(thisPageKey.object))
						size += int64(len(object.body))
						size += sizeOfObjectInfo(object.info)
						return size, object, c.expireAt(), nil
					})
					return obj, err
				},
			) {
				if err != nil {
					errch <- err
					return
				} else {
					select {
					case pages <- page:
					case <-ctx.Done():
						errch <- context.Cause(ctx)
						return
					}
				}
			}
		}()

		select {
		case firstPage, ok := <-pages:
			if !ok {
				cancel()
				return nil, ObjectInfo{}, <-errch
			}
			offset := getOptions.start % pageSize
			length := getOptions.end - getOptions.start + 1
			firstPage.body = firstPage.body[offset:]
			firstPage.body = firstPage.body[:min(length, int64(len(firstPage.body)))]
			return &cachedPageReader{
				currentPage: firstPage,
				nextPages:   pages,
				nextErrors:  errch,
				remain:      length - int64(len(firstPage.body)),
				cancel:      cancel,
			}, firstPage.info, nil
		case <-ctx.Done():
			cancel()
			for range pages {
			}
			return nil, ObjectInfo{}, context.Cause(ctx)
		}
	}

	object, _, err = c.objects.Load(key, time.Now(), false, func() (int64, cachedObject, time.Time, error) {
		var object cachedObject
		reader, info, err := c.bucket.GetObject(ctx, key)
		if err != nil {
			return 0, object, time.Time{}, err
		}
		defer reader.Close()
		object.info = info
		object.body = make([]byte, info.Size)
		if _, err := io.ReadFull(reader, object.body); err != nil {
			return 0, object, time.Time{}, err
		}
		size := int64(0)
		size += int64(len(key))
		size += int64(len(object.body))
		size += sizeOfObjectInfo(object.info)
		return size, object, c.expireAt(), nil
	})
	if err != nil {
		return nil, ObjectInfo{}, err
	}

	body := object.body
	if getOptions.byteRange {
		body = body[:min(getOptions.end+1, int64(len(body)))]
		body = body[min(getOptions.start, int64(len(body))):]
	}
	return newCachedObjectBody(body), object.info, nil
}

func (c *cachedBucket) PutObject(ctx context.Context, key string, r io.Reader, options ...PutOption) (ObjectInfo, error) {
	info, err := c.bucket.PutObject(ctx, key, r, options...)
	c.objects.Drop(key)
	c.infos.Drop(key)
	return info, err
}

func (c *cachedBucket) DeleteObject(ctx context.Context, key string) error {
	c.objects.Drop(key)
	c.infos.Drop(key)
	return c.bucket.DeleteObject(ctx, key)
}

func (c *cachedBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return c.bucket.DeleteObjects(ctx, func(yield func(string, error) bool) {
		for key, err := range objects {
			c.objects.Drop(key)
			c.infos.Drop(key)
			if !yield(key, err) {
				return
			}
		}
	})
}

func (c *cachedBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	// Invalidate destination cache entry
	c.objects.Drop(to)
	c.infos.Drop(to)
	return c.bucket.CopyObject(ctx, from, to, options...)
}

func (c *cachedBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return c.bucket.ListObjects(ctx, options...)
}

func (c *cachedBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return c.bucket.WatchObjects(ctx, options...)
}

func (c *cachedBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	return c.bucket.PresignGetObject(ctx, key, expiration, options...)
}

func (c *cachedBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	return c.bucket.PresignPutObject(ctx, key, expiration, options...)
}

func (c *cachedBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return c.bucket.PresignHeadObject(ctx, key, expiration)
}

func (c *cachedBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return c.bucket.PresignDeleteObject(ctx, key, expiration)
}

type cachedObjectBody struct {
	bytes.Reader
}

func (r *cachedObjectBody) Close() error {
	r.Reset(nil)
	return nil
}

func newCachedObjectBody(data []byte) *cachedObjectBody {
	r := new(cachedObjectBody)
	r.Reset(data)
	return r
}

// sizeOfObjectInfo returns an estimation of the size of the given ObjectInfo
// in bytes. Ths result does not need to be exactly accurate, for example, we
// don't account for the size of the ObjectInfo struct itself, nor the metadata
// map's overhead.
func sizeOfObjectInfo(info ObjectInfo) (size int64) {
	for k, v := range info.Metadata {
		size += int64(len(k))
		size += int64(len(v))
	}
	size += int64(len(info.CacheControl))
	size += int64(len(info.ContentType))
	size += int64(len(info.ContentEncoding))
	size += int64(len(info.ETag))
	return
}

type cachedPageReader struct {
	currentPage cachedObject
	nextPages   <-chan cachedObject
	nextErrors  <-chan error
	remain      int64
	cancel      context.CancelFunc
}

func (r *cachedPageReader) Read(p []byte) (int, error) {
	for {
		if len(r.currentPage.body) != 0 {
			n := copy(p, r.currentPage.body)
			r.currentPage.body = r.currentPage.body[n:]
			return n, nil
		}
		if r.remain <= 0 {
			return 0, io.EOF
		}
		select {
		case nextPage, ok := <-r.nextPages:
			if !ok {
				return 0, cmp.Or(<-r.nextErrors, io.EOF)
			}
			// Truncate the next page to not exceed remain
			pageSize := int64(len(nextPage.body))
			if pageSize > r.remain {
				nextPage.body = nextPage.body[:r.remain]
				pageSize = r.remain
			}
			r.remain -= pageSize
			r.currentPage = nextPage
		case err := <-r.nextErrors:
			return 0, err
		}
	}
}

func (r *cachedPageReader) Close() error {
	r.cancel()
	for range r.nextPages {
	}
	return nil
}
