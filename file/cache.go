package file

import (
	"cmp"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/EmissarySocial/emissary/tools/cacheheader"
	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/cache/lru"
)

// isErrNoSpace checks if an error is due to no space left on device (ENOSPC)
// or disk quota exceeded (EDQUOT).
func isErrNoSpace(err error) bool {
	return errors.Is(err, syscall.ENOSPC) || errors.Is(err, syscall.EDQUOT)
}

type Cache struct {
	tmpdir string
	size   int64
	log    *slog.Logger
	init   bool
	once   sync.Once
	mutex  sync.Mutex
	items  lru.LRU[string, struct{}]
}

func NewCache(tmpdir string, size int64) *Cache {
	return &Cache{tmpdir: tmpdir, size: size, log: slog.Default()}
}

// shouldCacheObject determines if an object should be cached based on its cache control headers
func shouldCacheObject(info storage.ObjectInfo) bool {
	// If there is no cache control, do not cache the object
	if info.CacheControl == "" {
		return false
	}

	// Parse the cache control header
	cc := cacheheader.ParseString(info.CacheControl)

	// Do not cache objects that are private
	if cc.Private {
		return false
	}

	// Do not cache objects that have no-store (but allow no-cache and must-revalidate for revalidation)
	if cc.NoStore {
		return false
	}

	// Cache if there's a max age, immutable, or needs revalidation (no-cache/must-revalidate)
	return cc.MaxAge > 0 || cc.Immutable || cc.NoCache || cc.MustRevalidate
}

// isObjectExpired checks if a cached object has expired based on its cache control and file modification time
func isObjectExpired(info storage.ObjectInfo, fileInfo os.FileInfo) bool {
	// If there's no cache control, consider it expired
	if info.CacheControl == "" {
		return true
	}

	cc := cacheheader.ParseString(info.CacheControl)

	// If object is immutable, it never expires
	if cc.Immutable {
		return false
	}

	// If there's no max age, cannot expire
	if cc.MaxAge <= 0 {
		return false
	}

	// Check if the object has exceeded its max age based on file creation time
	age := time.Since(fileInfo.ModTime())
	maxAge := time.Duration(cc.MaxAge) * time.Second
	return age > maxAge
}

// needsRevalidation checks if a cached object needs to be revalidated with the backend
func needsRevalidation(info storage.ObjectInfo) bool {
	if info.CacheControl == "" {
		return false
	}
	cc := cacheheader.ParseString(info.CacheControl)
	return cc.NoCache || cc.MustRevalidate
}

func (c *Cache) Stat() storage.CacheStat {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return storage.CacheStat{
		Limit:     c.size,
		Size:      c.items.Size,
		Hits:      c.items.Hits,
		Misses:    c.items.Misses,
		Evictions: c.items.Evictions,
	}
}

func (c *Cache) AdaptBucket(bucket storage.Bucket) storage.Bucket {
	c.once.Do(func() {
		if err := os.MkdirAll(c.tmpdir, 0755); err != nil {
			return
		}

		filepath.Walk(c.tmpdir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !info.IsDir() {
				c.insert(path, info.Size())
			}
			return nil
		})

		c.init = true
	})

	if !c.init {
		return bucket
	}

	prefix := md5.Sum([]byte(bucket.Location()))
	return &cachedBucket{
		Cache:  c,
		bucket: bucket,
		prefix: hex.EncodeToString(prefix[:]),
	}
}

func (c *Cache) delete(path string) {
	c.mutex.Lock()
	c.items.Delete(path)
	c.mutex.Unlock()
}

func (c *Cache) lookup(path string) {
	c.mutex.Lock()
	c.items.Lookup(path) // just touch it to update the LRU data structure
	c.mutex.Unlock()
}

func (c *Cache) insert(path string, size int64) {
	var evictedPaths []string

	defer func() {
		for _, evictedPath := range evictedPaths {
			os.Remove(evictedPath)
		}
	}()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.items.Insert(path, struct{}{}, size) {
		for c.items.Size > c.size {
			evictedPath, _, _, hasEvicted := c.items.Evict()
			if !hasEvicted {
				break
			}
			evictedPaths = append(evictedPaths, evictedPath)
		}
	}
}

// evictForSpace performs emergency eviction to free up disk space.
// It evicts LRU entries until at least targetSize bytes are freed.
// Returns the total bytes freed.
func (c *Cache) evictForSpace(targetSize int64) int64 {
	var evictedPaths []string
	var freedBytes int64

	defer func() {
		for _, evictedPath := range evictedPaths {
			os.Remove(evictedPath)
		}
	}()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for freedBytes < targetSize {
		evictedPath, _, evictedSize, hasEvicted := c.items.Evict()
		if !hasEvicted {
			break
		}
		evictedPaths = append(evictedPaths, evictedPath)
		freedBytes += evictedSize
	}

	return freedBytes
}

type cachedBucket struct {
	*Cache
	bucket storage.Bucket
	prefix string
}

func (b *cachedBucket) Location() string {
	return b.bucket.Location()
}

func (b *cachedBucket) Access(ctx context.Context) error {
	return b.bucket.Access(ctx)
}

func (b *cachedBucket) Create(ctx context.Context) error {
	return b.bucket.Create(ctx)
}

func (b *cachedBucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	filePath := b.makeFilePath(key)
	f, err := os.Open(filePath)
	if err == nil {
		defer f.Close()
		if info, _, err := readObjectInfo(f); err == nil {
			// Check if the cached object has expired
			if fileInfo, err := f.Stat(); err == nil && !isObjectExpired(info, fileInfo) {
				b.lookup(f.Name())
				return info, nil
			} else {
				// Object has expired, remove it from cache
				b.delete(filePath)
				os.Remove(filePath)
			}
		}
	}

	info, err := b.bucket.HeadObject(ctx, key)
	if err != nil {
		return storage.ObjectInfo{}, err
	}

	if info.Size <= 256*1024 && shouldCacheObject(info) {
		body, _, err := b.getObjectFromBucket(ctx, key, filePath, 0, 0, false)
		if err == nil {
			body.Close()
		}
	}

	return info, nil
}

func (b *cachedBucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return nil, storage.ObjectInfo{}, err
	}

	getOptions := storage.NewGetOptions(options...)
	start, end, hasBytesRange := getOptions.BytesRange()
	if hasBytesRange {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			return nil, storage.ObjectInfo{}, err
		}
	}

	var cachedFileInfo os.FileInfo
	var cachedInfo storage.ObjectInfo
	var headInfo storage.ObjectInfo
	var valid bool
	var body io.ReadCloser

	filePath := b.makeFilePath(key)
	cachedFile, err := os.Open(filePath)
	if err != nil {
		goto headObject
	}
	defer func() {
		if cachedFile != nil {
			cachedFile.Close()
		}
	}()

	cachedInfo, _, err = readObjectInfo(cachedFile)
	if err != nil {
		goto headObject
	}

	// Check if the cached object has expired
	cachedFileInfo, err = cachedFile.Stat()
	if err != nil || isObjectExpired(cachedInfo, cachedFileInfo) {
		// Object has expired, remove it from cache
		b.delete(filePath)
		os.Remove(filePath)
		goto headObject
	}

	// Check if the cached object needs revalidation
	valid = !needsRevalidation(cachedInfo)
	if !valid {
		// If backend is unreachable or object didn't change, serve from cache
		headInfo, err = b.bucket.HeadObject(ctx, key)
		valid = err != nil || headInfo.ETag == cachedInfo.ETag
		if !valid {
			goto getObject // we already have the file info
		}
	}

	defer func() {
		cachedFile = nil
	}()

	body = io.ReadCloser(cachedFile)
	if hasBytesRange {
		effEnd := end
		if effEnd < 0 {
			effEnd = cachedInfo.Size - 1
		}
		if start >= cachedInfo.Size {
			body = emptyBodyClosing(cachedFile)
		} else {
			body = bytesRangeReadCloser(cachedFile, start, effEnd)
		}
	}
	b.lookup(cachedFile.Name())
	return body, cachedInfo, nil

headObject:
	headInfo, err = b.bucket.HeadObject(ctx, key)
	if err != nil {
		return nil, headInfo, err
	}
	if !shouldCacheObject(headInfo) {
		return b.bucket.GetObject(ctx, key, options...)
	}
getObject:
	return b.getObjectFromBucket(ctx, key, filePath, start, end, hasBytesRange)
}

func (b *cachedBucket) getObjectFromBucket(ctx context.Context, key, filePath string, start, end int64, hasBytesRange bool) (io.ReadCloser, storage.ObjectInfo, error) {
	body, info, err := b.bucket.GetObject(ctx, key)
	if err != nil {
		return nil, storage.ObjectInfo{}, err
	}

	dirPath := filepath.Dir(filePath)
	if err = os.Mkdir(dirPath, 0755); err != nil {
		if errors.Is(err, fs.ErrExist) {
			err = nil
		}
	}
	var f *os.File
	if err == nil {
		f, err = os.CreateTemp(dirPath, tempFilePattern)
	}
	if err != nil {
		if hasBytesRange {
			effEnd := end
			if effEnd < 0 {
				effEnd = info.Size - 1
			}
			io.CopyN(io.Discard, body, start)
			limit := effEnd - start + 1
			if limit < 0 {
				limit = 0
			}
			body = &struct {
				io.LimitedReader
				io.Closer
			}{
				LimitedReader: io.LimitedReader{R: body, N: limit},
				Closer:        body,
			}
		}
		return body, info, nil
	}

	closeFile := true
	defer body.Close()
	defer func() {
		if closeFile {
			f.Close()
			os.Remove(f.Name())
		} else {
			os.Rename(f.Name(), filePath)
		}
	}()

	if _, err := io.Copy(f, body); err != nil {
		b.log.WarnContext(ctx, "cache: failed to write object", "key", key, "error", err)
		if isErrNoSpace(err) {
			// Trigger emergency eviction to free disk space for future requests
			b.evictForSpace(info.Size * 2)
		}
		// Re-fetch from backend without caching (graceful degradation)
		// The original body is now consumed/corrupted, so we need a fresh fetch
		return b.bucket.GetObject(ctx, key)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		b.log.WarnContext(ctx, "cache: failed to seek", "key", key, "error", err)
		return b.bucket.GetObject(ctx, key)
	}
	if err := os.Chtimes(f.Name(), info.LastModified, info.LastModified); err != nil {
		b.log.WarnContext(ctx, "cache: failed to set mtime", "key", key, "error", err)
		return b.bucket.GetObject(ctx, key)
	}
	if err := writeObjectInfo(f, info); err != nil {
		b.log.WarnContext(ctx, "cache: failed to write metadata", "key", key, "error", err)
		return b.bucket.GetObject(ctx, key)
	}

	body = io.ReadCloser(f)
	if hasBytesRange {
		// info.Size can be smaller than the bytes we just wrote (the
		// gs transcoded case: stored compressed length + decompressed
		// body). Use the on-disk size as the authoritative clamp to
		// avoid truncating tail reads.
		fi, err := f.Stat()
		if err != nil {
			return nil, info, err
		}
		fileSize := fi.Size()
		effEnd := end
		if effEnd < 0 || effEnd >= fileSize {
			effEnd = fileSize - 1
		}
		if start >= fileSize {
			body = emptyBodyClosing(f)
		} else {
			body = bytesRangeReadCloser(f, start, effEnd)
		}
	}

	b.lookup(f.Name())
	closeFile = false
	return body, info, nil
}

func (b *cachedBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	filePath := b.makeFilePath(key)
	dirPath := filepath.Dir(filePath)
	tmpFile := (*os.File)(nil)
	renameFile := false

	if err := os.Mkdir(dirPath, 0755); err == nil || errors.Is(err, fs.ErrExist) {
		tmpFile, err = os.CreateTemp(dirPath, tempFilePattern)
		if err == nil {
			defer func() {
				if tmpFile != nil {
					tmpFile.Close()
					if renameFile {
						os.Rename(tmpFile.Name(), filePath)
					} else {
						os.Remove(tmpFile.Name())
					}
				}
			}()

			if _, err := io.Copy(tmpFile, value); err != nil {
				b.log.WarnContext(ctx, "cache: failed to buffer object", "key", key, "error", err)
				// Get how much was written before the error
				written, _ := tmpFile.Seek(0, io.SeekCurrent)
				if isErrNoSpace(err) {
					// Trigger emergency eviction to free disk space for future requests
					b.evictForSpace(written * 2)
				}
				// Seek back to start of temp file to recover data already written
				if _, seekErr := tmpFile.Seek(0, io.SeekStart); seekErr == nil {
					// Chain: data in temp file + remaining data in value
					// This allows the backend upload to succeed without caching
					// Note: we keep tmpFile open so the defer will close it after putObject
					recoveryFile := tmpFile
					value = io.MultiReader(recoveryFile, value)
					tmpFile = nil // Skip caching, but keep recoveryFile for cleanup
					defer recoveryFile.Close()
					defer os.Remove(recoveryFile.Name())
					goto putObject
				}
				// Can't recover - skip caching entirely
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				tmpFile = nil
				goto putObject
			}
			if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
				b.log.WarnContext(ctx, "cache: failed to seek", "key", key, "error", err)
				// Can't recover - skip caching entirely
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				tmpFile = nil
				goto putObject
			}

			value = tmpFile
		}
	}

putObject:
	info, err := b.bucket.PutObject(ctx, key, value, options...)
	if err != nil {
		return info, err
	}

	// Only cache the object if it should be cached based on cache control headers
	if tmpFile != nil && shouldCacheObject(info) {
		if err := os.Chtimes(tmpFile.Name(), info.LastModified, info.LastModified); err != nil {
			b.log.WarnContext(ctx, "cache: failed to set mtime", "key", key, "error", err)
			return info, nil
		}
		if err := writeObjectInfo(tmpFile, info); err != nil {
			b.log.WarnContext(ctx, "cache: failed to write metadata", "key", key, "error", err)
			return info, nil
		}
		b.insert(filePath, info.Size)
		renameFile = true
	}
	return info, nil
}

func (b *cachedBucket) DeleteObject(ctx context.Context, key string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return err
	}
	filePath := b.makeFilePath(key)
	b.delete(filePath)
	os.Remove(filePath)
	return b.bucket.DeleteObject(ctx, key)
}

func (b *cachedBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return b.bucket.DeleteObjects(ctx, func(yield func(string, error) bool) {
		for key, err := range objects {
			err = cmp.Or(err, context.Cause(ctx), storage.ValidObjectKey(key))

			if err == nil {
				filePath := b.makeFilePath(key)
				b.delete(filePath)
				os.Remove(filePath)
			}

			if !yield(key, err) {
				return
			}
		}
	})
}

func (b *cachedBucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	// Invalidate destination cache entry
	filePath := b.makeFilePath(to)
	b.delete(filePath)
	os.Remove(filePath)
	return b.bucket.CopyObject(ctx, from, to, options...)
}

func (b *cachedBucket) ListObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return b.bucket.ListObjects(ctx, options...)
}

func (b *cachedBucket) WatchObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return b.bucket.WatchObjects(ctx, options...)
}

func (b *cachedBucket) makeFilePath(key string) string {
	sha := sha256.Sum256([]byte(key))
	return filepath.Join(b.tmpdir, fmt.Sprintf("%01X/%064x", sha[0:1], sha))
}

func (b *cachedBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	return b.bucket.PresignGetObject(ctx, key, expiration, options...)
}

func (b *cachedBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...storage.PutOption) (string, error) {
	return b.bucket.PresignPutObject(ctx, key, expiration, options...)
}

func (b *cachedBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return b.bucket.PresignHeadObject(ctx, key, expiration)
}

func (b *cachedBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return b.bucket.PresignDeleteObject(ctx, key, expiration)
}
