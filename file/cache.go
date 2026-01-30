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
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/EmissarySocial/emissary/tools/cacheheader"
	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/cache/lru"
)

type rangeKey struct {
	filePath string
	start    int64
	end      int64
}

type rangeInfo struct {
	diskUsage int64
}

type Cache struct {
	tmpdir string
	size   int64
	init   bool
	once   sync.Once
	mutex  sync.Mutex
	items  lru.LRU[rangeKey, rangeInfo]
	files  map[string]map[rangeKey]struct{} // secondary index for efficient file-level deletion
}

func NewCache(tmpdir string, size int64) *Cache {
	return &Cache{tmpdir: tmpdir, size: size}
}

func shouldCacheObject(info storage.ObjectInfo) bool {
	if info.CacheControl == "" {
		return false
	}
	cc := cacheheader.ParseString(info.CacheControl)
	if cc.Private || cc.NoStore {
		return false
	}
	return cc.MaxAge > 0 || cc.Immutable || cc.NoCache || cc.MustRevalidate
}

func isObjectExpired(info storage.ObjectInfo, fileInfo os.FileInfo) bool {
	if info.CacheControl == "" {
		return true
	}
	cc := cacheheader.ParseString(info.CacheControl)
	if cc.Immutable {
		return false
	}
	if cc.MaxAge <= 0 {
		return false
	}
	age := time.Since(fileInfo.ModTime())
	maxAge := time.Duration(cc.MaxAge) * time.Second
	return age > maxAge
}

func needsRevalidation(info storage.ObjectInfo) bool {
	if info.CacheControl == "" {
		return false
	}
	cc := cacheheader.ParseString(info.CacheControl)
	return cc.NoCache || cc.MustRevalidate
}

func blockAlign(start, end int64) (alignedStart, alignedEnd int64) {
	alignedStart = (start / sparseBlockSize) * sparseBlockSize
	alignedEnd = ((end + sparseBlockSize) / sparseBlockSize) * sparseBlockSize
	return
}

func isRangeCached(f *os.File, start, end int64) (bool, error) {
	pos := start
	for pos <= end {
		dataStart, err := seekData(f, pos)
		if err != nil {
			return false, nil
		}
		if dataStart > pos {
			return false, nil
		}
		holeStart, err := seekHole(f, dataStart)
		if err != nil {
			return true, nil
		}
		if holeStart > end {
			return true, nil
		}
		pos = holeStart
	}
	return true, nil
}

func uncachedRanges(f *os.File, start, end int64) ([]struct{ Start, End int64 }, error) {
	var uncached []struct{ Start, End int64 }
	pos := start
	for pos <= end {
		holeStart, err := seekHole(f, pos)
		if err != nil || holeStart >= end {
			break
		}
		dataStart, err := seekData(f, holeStart)
		if err != nil {
			dataStart = end + 1
		}
		holeEnd := min(dataStart-1, end)
		if holeStart < holeEnd {
			uncached = append(uncached, struct{ Start, End int64 }{holeStart, holeEnd})
		}
		pos = dataStart
	}
	return uncached, nil
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

		filepath.Walk(c.tmpdir, func(path string, fileInfo os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if !fileInfo.IsDir() {
				f, err := os.Open(path)
				if err != nil {
					return nil
				}
				usage, err := diskUsage(f)
				f.Close()
				logicalSize := fileInfo.Size()
				if err != nil || usage == 0 || usage > logicalSize {
					usage = logicalSize
				}
				c.insertFile(path, logicalSize, usage)
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

// must be called with mutex held
func (c *Cache) removeFromSecondaryIndex(key rangeKey) bool {
	if ranges, ok := c.files[key.filePath]; ok {
		delete(ranges, key)
		if len(ranges) == 0 {
			delete(c.files, key.filePath)
			return true
		}
	}
	return false
}

func (c *Cache) deleteFile(filePath string) {
	c.mutex.Lock()
	ranges := c.files[filePath]
	if ranges != nil {
		for key := range ranges {
			c.items.Delete(key)
		}
		delete(c.files, filePath)
	}
	c.mutex.Unlock()

	os.Remove(filePath)
}

func (c *Cache) lookupFile(filePath string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if ranges, ok := c.files[filePath]; ok {
		for key := range ranges {
			c.items.Lookup(key)
		}
	}
}

func (c *Cache) insertFile(filePath string, size int64, usage int64) {
	key := rangeKey{filePath: filePath, start: 0, end: size}
	c.insertRange(key, usage)
}

func (c *Cache) insertRange(key rangeKey, usage int64) {
	var evictedRanges []rangeKey

	defer func() {
		// outside the lock
		for _, evictedKey := range evictedRanges {
			c.punchEvictedRange(evictedKey)
		}
	}()

	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.files == nil {
		c.files = make(map[string]map[rangeKey]struct{})
	}

	if c.items.Insert(key, rangeInfo{diskUsage: usage}, usage) {
		if c.files[key.filePath] == nil {
			c.files[key.filePath] = make(map[rangeKey]struct{})
		}
		c.files[key.filePath][key] = struct{}{}

		for c.items.Size > c.size {
			evictedKey, _, _, hasEvicted := c.items.Evict()
			if !hasEvicted {
				break
			}
			evictedRanges = append(evictedRanges, evictedKey)
			c.removeFromSecondaryIndex(evictedKey)
		}
	}
}

func (c *Cache) punchEvictedRange(key rangeKey) {
	f, err := os.OpenFile(key.filePath, os.O_RDWR, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	length := key.end - key.start
	if err := punchHole(f, key.start, length); err != nil {
		return
	}

	c.mutex.Lock()
	isEmpty := len(c.files[key.filePath]) == 0
	c.mutex.Unlock()

	if isEmpty {
		os.Remove(key.filePath)
	}
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
		if info, err := readObjectInfo(f); err == nil {
			if fileInfo, err := f.Stat(); err == nil && !isObjectExpired(info, fileInfo) {
				b.lookupFile(f.Name())
				return info, nil
			} else {
				b.deleteFile(filePath)
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

	cachedInfo, err = readObjectInfo(cachedFile)
	if err != nil {
		goto headObject
	}

	cachedFileInfo, err = cachedFile.Stat()
	if err != nil || isObjectExpired(cachedInfo, cachedFileInfo) {
		b.deleteFile(filePath)
		goto headObject
	}

	valid = !needsRevalidation(cachedInfo)
	if !valid {
		headInfo, err = b.bucket.HeadObject(ctx, key)
		valid = err != nil || headInfo.ETag == cachedInfo.ETag
		if !valid {
			cachedFile.Close()
			cachedFile = nil
			b.deleteFile(filePath)
			goto getObject
		}
	}

	if hasBytesRange {
		objectSize := cachedFileInfo.Size()
		alignedStart, alignedEnd := blockAlign(start, min(end, objectSize-1))
		cached, _ := isRangeCached(cachedFile, alignedStart, min(alignedEnd, objectSize))
		if !cached {
			cachedFile.Close()
			cachedFile = nil
			goto fetchRange
		}
	}

	defer func() {
		cachedFile = nil
	}()

	body = io.ReadCloser(cachedFile)
	if hasBytesRange {
		body = bytesRangeReadCloser(cachedFile, start, end)
	}
	b.lookupFile(cachedFile.Name())
	return body, cachedInfo, nil

fetchRange:
	return b.fetchRangeToCache(ctx, key, filePath, start, end, cachedInfo)

headObject:
	headInfo, err = b.bucket.HeadObject(ctx, key)
	if err != nil {
		return nil, headInfo, err
	}
	if !shouldCacheObject(headInfo) {
		return b.bucket.GetObject(ctx, key, options...)
	}
getObject:
	if hasBytesRange {
		return b.fetchRangeToCache(ctx, key, filePath, start, end, headInfo)
	}
	return b.getObjectFromBucket(ctx, key, filePath, start, end, hasBytesRange)
}

func (b *cachedBucket) fetchRangeToCache(ctx context.Context, key, filePath string, start, end int64, info storage.ObjectInfo) (io.ReadCloser, storage.ObjectInfo, error) {
	objectSize := info.Size
	alignedStart, alignedEnd := blockAlign(start, min(end, objectSize-1))
	if alignedEnd > objectSize {
		alignedEnd = objectSize
	}

	dirPath := filepath.Dir(filePath)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return b.bucket.GetObject(ctx, key, storage.BytesRange(start, end))
	}

	f, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		f, err = os.Create(filePath)
		if err != nil {
			return b.bucket.GetObject(ctx, key, storage.BytesRange(start, end))
		}
		if err := f.Truncate(objectSize); err != nil {
			f.Close()
			os.Remove(filePath)
			return b.bucket.GetObject(ctx, key, storage.BytesRange(start, end))
		}
		if err := writeObjectInfo(f, info); err != nil {
			f.Close()
			os.Remove(filePath)
			return b.bucket.GetObject(ctx, key, storage.BytesRange(start, end))
		}
	}

	closeFile := true
	defer func() {
		if closeFile {
			f.Close()
		}
	}()

	uncached, err := uncachedRanges(f, alignedStart, alignedEnd)
	if err != nil {
		return b.bucket.GetObject(ctx, key, storage.BytesRange(start, end))
	}

	for _, r := range uncached {
		rangeEnd := min(r.End, objectSize-1)
		body, _, err := b.bucket.GetObject(ctx, key, storage.BytesRange(r.Start, rangeEnd))
		if err != nil {
			return nil, info, err
		}
		data, err := io.ReadAll(body)
		body.Close()
		if err != nil {
			return nil, info, err
		}
		if _, err := f.WriteAt(data, r.Start); err != nil {
			return nil, info, err
		}
		rangeUsage := int64(len(data))
		rKey := rangeKey{filePath: filePath, start: r.Start, end: rangeEnd + 1}
		b.insertRange(rKey, rangeUsage)
	}

	f.Sync()
	closeFile = false
	return bytesRangeReadCloser(f, start, end), info, nil
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
			io.CopyN(io.Discard, body, start)
			body = &struct {
				io.LimitedReader
				io.Closer
			}{
				LimitedReader: io.LimitedReader{R: body, N: end - start + 1},
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
		return nil, info, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, info, err
	}
	if err := os.Chtimes(f.Name(), info.LastModified, info.LastModified); err != nil {
		return nil, info, err
	}
	if err := writeObjectInfo(f, info); err != nil {
		return nil, info, err
	}

	body = io.ReadCloser(f)
	if hasBytesRange {
		body = bytesRangeReadCloser(f, start, end)
	}

	f.Sync()
	usage, err := diskUsage(f)
	if err != nil || usage == 0 || usage > info.Size {
		usage = info.Size
	}
	b.insertFile(filePath, info.Size, usage)
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
				tmpFile.Close()
				if renameFile {
					os.Rename(tmpFile.Name(), filePath)
				} else {
					os.Remove(tmpFile.Name())
				}
			}()

			if _, err := io.Copy(tmpFile, value); err != nil {
				return storage.ObjectInfo{}, err
			}
			if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
				return storage.ObjectInfo{}, err
			}

			value = tmpFile
		}
	}

	info, err := b.bucket.PutObject(ctx, key, value, options...)
	if err != nil {
		return info, err
	}

	if tmpFile != nil && shouldCacheObject(info) {
		if err := os.Chtimes(tmpFile.Name(), info.LastModified, info.LastModified); err != nil {
			return info, nil
		}
		if err := writeObjectInfo(tmpFile, info); err != nil {
			return info, nil
		}
		tmpFile.Sync()
		usage, err := diskUsage(tmpFile)
		if err != nil || usage == 0 || usage > info.Size {
			usage = info.Size
		}
		b.insertFile(filePath, info.Size, usage)
		renameFile = true
	}
	return info, err
}

func (b *cachedBucket) DeleteObject(ctx context.Context, key string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return err
	}
	filePath := b.makeFilePath(key)
	b.deleteFile(filePath)
	return b.bucket.DeleteObject(ctx, key)
}

func (b *cachedBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return b.bucket.DeleteObjects(ctx, func(yield func(string, error) bool) {
		for key, err := range objects {
			err = cmp.Or(err, context.Cause(ctx), storage.ValidObjectKey(key))

			if err == nil {
				filePath := b.makeFilePath(key)
				b.deleteFile(filePath)
			}

			if !yield(key, err) {
				return
			}
		}
	})
}

func (b *cachedBucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	b.deleteFile(b.makeFilePath(to))
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

func (b *cachedBucket) PresignHeadObject(ctx context.Context, key string) (string, error) {
	return b.bucket.PresignHeadObject(ctx, key)
}

func (b *cachedBucket) PresignDeleteObject(ctx context.Context, key string) (string, error) {
	return b.bucket.PresignDeleteObject(ctx, key)
}
