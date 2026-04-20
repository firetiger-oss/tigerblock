package file

import (
	"context"
	"io"
	"math"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/test"
)

func TestCacheWithLimit(t *testing.T) {
	test.TestStorage(t, func(t *testing.T) (storage.Bucket, error) {
		store := t.TempDir()
		cache := t.TempDir()

		bucket, err := NewRegistry(store).LoadBucket(t.Context(), "")
		if err != nil {
			return nil, err
		}

		return NewCache(cache, 16).AdaptBucket(bucket), nil
	})
}

func TestCacheWithoutLimit(t *testing.T) {
	test.TestStorage(t, func(t *testing.T) (storage.Bucket, error) {
		store := t.TempDir()
		cache := t.TempDir()

		bucket, err := NewRegistry(store).LoadBucket(t.Context(), "")
		if err != nil {
			return nil, err
		}

		return NewCache(cache, math.MaxInt64).AdaptBucket(bucket), nil
	})
}

func TestCacheReuse(t *testing.T) {
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	// Create larger objects to ensure evictions will happen
	const key1 = "cached-object-1"
	const key2 = "cached-object-2"
	const key3 = "cached-object-3"
	const key4 = "cached-object-4"
	const key5 = "cached-object-5"

	// Create content that's large enough to trigger evictions (each ~100 bytes)
	value1 := strings.Repeat("first cached content with lots of data to make it larger ", 2)
	value2 := strings.Repeat("second cached content with even more data to fill cache ", 2)
	value3 := strings.Repeat("third cached content that will definitely cause evictions ", 2)
	value4 := strings.Repeat("fourth cached content for testing eviction behavior nicely ", 2)
	value5 := strings.Repeat("fifth cached content to really push the cache limits hard ", 2)

	// Step 1: Create first cache with large size and populate it with multiple objects
	cacheInstance1 := NewCache(cacheDir, 2048) // Large cache to fit all objects initially
	bucket1, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create first bucket:", err)
	}
	cache1 := cacheInstance1.AdaptBucket(bucket1)

	// Add multiple objects through first cache with proper cache control headers
	obj1, err := cache1.PutObject(ctx, key1, strings.NewReader(value1), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put first object through first cache:", err)
	}

	obj2, err := cache1.PutObject(ctx, key2, strings.NewReader(value2), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put second object through first cache:", err)
	}

	_, err = cache1.PutObject(ctx, key3, strings.NewReader(value3), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put third object through first cache:", err)
	}

	_, err = cache1.PutObject(ctx, key4, strings.NewReader(value4), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put fourth object through first cache:", err)
	}

	// Access objects through first cache to ensure they're cached
	r1, _, err := cache1.GetObject(ctx, key1)
	if err != nil {
		t.Fatal("failed to get first object through first cache:", err)
	}
	r1.Close()

	r2, _, err := cache1.GetObject(ctx, key2)
	if err != nil {
		t.Fatal("failed to get second object through first cache:", err)
	}
	r2.Close()

	r3, _, err := cache1.GetObject(ctx, key3)
	if err != nil {
		t.Fatal("failed to get third object through first cache:", err)
	}
	r3.Close()

	r4, _, err := cache1.GetObject(ctx, key4)
	if err != nil {
		t.Fatal("failed to get fourth object through first cache:", err)
	}
	r4.Close()

	// Check initial cache statistics
	stat1Initial := cacheInstance1.Stat()
	t.Logf("Cache 1 initial stats: Size=%d, Hits=%d, Misses=%d, Evictions=%d",
		stat1Initial.Size, stat1Initial.Hits, stat1Initial.Misses, stat1Initial.Evictions)

	// Step 2: Create second cache with MUCH smaller size (exercises rehydration + eviction)
	cacheInstance2 := NewCache(cacheDir, 200) // Small cache that can only fit ~2 objects
	bucket2, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create second bucket:", err)
	}
	cache2 := cacheInstance2.AdaptBucket(bucket2)

	// Step 3: Put a new object through the small cache to trigger evictions
	obj5, err := cache2.PutObject(ctx, key5, strings.NewReader(value5), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put fifth object through second cache:", err)
	}

	// Step 4: Access objects through second cache - this should cause evictions
	r1_cache2, obj1_cache2, err := cache2.GetObject(ctx, key1)
	if err != nil {
		t.Fatal("failed to get first object through second cache:", err)
	}
	defer r1_cache2.Close()

	data1, err := io.ReadAll(r1_cache2)
	if err != nil {
		t.Fatal("failed to read first object data from second cache:", err)
	}

	if string(data1) != value1 {
		t.Errorf("first object data mismatch: %q != %q", string(data1), value1)
	}

	if obj1_cache2.ETag != obj1.ETag {
		t.Errorf("first object ETags don't match: %q != %q", obj1_cache2.ETag, obj1.ETag)
	}

	if obj1_cache2.Size != obj1.Size {
		t.Errorf("first object sizes don't match: %d != %d", obj1_cache2.Size, obj1.Size)
	}

	// Access more objects to trigger more evictions
	r2_cache2, obj2_cache2, err := cache2.GetObject(ctx, key2)
	if err != nil {
		t.Fatal("failed to get second object through second cache:", err)
	}
	defer r2_cache2.Close()

	data2, err := io.ReadAll(r2_cache2)
	if err != nil {
		t.Fatal("failed to read second object data from second cache:", err)
	}

	if string(data2) != value2 {
		t.Errorf("second object data mismatch: %q != %q", string(data2), value2)
	}

	if obj2_cache2.ETag != obj2.ETag {
		t.Errorf("second object ETags don't match: %q != %q", obj2_cache2.ETag, obj2.ETag)
	}

	if obj2_cache2.Size != obj2.Size {
		t.Errorf("second object sizes don't match: %d != %d", obj2_cache2.Size, obj2.Size)
	}

	// Access the fifth object through second cache
	r5_cache2, obj5_cache2, err := cache2.GetObject(ctx, key5)
	if err != nil {
		t.Fatal("failed to get fifth object through second cache:", err)
	}
	defer r5_cache2.Close()

	data5, err := io.ReadAll(r5_cache2)
	if err != nil {
		t.Fatal("failed to read fifth object data from second cache:", err)
	}

	if string(data5) != value5 {
		t.Errorf("fifth object data mismatch: %q != %q", string(data5), value5)
	}

	if obj5_cache2.ETag != obj5.ETag {
		t.Errorf("fifth object ETags don't match: %q != %q", obj5_cache2.ETag, obj5.ETag)
	}

	if obj5_cache2.Size != obj5.Size {
		t.Errorf("fifth object sizes don't match: %d != %d", obj5_cache2.Size, obj5.Size)
	}

	// Step 5: Test cache statistics and validate evictions occurred
	stat1Final := cacheInstance1.Stat()
	stat2Final := cacheInstance2.Stat()

	// Log final cache statistics
	t.Logf("Cache 1 final stats: Size=%d, Hits=%d, Misses=%d, Evictions=%d",
		stat1Final.Size, stat1Final.Hits, stat1Final.Misses, stat1Final.Evictions)
	t.Logf("Cache 2 final stats: Size=%d, Hits=%d, Misses=%d, Evictions=%d",
		stat2Final.Size, stat2Final.Hits, stat2Final.Misses, stat2Final.Evictions)

	// Verify the limits are what we set
	expectedLimit1 := int64(2048)
	expectedLimit2 := int64(200)
	if stat1Final.Limit != expectedLimit1 {
		t.Errorf("unexpected cache 1 limit: %d != %d", stat1Final.Limit, expectedLimit1)
	}
	if stat2Final.Limit != expectedLimit2 {
		t.Errorf("unexpected cache 2 limit: %d != %d", stat2Final.Limit, expectedLimit2)
	}

	// Validate that the second cache had evictions due to its small size
	if stat2Final.Evictions == 0 {
		t.Error("expected evictions in second cache due to small size, but got 0")
	}

	// Validate that second cache size is within its limit
	if stat2Final.Size > stat2Final.Limit {
		t.Errorf("cache 2 size (%d) exceeds its limit (%d)", stat2Final.Size, stat2Final.Limit)
	}

	// Step 6: Verify both caches can still access objects (rehydration still works)
	r1_again, _, err := cache1.GetObject(ctx, key1)
	if err != nil {
		t.Fatal("failed to re-access first object through first cache:", err)
	}
	defer r1_again.Close()

	data1_again, err := io.ReadAll(r1_again)
	if err != nil {
		t.Fatal("failed to read first object data again from first cache:", err)
	}

	if string(data1_again) != value1 {
		t.Errorf("first cache data corrupted: %q != %q", string(data1_again), value1)
	}

	// Validate statistics are non-negative
	if stat1Final.Limit < 0 || stat2Final.Limit < 0 {
		t.Error("cache limits should be non-negative")
	}
	if stat1Final.Size < 0 || stat2Final.Size < 0 {
		t.Error("cache sizes should be non-negative")
	}
	if stat1Final.Hits < 0 || stat2Final.Hits < 0 {
		t.Error("cache hits should be non-negative")
	}
	if stat1Final.Misses < 0 || stat2Final.Misses < 0 {
		t.Error("cache misses should be non-negative")
	}
	if stat1Final.Evictions < 0 || stat2Final.Evictions < 0 {
		t.Error("cache evictions should be non-negative")
	}
}

func TestCacheControlRespect(t *testing.T) {
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Test 1: Object with no cache control should not be cached
	key1 := "no-cache-control"
	value1 := "content without cache control"
	_, err = cachedBucket.PutObject(ctx, key1, strings.NewReader(value1))
	if err != nil {
		t.Fatal("failed to put object without cache control:", err)
	}

	// Verify cache statistics don't show any growth yet since objects without proper cache control shouldn't be cached
	stat1 := cache.Stat()
	if stat1.Size > 0 {
		t.Error("cache should be empty since no objects with proper cache control have been added yet")
	}

	// Test 2: Object with private cache control should not be cached
	key2 := "private-object"
	value2 := "private content"
	_, err = cachedBucket.PutObject(ctx, key2, strings.NewReader(value2), storage.CacheControl("private, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put private object:", err)
	}

	// Test 3: Object with no-cache should now be cached (for revalidation)
	key3 := "no-cache-object"
	value3 := "no cache content"
	_, err = cachedBucket.PutObject(ctx, key3, strings.NewReader(value3), storage.CacheControl("no-cache"))
	if err != nil {
		t.Fatal("failed to put no-cache object:", err)
	}

	// Test 4: Object with no-store should not be cached
	key4 := "no-store-object"
	value4 := "no store content"
	_, err = cachedBucket.PutObject(ctx, key4, strings.NewReader(value4), storage.CacheControl("no-store"))
	if err != nil {
		t.Fatal("failed to put no-store object:", err)
	}

	// Test 4b: Object with must-revalidate should now be cached (for revalidation)
	key4b := "must-revalidate-object"
	value4b := "must revalidate content"
	_, err = cachedBucket.PutObject(ctx, key4b, strings.NewReader(value4b), storage.CacheControl("public, max-age=3600, must-revalidate"))
	if err != nil {
		t.Fatal("failed to put must-revalidate object:", err)
	}

	// Test 5: Object with max-age should be cached
	key5 := "max-age-object"
	value5 := "max age content"
	_, err = cachedBucket.PutObject(ctx, key5, strings.NewReader(value5), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put max-age object:", err)
	}

	// Test 6: Immutable object should be cached
	key6 := "immutable-object"
	value6 := "immutable content"
	_, err = cachedBucket.PutObject(ctx, key6, strings.NewReader(value6), storage.CacheControl("public, immutable"))
	if err != nil {
		t.Fatal("failed to put immutable object:", err)
	}

	// Verify cache statistics show the cacheable objects
	stat := cache.Stat()
	t.Logf("Cache stats after putting objects: Size=%d, Hits=%d, Misses=%d, Evictions=%d",
		stat.Size, stat.Hits, stat.Misses, stat.Evictions)

	// The cache should now contain objects 3, 4b, 5, and 6 (no-cache, must-revalidate, max-age, immutable)
	// Object 4 (no-store) should still not be cached
	// Note: we can't check exact size since different objects may have different sizes
	// but we can verify that some objects are cached
	if stat.Size == 0 {
		t.Error("expected some objects to be cached (those with proper cache control)")
	}
}

func TestCacheExpiration(t *testing.T) {
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Test 1: Put an object with a very short max-age (1 second)
	key1 := "short-lived-object"
	value1 := "content that expires quickly"
	_, err = cachedBucket.PutObject(ctx, key1, strings.NewReader(value1), storage.CacheControl("public, max-age=1"))
	if err != nil {
		t.Fatal("failed to put short-lived object:", err)
	}

	// Verify it's initially cached
	stat1 := cache.Stat()
	if stat1.Size == 0 {
		t.Error("object should be initially cached")
	}

	// Access the object immediately - should hit cache
	r1, _, err := cachedBucket.GetObject(ctx, key1)
	if err != nil {
		t.Fatal("failed to get object:", err)
	}
	r1.Close()

	// Wait for expiration (2 seconds to be safe)
	time.Sleep(2 * time.Second)

	// Access the object again - should be expired and fetched from underlying storage
	r2, _, err := cachedBucket.GetObject(ctx, key1)
	if err != nil {
		t.Fatal("failed to get expired object:", err)
	}
	r2.Close()

	// Test 2: Put an immutable object - should never expire
	key2 := "immutable-object"
	value2 := "immutable content"
	_, err = cachedBucket.PutObject(ctx, key2, strings.NewReader(value2), storage.CacheControl("public, immutable"))
	if err != nil {
		t.Fatal("failed to put immutable object:", err)
	}

	// Wait a bit and verify it's still cached
	time.Sleep(1 * time.Second)
	r3, _, err := cachedBucket.GetObject(ctx, key2)
	if err != nil {
		t.Fatal("failed to get immutable object:", err)
	}
	r3.Close()

	t.Logf("Cache expiration test completed successfully")
}

func TestCacheRevalidation(t *testing.T) {
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Test 1: Put an object with no-cache directive - should be cached but always revalidated
	key1 := "no-cache-object"
	value1 := "no cache content"
	obj1, err := cachedBucket.PutObject(ctx, key1, strings.NewReader(value1), storage.CacheControl("no-cache"))
	if err != nil {
		t.Fatal("failed to put no-cache object:", err)
	}

	// Verify it's cached
	stat1 := cache.Stat()
	if stat1.Size == 0 {
		t.Error("no-cache object should be cached for revalidation")
	}

	// Get the object - this should trigger revalidation via HeadObject
	r1, info1, err := cachedBucket.GetObject(ctx, key1)
	if err != nil {
		t.Fatal("failed to get no-cache object:", err)
	}
	r1.Close()

	if info1.ETag != obj1.ETag {
		t.Error("ETag should match after revalidation")
	}

	// Test 2: Put an object with must-revalidate directive
	key2 := "must-revalidate-object"
	value2 := "must revalidate content"
	obj2, err := cachedBucket.PutObject(ctx, key2, strings.NewReader(value2), storage.CacheControl("public, max-age=3600, must-revalidate"))
	if err != nil {
		t.Fatal("failed to put must-revalidate object:", err)
	}

	// Get the object - this should trigger revalidation
	r2, info2, err := cachedBucket.GetObject(ctx, key2)
	if err != nil {
		t.Fatal("failed to get must-revalidate object:", err)
	}
	r2.Close()

	if info2.ETag != obj2.ETag {
		t.Error("ETag should match after revalidation")
	}

	// Test 3: Simulate ETag change by updating the object directly in the underlying storage
	// This will cause the cache to miss on revalidation and fetch fresh content
	newValue1 := "updated no cache content"
	newObj1, err := bucket.PutObject(ctx, key1, strings.NewReader(newValue1), storage.CacheControl("no-cache"))
	if err != nil {
		t.Fatal("failed to update object in underlying storage:", err)
	}

	// Now get the object through cache - should detect ETag change and fetch fresh
	r3, info3, err := cachedBucket.GetObject(ctx, key1)
	if err != nil {
		t.Fatal("failed to get updated object:", err)
	}
	defer r3.Close()

	// Verify we got the updated content
	data3, err := io.ReadAll(r3)
	if err != nil {
		t.Fatal("failed to read updated object data:", err)
	}

	if string(data3) != newValue1 {
		t.Errorf("expected updated content %q, got %q", newValue1, string(data3))
	}

	if info3.ETag == obj1.ETag {
		t.Error("ETag should have changed after object update")
	}

	if info3.ETag != newObj1.ETag {
		t.Error("ETag should match the updated object")
	}

	t.Logf("Cache revalidation test completed successfully")
}

// TestCacheTailReadPastEndDoesNotLeakFDs verifies that calling GetObject
// with BytesRange(start, -1) where start >= object size — a valid
// past-EOF tail read — closes the underlying cache file when the
// returned reader is closed. A prior revision returned a
// io.NopCloser(strings.NewReader("")) that silently dropped the open
// *os.File, so repeated past-end reads (common in File.ReadAt and the
// FUSE adapter) would exhaust the process file descriptor limit.
//
// The test temporarily lowers the process's open-file soft limit so a
// leak manifests as a deterministic "too many open files" error on any
// Unix host, independent of whatever the user's default ulimit is.
func TestCacheTailReadPastEndDoesNotLeakFDs(t *testing.T) {
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	const key = "test-object"
	const value = "hello, world!"

	underlying, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := underlying.PutObject(ctx, key, strings.NewReader(value), storage.CacheControl("max-age=3600")); err != nil {
		t.Fatal(err)
	}
	bucket := NewCache(cacheDir, math.MaxInt64).AdaptBucket(underlying)

	// Prime the cache so subsequent reads hit the cache-hit path.
	r, _, err := bucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(io.Discard, r); err != nil {
		t.Fatal(err)
	}
	r.Close()

	// Lower the soft limit so a per-iteration fd leak exhausts fds fast.
	var orig syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &orig); err != nil {
		t.Skipf("getrlimit: %v", err)
	}
	lowered := orig
	lowered.Cur = 128
	if orig.Max < lowered.Cur {
		lowered.Cur = orig.Max
	}
	if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lowered); err != nil {
		t.Skipf("setrlimit: %v", err)
	}
	t.Cleanup(func() { _ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &orig) })

	// Iterate past the lowered soft limit so any leak manifests as
	// "too many open files".
	const iterations = 512
	for i := 0; i < iterations; i++ {
		r, _, err := bucket.GetObject(ctx, key, storage.BytesRange(int64(len(value)), -1))
		if err != nil {
			t.Fatalf("iteration %d: unexpected error: %v", i, err)
		}
		b, err := io.ReadAll(r)
		if err != nil {
			r.Close()
			t.Fatalf("iteration %d: unexpected read error: %v", i, err)
		}
		if len(b) != 0 {
			r.Close()
			t.Fatalf("iteration %d: expected empty body, got %q", i, b)
		}
		if err := r.Close(); err != nil {
			t.Fatalf("iteration %d: unexpected close error: %v", i, err)
		}
	}
}

// TestCacheTailReadUsesCachedFileSizeNotInfoSize covers a GCS-style
// transcoded object cached through file.NewCache: the bucket writes
// the decompressed bytes to disk, but ObjectInfo.Size reports the
// stored compressed length. Both the cache-miss path (range slicing
// directly after writing the body) and the subsequent cache-hit path
// (readObjectInfo) must use the cached file's actual size, not
// info.Size — otherwise tail reads are truncated.
func TestCacheTailReadUsesCachedFileSizeNotInfoSize(t *testing.T) {
	cacheDir := t.TempDir()
	ctx := t.Context()

	body := strings.Repeat("decompressed body ", 100) // 1800 bytes
	underlying := &sizeLyingBucket{
		info: storage.ObjectInfo{
			Size:         60, // pretend compressed
			CacheControl: "max-age=3600",
			ETag:         "\"etag\"",
		},
		body: body,
	}
	bucket := NewCache(cacheDir, math.MaxInt64).AdaptBucket(underlying)

	t.Run("cache miss", func(t *testing.T) {
		r, _, err := bucket.GetObject(ctx, "k", storage.BytesRange(10, -1))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if want := body[10:]; len(got) != len(want) {
			t.Fatalf("body length = %d, want %d (clamped to info.Size?)", len(got), len(want))
		}
	})

	t.Run("cache hit", func(t *testing.T) {
		// The prior cache-miss test primed the cache file. This run
		// exercises the cache-hit path in cachedBucket.GetObject,
		// where the returned ObjectInfo comes from readObjectInfo and
		// its Size is the file's on-disk size.
		r, _, err := bucket.GetObject(ctx, "k", storage.BytesRange(100, -1))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if want := body[100:]; len(got) != len(want) {
			t.Fatalf("body length = %d, want %d (cache hit clamped to stored info.Size?)", len(got), len(want))
		}
	})
}

// sizeLyingBucket simulates a backend that returns an ObjectInfo.Size
// smaller than the actual body length (the gs transcoded scenario).
type sizeLyingBucket struct {
	storage.Bucket
	info storage.ObjectInfo
	body string
}

func (b *sizeLyingBucket) Location() string { return "mock://lies" }

func (b *sizeLyingBucket) Access(ctx context.Context) error { return nil }

func (b *sizeLyingBucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return b.info, nil
}

func (b *sizeLyingBucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	getOptions := storage.NewGetOptions(options...)
	body := b.body
	if start, _, ok := getOptions.BytesRange(); ok {
		if start >= int64(len(body)) {
			body = ""
		} else {
			body = body[start:]
		}
	}
	return io.NopCloser(strings.NewReader(body)), b.info, nil
}
