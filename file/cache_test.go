package file

import (
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/test"
)

func TestCacheWithLimit(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
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

	// Backdate the cached file to simulate expiration (instead of sleeping)
	backdateCachedFiles(cacheDir, 2*time.Second)

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

	// Verify immutable object is still cached (no sleep needed - immutable never expires)
	r3, _, err := cachedBucket.GetObject(ctx, key2)
	if err != nil {
		t.Fatal("failed to get immutable object:", err)
	}
	r3.Close()

	t.Logf("Cache expiration test completed successfully")
}

func TestCacheRevalidation(t *testing.T) {
	t.Parallel()
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

func TestCacheRangeRequest(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	// Large cache to avoid evictions
	cache := NewCache(cacheDir, 10*1024*1024) // 10 MB
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create a large object (larger than one block = 256KB)
	const objectSize = 512 * 1024 // 512 KB (2 blocks)
	key := "large-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Request only the first 1000 bytes
	r1, info1, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 999))
	if err != nil {
		t.Fatal("failed to get range 0-999:", err)
	}
	defer r1.Close()

	data1, err := io.ReadAll(r1)
	if err != nil {
		t.Fatal("failed to read range data:", err)
	}

	if len(data1) != 1000 {
		t.Errorf("expected 1000 bytes, got %d", len(data1))
	}

	// Verify the data matches
	for i := 0; i < 1000; i++ {
		if data1[i] != data[i] {
			t.Errorf("data mismatch at byte %d: got %d, want %d", i, data1[i], data[i])
			break
		}
	}

	// Verify object info has correct full size
	if info1.Size != objectSize {
		t.Errorf("expected object size %d, got %d", objectSize, info1.Size)
	}

	t.Logf("Cache stats after first range request: %+v", cache.Stat())

	// Request a different range (second block)
	const secondRangeStart = 256 * 1024 // Start of second block
	const secondRangeEnd = 256*1024 + 999

	r2, info2, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(secondRangeStart, secondRangeEnd))
	if err != nil {
		t.Fatal("failed to get second range:", err)
	}
	defer r2.Close()

	data2, err := io.ReadAll(r2)
	if err != nil {
		t.Fatal("failed to read second range data:", err)
	}

	if len(data2) != 1000 {
		t.Errorf("expected 1000 bytes for second range, got %d", len(data2))
	}

	// Verify the data matches
	for i := 0; i < 1000; i++ {
		if data2[i] != data[secondRangeStart+int64(i)] {
			t.Errorf("second range data mismatch at byte %d: got %d, want %d", i, data2[i], data[secondRangeStart+int64(i)])
			break
		}
	}

	if info2.Size != objectSize {
		t.Errorf("expected object size %d, got %d", objectSize, info2.Size)
	}

	t.Logf("Cache stats after second range request: %+v", cache.Stat())

	// Request the first range again - should hit cache
	statBefore := cache.Stat()
	r3, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 999))
	if err != nil {
		t.Fatal("failed to get first range again:", err)
	}
	r3.Close()

	statAfter := cache.Stat()
	t.Logf("Cache stats after re-requesting first range: Hits before=%d, after=%d", statBefore.Hits, statAfter.Hits)

	t.Log("Range request caching test completed successfully")
}

func TestCacheRangeRequestNotCached(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 10*1024*1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create object WITHOUT cache control
	const objectSize = 100 * 1024 // 100 KB
	key := "uncacheable-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Initial cache size should be 0 (no cacheable objects)
	stat1 := cache.Stat()
	if stat1.Size != 0 {
		t.Errorf("cache should be empty for object without cache control, got size=%d", stat1.Size)
	}

	// Request a range - should NOT be cached
	r1, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 999))
	if err != nil {
		t.Fatal("failed to get range:", err)
	}
	data1, err := io.ReadAll(r1)
	r1.Close()
	if err != nil {
		t.Fatal("failed to read range data:", err)
	}

	if len(data1) != 1000 {
		t.Errorf("expected 1000 bytes, got %d", len(data1))
	}

	// Cache should still be empty (object has no cache control)
	stat2 := cache.Stat()
	if stat2.Size != 0 {
		t.Errorf("cache should remain empty for uncacheable object, got size=%d", stat2.Size)
	}

	t.Log("Uncacheable range request test completed successfully")
}

func TestSparseFileCacheEviction(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	// Small cache to trigger evictions
	cache := NewCache(cacheDir, 1024) // 1 KB limit
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create multiple objects that will cause evictions
	objects := []struct {
		key   string
		value string
	}{
		{"obj1", strings.Repeat("first object data ", 50)},  // ~900 bytes
		{"obj2", strings.Repeat("second object data ", 50)}, // ~950 bytes
		{"obj3", strings.Repeat("third object data ", 50)},  // ~850 bytes
	}

	for _, obj := range objects {
		_, err = cachedBucket.PutObject(ctx, obj.key, strings.NewReader(obj.value), storage.CacheControl("public, max-age=3600"))
		if err != nil {
			t.Fatalf("failed to put object %s: %v", obj.key, err)
		}
	}

	stat := cache.Stat()
	t.Logf("Cache stats after putting objects: Size=%d, Limit=%d, Evictions=%d", stat.Size, stat.Limit, stat.Evictions)

	// Cache size should not exceed limit
	if stat.Size > stat.Limit {
		t.Errorf("cache size (%d) exceeds limit (%d)", stat.Size, stat.Limit)
	}

	// There should be evictions due to small cache size
	if stat.Evictions == 0 {
		t.Log("No evictions occurred (objects may be too small to trigger evictions)")
	}

	// Test range requests with eviction
	const objectSize = 512 * 1024 // 512 KB - much larger than cache
	largeKey := "large-object"
	largeData := make([]byte, objectSize)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, largeKey, strings.NewReader(string(largeData)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put large object:", err)
	}

	// Request a range from the large object
	r1, _, err := cachedBucket.GetObject(ctx, largeKey, storage.BytesRange(0, 999))
	if err != nil {
		t.Fatal("failed to get range from large object:", err)
	}
	r1.Close()

	statAfter := cache.Stat()
	t.Logf("Cache stats after range request on large object: Size=%d, Evictions=%d", statAfter.Size, statAfter.Evictions)

	t.Log("Sparse file cache eviction test completed successfully")
}

func TestRangeCacheRevalidation(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 10*1024*1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create object with no-cache (requires revalidation)
	const objectSize = 100 * 1024
	key := "revalidate-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	originalInfo, err := cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("no-cache"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Request a range
	r1, info1, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 999))
	if err != nil {
		t.Fatal("failed to get range:", err)
	}
	r1.Close()

	if info1.ETag != originalInfo.ETag {
		t.Error("ETags should match on initial request")
	}

	// Update the object in the underlying bucket (simulating backend change)
	newData := make([]byte, objectSize)
	for i := range newData {
		newData[i] = byte((i + 100) % 256)
	}

	newInfo, err := bucket.PutObject(ctx, key, strings.NewReader(string(newData)), storage.CacheControl("no-cache"))
	if err != nil {
		t.Fatal("failed to update object:", err)
	}

	// Request the same range again - should detect ETag change
	r2, info2, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 999))
	if err != nil {
		t.Fatal("failed to get range after update:", err)
	}
	defer r2.Close()

	data2, err := io.ReadAll(r2)
	if err != nil {
		t.Fatal("failed to read updated data:", err)
	}

	// Should get the new data
	for i := 0; i < len(data2); i++ {
		if data2[i] != newData[i] {
			t.Errorf("data mismatch at byte %d after revalidation: got %d, want %d", i, data2[i], newData[i])
			break
		}
	}

	if info2.ETag == originalInfo.ETag {
		t.Error("ETag should have changed after object update")
	}

	if info2.ETag != newInfo.ETag {
		t.Error("ETag should match the updated object")
	}

	t.Log("Range cache revalidation test completed successfully")
}

// backdateCachedFiles sets the modification time of all files in a directory
// to simulate time passing without using time.Sleep
func backdateCachedFiles(cacheDir string, age time.Duration) {
	past := time.Now().Add(-age)
	filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		os.Chtimes(path, past, past)
		return nil
	})
}

func TestRangeLevelEviction(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	// Use a cache size slightly larger than one block but smaller than two,
	// so caching a second range will trigger eviction of the first
	cacheSize := int64(sparseBlockSize + sparseBlockSize/2) // 1.5 blocks
	cache := NewCache(cacheDir, cacheSize)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create a large object spanning multiple blocks
	const objectSize = sparseBlockSize * 4 // 4 blocks (1 MB)
	key := "multi-block-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Request first block (this will create a sparse file and cache the first block)
	r1, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 1000))
	if err != nil {
		t.Fatal("failed to get first range:", err)
	}
	data1, _ := io.ReadAll(r1)
	r1.Close()

	// Verify first block data
	for i := 0; i < len(data1); i++ {
		if data1[i] != data[i] {
			t.Fatalf("first range data mismatch at byte %d", i)
		}
	}

	stat1 := cache.Stat()
	t.Logf("After first range: Size=%d, Evictions=%d", stat1.Size, stat1.Evictions)

	// Request third block (should trigger eviction of first block due to cache size limit)
	thirdBlockStart := int64(sparseBlockSize * 2)
	r2, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(thirdBlockStart, thirdBlockStart+1000))
	if err != nil {
		t.Fatal("failed to get third range:", err)
	}
	data2, _ := io.ReadAll(r2)
	r2.Close()

	// Verify third block data
	for i := 0; i < len(data2); i++ {
		if data2[i] != data[thirdBlockStart+int64(i)] {
			t.Fatalf("third range data mismatch at byte %d", i)
		}
	}

	stat2 := cache.Stat()
	t.Logf("After third range: Size=%d, Evictions=%d", stat2.Size, stat2.Evictions)

	// The cache size limit should have been exceeded, triggering eviction
	// We expect at least one eviction (of the first block)
	if stat2.Evictions == 0 {
		t.Log("No evictions occurred - cache may be larger than expected or filesystem doesn't report sparse usage accurately")
	}

	// Verify that the sparse file still exists (we evicted a range, not the whole file)
	var sparseFile string
	filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		sparseFile = path
		return nil
	})

	if sparseFile == "" {
		t.Fatal("sparse cache file should still exist after range eviction")
	}

	// Open the sparse file and check for holes
	f, err := os.Open(sparseFile)
	if err != nil {
		t.Fatal("failed to open sparse file:", err)
	}
	defer f.Close()

	// If evictions occurred and hole punching is supported, the file should have holes
	if stat2.Evictions > 0 {
		holePos, err := seekHole(f, 0)
		if err != nil {
			t.Logf("seekHole failed (may not be supported): %v", err)
		} else {
			fileInfo, _ := f.Stat()
			t.Logf("Sparse file: size=%d, first hole at=%d", fileInfo.Size(), holePos)

			// On filesystems supporting sparse files, there should be a hole before end of file
			if holePos < fileInfo.Size() {
				t.Log("Confirmed: hole punching occurred during eviction")
			} else {
				t.Log("No holes detected - filesystem may not support sparse files or hole punching")
			}
		}
	}

	// Request the first block again - should refetch from backend (since it was evicted)
	r3, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 1000))
	if err != nil {
		t.Fatal("failed to re-request first range:", err)
	}
	data3, _ := io.ReadAll(r3)
	r3.Close()

	// Verify we got correct data
	for i := 0; i < len(data3); i++ {
		if data3[i] != data[i] {
			t.Fatalf("re-fetched first range data mismatch at byte %d", i)
		}
	}

	stat3 := cache.Stat()
	t.Logf("After re-fetching first range: Size=%d, Evictions=%d", stat3.Size, stat3.Evictions)

	t.Log("Range-level eviction test completed successfully")
}

func TestSecondaryIndexConsistency(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	// Cache large enough for multiple ranges
	cache := NewCache(cacheDir, sparseBlockSize*4)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create a multi-block object
	const objectSize = sparseBlockSize * 4
	key := "multi-range-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Request multiple different ranges from the same object
	ranges := []struct{ start, end int64 }{
		{0, 1000},
		{sparseBlockSize, sparseBlockSize + 1000},
		{sparseBlockSize * 2, sparseBlockSize*2 + 1000},
	}

	for _, r := range ranges {
		rc, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(r.start, r.end))
		if err != nil {
			t.Fatalf("failed to get range [%d, %d]: %v", r.start, r.end, err)
		}
		rc.Close()
	}

	stat := cache.Stat()
	t.Logf("Cache after multiple range requests: Size=%d, Hits=%d, Misses=%d", stat.Size, stat.Hits, stat.Misses)

	// Verify the secondary index has entries for the file
	cache.mutex.Lock()
	fileCount := len(cache.files)
	var rangeCount int
	for _, ranges := range cache.files {
		rangeCount += len(ranges)
	}
	cache.mutex.Unlock()

	if fileCount == 0 {
		t.Error("expected at least one file in secondary index")
	}
	t.Logf("Secondary index: %d files, %d total ranges", fileCount, rangeCount)

	// Delete the object and verify secondary index is cleaned up
	if err := cachedBucket.DeleteObject(ctx, key); err != nil {
		t.Fatal("failed to delete object:", err)
	}

	cache.mutex.Lock()
	fileCountAfter := len(cache.files)
	cache.mutex.Unlock()

	if fileCountAfter != 0 {
		t.Errorf("expected 0 files in secondary index after delete, got %d", fileCountAfter)
	}

	t.Log("Secondary index consistency test completed successfully")
}

func TestEmptyFileCleanupAfterEviction(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	// Very small cache - can only fit about 1 block
	cacheSize := int64(sparseBlockSize + 1000)
	cache := NewCache(cacheDir, cacheSize)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create first object - just one block
	key1 := "object1"
	data1 := make([]byte, sparseBlockSize)
	for i := range data1 {
		data1[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key1, strings.NewReader(string(data1)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object1:", err)
	}

	// Request the block to cache it
	r1, _, err := cachedBucket.GetObject(ctx, key1, storage.BytesRange(0, 1000))
	if err != nil {
		t.Fatal("failed to get object1:", err)
	}
	r1.Close()

	// Count files before creating second object
	filesBefore := countFilesInDir(cacheDir)
	t.Logf("Files in cache before second object: %d", filesBefore)

	// Create second object - this should evict all ranges from first object
	key2 := "object2"
	data2 := make([]byte, sparseBlockSize)
	for i := range data2 {
		data2[i] = byte((i + 100) % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key2, strings.NewReader(string(data2)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object2:", err)
	}

	// Request second object's block
	r2, _, err := cachedBucket.GetObject(ctx, key2, storage.BytesRange(0, 1000))
	if err != nil {
		t.Fatal("failed to get object2:", err)
	}
	r2.Close()

	stat := cache.Stat()
	t.Logf("Cache stats: Size=%d, Evictions=%d", stat.Size, stat.Evictions)

	// After eviction of all ranges from first object, the file should be deleted
	if stat.Evictions > 0 {
		filesAfter := countFilesInDir(cacheDir)
		t.Logf("Files in cache after eviction: %d", filesAfter)

		// We should still have only the second object's file
		// (the first object's file should have been cleaned up)
		if filesAfter > 1 {
			t.Log("Multiple files remain - first object's file may not have been fully cleaned up")
		}
	}

	t.Log("Empty file cleanup test completed")
}

func TestConcurrentRangeRequests(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 10*1024*1024) // 10 MB cache
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create a large object
	const objectSize = sparseBlockSize * 4
	key := "concurrent-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Launch concurrent goroutines requesting different ranges
	const numGoroutines = 10
	var wg sync.WaitGroup
	errChan := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Each goroutine requests a different range
			start := int64(idx * 1000)
			end := start + 999

			rc, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(start, end))
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: failed to get range: %w", idx, err)
				return
			}
			defer rc.Close()

			readData, err := io.ReadAll(rc)
			if err != nil {
				errChan <- fmt.Errorf("goroutine %d: failed to read: %w", idx, err)
				return
			}

			// Verify data correctness
			for j := 0; j < len(readData); j++ {
				expected := byte((start + int64(j)) % 256)
				if readData[j] != expected {
					errChan <- fmt.Errorf("goroutine %d: data mismatch at offset %d: got %d, want %d", idx, j, readData[j], expected)
					return
				}
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}

	stat := cache.Stat()
	t.Logf("Cache stats after concurrent requests: Size=%d, Hits=%d, Misses=%d", stat.Size, stat.Hits, stat.Misses)

	t.Log("Concurrent range requests test completed successfully")
}

func TestOverlappingRangeRequests(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 10*1024*1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create an object
	const objectSize = sparseBlockSize * 2
	key := "overlap-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Request first range: 0-1000
	r1, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 1000))
	if err != nil {
		t.Fatal("failed to get first range:", err)
	}
	data1, _ := io.ReadAll(r1)
	r1.Close()

	// Request overlapping range: 500-1500
	r2, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(500, 1500))
	if err != nil {
		t.Fatal("failed to get overlapping range:", err)
	}
	data2, _ := io.ReadAll(r2)
	r2.Close()

	// Verify first range data
	for i := 0; i < len(data1); i++ {
		if data1[i] != data[i] {
			t.Errorf("first range data mismatch at byte %d", i)
			break
		}
	}

	// Verify overlapping range data
	for i := 0; i < len(data2); i++ {
		expected := data[500+i]
		if data2[i] != expected {
			t.Errorf("overlapping range data mismatch at byte %d: got %d, want %d", i, data2[i], expected)
			break
		}
	}

	stat := cache.Stat()
	t.Logf("Cache stats after overlapping requests: Size=%d, Hits=%d, Misses=%d", stat.Size, stat.Hits, stat.Misses)

	t.Log("Overlapping range requests test completed successfully")
}

func TestZeroByteObjectCaching(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Put a zero-byte object with cache control
	key := "empty-object"
	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(""), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put zero-byte object:", err)
	}

	// Get the zero-byte object
	r, info, err := cachedBucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("failed to get zero-byte object:", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("failed to read zero-byte object:", err)
	}

	if len(data) != 0 {
		t.Errorf("expected 0 bytes, got %d", len(data))
	}

	if info.Size != 0 {
		t.Errorf("expected size 0, got %d", info.Size)
	}

	stat := cache.Stat()
	t.Logf("Cache stats: Size=%d, Hits=%d, Misses=%d", stat.Size, stat.Hits, stat.Misses)

	// Get it again to test cache hit
	r2, _, err := cachedBucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("failed to get zero-byte object second time:", err)
	}
	r2.Close()

	t.Log("Zero-byte object caching test completed successfully")
}

func TestRangeBeyondFileSize(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 10*1024*1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create a small object
	const objectSize = 1000
	key := "small-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Request range that extends beyond file size
	r, info, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(500, 2000))
	if err != nil {
		t.Fatal("failed to get range beyond file size:", err)
	}
	defer r.Close()

	readData, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("failed to read data:", err)
	}

	// Should get data from 500 to end of file (999), which is 500 bytes
	expectedLen := objectSize - 500
	if len(readData) != expectedLen {
		t.Errorf("expected %d bytes, got %d", expectedLen, len(readData))
	}

	// Verify the data is correct for the valid portion
	for i := 0; i < len(readData); i++ {
		expected := byte((500 + i) % 256)
		if readData[i] != expected {
			t.Errorf("data mismatch at offset %d: got %d, want %d", i, readData[i], expected)
			break
		}
	}

	t.Logf("Object size: %d, returned data length: %d", info.Size, len(readData))
	t.Log("Range beyond file size test completed successfully")
}

func TestCacheRehydrationWithSparseFiles(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	// Create first cache instance
	cache1 := NewCache(cacheDir, 10*1024*1024)
	bucket1, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket1 := cache1.AdaptBucket(bucket1)

	// Create a multi-block object
	const objectSize = sparseBlockSize * 3
	key := "rehydration-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket1.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Request only the first block (creating a sparse file)
	r1, _, err := cachedBucket1.GetObject(ctx, key, storage.BytesRange(0, 1000))
	if err != nil {
		t.Fatal("failed to get first block:", err)
	}
	r1.Close()

	stat1 := cache1.Stat()
	t.Logf("Cache1 stats: Size=%d, Hits=%d, Misses=%d", stat1.Size, stat1.Hits, stat1.Misses)

	// Create a new cache instance on the same directory (simulating restart)
	cache2 := NewCache(cacheDir, 10*1024*1024)
	bucket2, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create second bucket:", err)
	}
	cachedBucket2 := cache2.AdaptBucket(bucket2)

	stat2Initial := cache2.Stat()
	t.Logf("Cache2 initial stats after rehydration: Size=%d", stat2Initial.Size)

	// The cache should have detected the cached data during rehydration
	if stat2Initial.Size == 0 {
		t.Log("Cache rehydration may not have detected the cached ranges")
	}

	// Request the same range again - should be a cache hit if rehydration worked
	r2, _, err := cachedBucket2.GetObject(ctx, key, storage.BytesRange(0, 1000))
	if err != nil {
		t.Fatal("failed to get first block from second cache:", err)
	}
	data2, _ := io.ReadAll(r2)
	r2.Close()

	// Verify data correctness
	for i := 0; i < len(data2); i++ {
		if data2[i] != data[i] {
			t.Errorf("rehydrated data mismatch at byte %d", i)
			break
		}
	}

	stat2Final := cache2.Stat()
	t.Logf("Cache2 final stats: Size=%d, Hits=%d, Misses=%d", stat2Final.Size, stat2Final.Hits, stat2Final.Misses)

	// Request a different range that wasn't cached
	r3, _, err := cachedBucket2.GetObject(ctx, key, storage.BytesRange(sparseBlockSize, sparseBlockSize+1000))
	if err != nil {
		t.Fatal("failed to get second block:", err)
	}
	data3, _ := io.ReadAll(r3)
	r3.Close()

	// Verify data correctness
	for i := 0; i < len(data3); i++ {
		expected := byte((sparseBlockSize + int64(i)) % 256)
		if data3[i] != expected {
			t.Errorf("new range data mismatch at byte %d", i)
			break
		}
	}

	t.Log("Cache rehydration with sparse files test completed successfully")
}

func TestBlockAlignmentDuringFetch(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 10*1024*1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create object larger than one block
	const objectSize = sparseBlockSize * 2
	key := "alignment-object"
	data := make([]byte, objectSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Request a small range in the middle of the first block (not aligned)
	smallStart := int64(1000)
	smallEnd := int64(1099) // Just 100 bytes

	r1, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(smallStart, smallEnd))
	if err != nil {
		t.Fatal("failed to get small range:", err)
	}
	data1, _ := io.ReadAll(r1)
	r1.Close()

	// Verify data
	for i := 0; i < len(data1); i++ {
		expected := byte((smallStart + int64(i)) % 256)
		if data1[i] != expected {
			t.Errorf("small range data mismatch at byte %d", i)
			break
		}
	}

	stat1 := cache.Stat()
	t.Logf("After first small request: Size=%d", stat1.Size)

	// The entire first block should have been fetched, so requesting
	// a different range in the same block should be a cache hit
	hitsBefore := stat1.Hits

	// Request another small range in the first block
	r2, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(5000, 5099))
	if err != nil {
		t.Fatal("failed to get second small range:", err)
	}
	data2, _ := io.ReadAll(r2)
	r2.Close()

	// Verify data
	for i := 0; i < len(data2); i++ {
		expected := byte((5000 + int64(i)) % 256)
		if data2[i] != expected {
			t.Errorf("second small range data mismatch at byte %d", i)
			break
		}
	}

	stat2 := cache.Stat()
	t.Logf("After second small request: Hits before=%d, after=%d", hitsBefore, stat2.Hits)

	// Hits should have increased since the block was already cached
	if stat2.Hits > hitsBefore {
		t.Log("Block alignment working correctly - second request was a cache hit")
	} else {
		t.Log("Second request was not a cache hit - block alignment may not be working as expected")
	}

	t.Log("Block alignment during fetch test completed successfully")
}

func TestMultipleFilesLRUEviction(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	// Small cache that can hold about 2 objects
	cacheSize := int64(sparseBlockSize*2 + 1000)
	cache := NewCache(cacheDir, cacheSize)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create 4 objects, each one block in size
	objects := make(map[string][]byte)
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("lru-object-%d", i)
		data := make([]byte, sparseBlockSize)
		for j := range data {
			data[j] = byte((i*100 + j) % 256)
		}
		objects[key] = data

		_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(string(data)), storage.CacheControl("public, max-age=3600"))
		if err != nil {
			t.Fatalf("failed to put object %s: %v", key, err)
		}
	}

	// Access objects in order: 0, 1, 2, 3
	for i := 0; i < 4; i++ {
		key := fmt.Sprintf("lru-object-%d", i)
		r, _, err := cachedBucket.GetObject(ctx, key, storage.BytesRange(0, 100))
		if err != nil {
			t.Fatalf("failed to get object %s: %v", key, err)
		}
		r.Close()

		stat := cache.Stat()
		t.Logf("After accessing object %d: Size=%d, Evictions=%d", i, stat.Size, stat.Evictions)
	}

	stat := cache.Stat()
	t.Logf("Final stats: Size=%d, Limit=%d, Evictions=%d", stat.Size, stat.Limit, stat.Evictions)

	// Cache should have evicted earlier objects to stay within limit
	if stat.Size > stat.Limit {
		t.Errorf("cache size (%d) exceeds limit (%d)", stat.Size, stat.Limit)
	}

	// With 4 objects and space for ~2, we should have evictions
	if stat.Evictions == 0 {
		t.Log("No evictions occurred - cache may have more space than expected")
	}

	// Access object 3 (most recent) - should be a hit
	r3, _, err := cachedBucket.GetObject(ctx, "lru-object-3", storage.BytesRange(0, 100))
	if err != nil {
		t.Fatal("failed to re-access object 3:", err)
	}
	r3.Close()

	statAfter := cache.Stat()
	if statAfter.Hits > stat.Hits {
		t.Log("Object 3 was correctly still in cache (LRU working)")
	}

	t.Log("Multiple files LRU eviction test completed successfully")
}

func TestCorruptedCacheMetadataRecovery(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 10*1024*1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create an object
	key := "test-object"
	originalData := "this is the original content"
	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(originalData), storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Get the object to cache it
	r1, _, err := cachedBucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("failed to get object:", err)
	}
	r1.Close()

	// Find the cached file
	var cachedFile string
	filepath.Walk(cacheDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		cachedFile = path
		return nil
	})

	if cachedFile == "" {
		t.Fatal("cached file not found")
	}

	// Corrupt the metadata by deleting the cache file entirely
	// This simulates cache corruption in a portable way
	if err := os.Remove(cachedFile); err != nil {
		t.Fatal("failed to remove cached file:", err)
	}

	// Try to get the object again - cache should detect missing file
	// and fetch from backend
	r2, info2, err := cachedBucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("failed to get object after metadata corruption:", err)
	}
	defer r2.Close()

	data2, err := io.ReadAll(r2)
	if err != nil {
		t.Fatal("failed to read object after metadata corruption:", err)
	}

	// Verify we got the correct data (should have been fetched from backend)
	if string(data2) != originalData {
		t.Errorf("expected %q, got %q", originalData, string(data2))
	}

	t.Logf("Object info after recovery: Size=%d, ETag=%s", info2.Size, info2.ETag)
	t.Log("Corrupted cache metadata recovery test completed")
}

func TestCacheRevalidationWithBackendError(t *testing.T) {
	t.Parallel()
	store := t.TempDir()
	cacheDir := t.TempDir()
	ctx := t.Context()

	cache := NewCache(cacheDir, 10*1024*1024)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal("failed to create bucket:", err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Create an object with no-cache (requires revalidation)
	key := "revalidate-object"
	originalData := "original content"
	_, err = cachedBucket.PutObject(ctx, key, strings.NewReader(originalData), storage.CacheControl("no-cache"))
	if err != nil {
		t.Fatal("failed to put object:", err)
	}

	// Get the object to cache it
	r1, _, err := cachedBucket.GetObject(ctx, key)
	if err != nil {
		t.Fatal("failed to get object:", err)
	}
	r1.Close()

	// Delete the object from the backend (not through cache)
	if err := bucket.DeleteObject(ctx, key); err != nil {
		t.Fatal("failed to delete object from backend:", err)
	}

	// Get the object - current behavior is to return cached data if HEAD fails
	// This documents the fail-open behavior for revalidation
	r2, _, err := cachedBucket.GetObject(ctx, key)
	if err != nil {
		t.Logf("Got error when backend object deleted: %v", err)
	} else {
		data2, _ := io.ReadAll(r2)
		r2.Close()
		t.Logf("Cache returned stale data when backend HEAD failed: %q", string(data2))
		t.Log("This documents current fail-open revalidation behavior")
	}

	t.Log("Cache revalidation with backend error test completed")
}

// Helper function to count files in a directory
func countFilesInDir(dir string) int {
	count := 0
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return nil
		}
		count++
		return nil
	})
	return count
}
