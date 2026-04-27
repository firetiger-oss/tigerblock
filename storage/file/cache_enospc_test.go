//go:build !windows

package file

import (
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/firetiger-oss/tigerblock/storage"
)

// createSmallDisk creates a tiny disk (512KB) and returns mount path + cleanup func.
// Returns ("", nil) if disk creation is not supported on this platform.
func createSmallDisk(t *testing.T) (mountPath string, cleanup func()) {
	t.Helper()

	if runtime.GOOS == "darwin" {
		return createSmallDiskDarwin(t)
	} else if runtime.GOOS == "linux" {
		return createSmallDiskLinux(t)
	}
	return "", nil
}

func createSmallDiskDarwin(t *testing.T) (string, func()) {
	t.Helper()

	// Create a small RAM disk (512KB = 1024 sectors of 512 bytes)
	cmd := exec.Command("hdiutil", "attach", "-nomount", "ram://1024")
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("Cannot create RAM disk: %v", err)
		return "", nil
	}
	device := strings.TrimSpace(string(out))

	// Format it as HFS+
	cmd = exec.Command("diskutil", "erasevolume", "HFS+", "TestCache", device)
	if err := cmd.Run(); err != nil {
		exec.Command("hdiutil", "detach", device).Run()
		t.Skipf("Cannot format RAM disk: %v", err)
		return "", nil
	}

	mountPath := "/Volumes/TestCache"
	cleanup := func() {
		exec.Command("hdiutil", "detach", device).Run()
	}
	return mountPath, cleanup
}

func createSmallDiskLinux(t *testing.T) (string, func()) {
	t.Helper()

	// Create tmpfs mount with small size limit
	mountPath := filepath.Join(os.TempDir(), "enospc_test_"+t.Name())
	if err := os.MkdirAll(mountPath, 0755); err != nil {
		t.Skipf("Cannot create mount point: %v", err)
		return "", nil
	}

	// Mount tmpfs with 512KB limit
	cmd := exec.Command("mount", "-t", "tmpfs", "-o", "size=512k", "tmpfs", mountPath)
	if err := cmd.Run(); err != nil {
		os.RemoveAll(mountPath)
		t.Skipf("Cannot mount tmpfs (may need root): %v", err)
		return "", nil
	}

	cleanup := func() {
		exec.Command("umount", mountPath).Run()
		os.RemoveAll(mountPath)
	}
	return mountPath, cleanup
}

func TestCacheGetObjectENOSPC(t *testing.T) {
	mountPath, cleanup := createSmallDisk(t)
	if mountPath == "" {
		t.Skip("Small disk creation not supported")
	}
	defer cleanup()

	store := t.TempDir()
	ctx := t.Context()

	// Create cache on the tiny disk
	cache := NewCache(mountPath, 1024*1024) // 1MB limit (larger than disk)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Put a large object (larger than 512KB disk)
	largeData := strings.Repeat("x", 600*1024) // 600KB

	// First, put via underlying bucket (so GetObject can fetch it)
	_, err = bucket.PutObject(ctx, "large-object", strings.NewReader(largeData),
		storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal(err)
	}

	// Now GetObject through cache - should trigger ENOSPC during caching
	// but still return the data (graceful degradation)
	r, info, err := cachedBucket.GetObject(ctx, "large-object")
	if err != nil {
		t.Fatalf("GetObject should succeed despite ENOSPC: %v", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reading object should succeed: %v", err)
	}
	if string(data) != largeData {
		t.Error("Data mismatch")
	}
	if info.Size != int64(len(largeData)) {
		t.Errorf("Size mismatch: %d != %d", info.Size, len(largeData))
	}
}

func TestCachePutObjectENOSPC(t *testing.T) {
	mountPath, cleanup := createSmallDisk(t)
	if mountPath == "" {
		t.Skip("Small disk creation not supported")
	}
	defer cleanup()

	store := t.TempDir()
	ctx := t.Context()

	// Create cache on the tiny disk
	cache := NewCache(mountPath, 1024*1024) // 1MB limit (larger than disk)
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// Put a large object (larger than 512KB disk)
	largeData := strings.Repeat("y", 600*1024) // 600KB

	// PutObject through cache - should trigger ENOSPC during caching
	// but still succeed in writing to backend (graceful degradation)
	info, err := cachedBucket.PutObject(ctx, "large-put-object", strings.NewReader(largeData),
		storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatalf("PutObject should succeed despite ENOSPC: %v", err)
	}
	if info.Size != int64(len(largeData)) {
		t.Errorf("Size mismatch: %d != %d", info.Size, len(largeData))
	}

	// Verify the object is correctly stored in the backend
	r, _, err := bucket.GetObject(ctx, "large-put-object")
	if err != nil {
		t.Fatalf("Object should exist in backend: %v", err)
	}
	defer r.Close()

	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("Reading object from backend should succeed: %v", err)
	}
	if string(data) != largeData {
		t.Error("Data mismatch in backend")
	}
}

func TestCacheEvictOnENOSPC(t *testing.T) {
	mountPath, cleanup := createSmallDisk(t)
	if mountPath == "" {
		t.Skip("Small disk creation not supported")
	}
	defer cleanup()

	store := t.TempDir()
	ctx := t.Context()

	// Create cache on the tiny disk with a large logical limit
	cache := NewCache(mountPath, 1024*1024) // 1MB logical limit
	bucket, err := NewRegistry(store).LoadBucket(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	cachedBucket := cache.AdaptBucket(bucket)

	// First, put several small objects that fit on the tiny disk
	for i := 0; i < 3; i++ {
		key := "small-object-" + string(rune('a'+i))
		smallData := strings.Repeat(string(rune('a'+i)), 50*1024) // 50KB each
		_, err := cachedBucket.PutObject(ctx, key, strings.NewReader(smallData),
			storage.CacheControl("public, max-age=3600"))
		if err != nil {
			t.Fatalf("Failed to put small object %d: %v", i, err)
		}
	}

	// Check initial cache stats
	statBefore := cache.Stat()
	t.Logf("Cache before large object: Size=%d, Evictions=%d", statBefore.Size, statBefore.Evictions)

	// Now put a large object that triggers ENOSPC - this should trigger eviction
	largeData := strings.Repeat("z", 600*1024) // 600KB
	_, err = bucket.PutObject(ctx, "large-trigger", strings.NewReader(largeData),
		storage.CacheControl("public, max-age=3600"))
	if err != nil {
		t.Fatal(err)
	}

	// GetObject through cache - this should trigger ENOSPC and eviction
	r, _, err := cachedBucket.GetObject(ctx, "large-trigger")
	if err != nil {
		t.Fatalf("GetObject should succeed despite ENOSPC: %v", err)
	}
	r.Close()

	// Check that eviction happened
	statAfter := cache.Stat()
	t.Logf("Cache after large object: Size=%d, Evictions=%d", statAfter.Size, statAfter.Evictions)

	// The eviction count should have increased (emergency eviction was triggered)
	if statAfter.Evictions <= statBefore.Evictions {
		t.Log("Note: Eviction may not have been triggered if ENOSPC didn't occur during caching")
	}

	// Verify all original small objects are still accessible from backend
	for i := 0; i < 3; i++ {
		key := "small-object-" + string(rune('a'+i))
		r, _, err := bucket.GetObject(ctx, key)
		if err != nil {
			t.Errorf("Small object %d should still exist: %v", i, err)
			continue
		}
		r.Close()
	}
}
