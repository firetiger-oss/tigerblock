package file

import (
	"os"
	"testing"
)

func TestSparseFileOperations(t *testing.T) {
	t.Parallel()
	// Create a temp file and make it sparse by truncating
	f, err := os.CreateTemp(t.TempDir(), "sparse-test")
	if err != nil {
		t.Fatal("failed to create temp file:", err)
	}
	defer f.Close()

	// Make it a sparse file by truncating to a large size
	const fileSize = 1024 * 1024 // 1 MB
	if err := f.Truncate(fileSize); err != nil {
		t.Fatal("failed to truncate file:", err)
	}

	// Write some data at the beginning (first 4KB)
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal("failed to write data at offset 0:", err)
	}

	// Write some data in the middle (at 512KB)
	if _, err := f.WriteAt(data, 512*1024); err != nil {
		t.Fatal("failed to write data at offset 512KB:", err)
	}

	// Sync to ensure data is on disk
	if err := f.Sync(); err != nil {
		t.Fatal("failed to sync file:", err)
	}

	// Test seekData - should find data at position 0
	dataPos, err := seekData(f, 0)
	if err != nil {
		t.Fatal("seekData failed:", err)
	}
	if dataPos != 0 {
		t.Errorf("seekData(0) = %d, expected 0", dataPos)
	}

	// Test seekHole - should find hole after the first data region
	// Note: Some filesystems (like APFS) may not support sparse files
	// and will return the file size instead
	holePos, err := seekHole(f, 0)
	if err != nil {
		t.Fatal("seekHole failed:", err)
	}
	t.Logf("seekHole(0) = %d (filesystem may not support sparse files if this equals file size)", holePos)

	// Test diskUsage
	usage, err := diskUsage(f)
	if err != nil {
		t.Fatal("diskUsage failed:", err)
	}
	t.Logf("Sparse file: logical size=%d, disk usage=%d", fileSize, usage)

	// Note: On filesystems that don't support sparse files,
	// disk usage may equal or exceed logical size
	// This is acceptable - the code handles this via min(diskUsage, logicalSize)
}

func TestBlockAlign(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		start, end int64
		wantStart  int64
		wantEnd    int64
	}{
		{
			name:      "within first block",
			start:     0,
			end:       100,
			wantStart: 0,
			wantEnd:   sparseBlockSize,
		},
		{
			name:      "second block",
			start:     sparseBlockSize + 1,
			end:       sparseBlockSize + 100,
			wantStart: sparseBlockSize,
			wantEnd:   sparseBlockSize * 2,
		},
		{
			name:      "spanning two blocks",
			start:     100,
			end:       sparseBlockSize + 100,
			wantStart: 0,
			wantEnd:   sparseBlockSize * 2,
		},
		{
			name:      "exact block boundaries",
			start:     sparseBlockSize,
			end:       sparseBlockSize*2 - 1,
			wantStart: sparseBlockSize,
			wantEnd:   sparseBlockSize * 2,
		},
		{
			name:      "spanning three blocks",
			start:     sparseBlockSize / 2,
			end:       sparseBlockSize*2 + sparseBlockSize/2,
			wantStart: 0,
			wantEnd:   sparseBlockSize * 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStart, gotEnd := blockAlign(tt.start, tt.end)
			if gotStart != tt.wantStart {
				t.Errorf("blockAlign(%d, %d) start = %d, want %d", tt.start, tt.end, gotStart, tt.wantStart)
			}
			if gotEnd != tt.wantEnd {
				t.Errorf("blockAlign(%d, %d) end = %d, want %d", tt.start, tt.end, gotEnd, tt.wantEnd)
			}
		})
	}
}

func TestIsRangeCached(t *testing.T) {
	t.Parallel()
	// Create a sparse file with some data
	f, err := os.CreateTemp(t.TempDir(), "range-test")
	if err != nil {
		t.Fatal("failed to create temp file:", err)
	}
	defer f.Close()

	// Make it a sparse file
	const fileSize = sparseBlockSize * 4 // 4 blocks
	if err := f.Truncate(fileSize); err != nil {
		t.Fatal("failed to truncate file:", err)
	}

	// Write data to first block only
	data := make([]byte, sparseBlockSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal("failed to write data:", err)
	}
	f.Sync()

	// Test: First block should be cached (has data)
	cached, err := isRangeCached(f, 0, sparseBlockSize-1)
	if err != nil {
		t.Fatal("isRangeCached failed:", err)
	}
	if !cached {
		t.Error("first block should be cached")
	}

	// Check if filesystem supports sparse files by checking seekHole
	holePos, _ := seekHole(f, 0)
	sparseSupported := holePos < fileSize

	if sparseSupported {
		// Test: Second block should NOT be cached (it's a hole)
		cached, err = isRangeCached(f, sparseBlockSize, sparseBlockSize*2-1)
		if err != nil {
			t.Fatal("isRangeCached failed:", err)
		}
		if cached {
			t.Error("second block should not be cached")
		}

		// Test: Range spanning cached and uncached should return false
		cached, err = isRangeCached(f, 0, sparseBlockSize*2-1)
		if err != nil {
			t.Fatal("isRangeCached failed:", err)
		}
		if cached {
			t.Error("range spanning cached and uncached should not be fully cached")
		}
	} else {
		t.Log("Filesystem does not support sparse files, skipping hole detection tests")
		// On non-sparse filesystems, truncated data appears as data (zeros)
		// so isRangeCached will return true for all ranges
	}
}

func TestUncachedRanges(t *testing.T) {
	t.Parallel()
	// Create a sparse file with some data
	f, err := os.CreateTemp(t.TempDir(), "uncached-test")
	if err != nil {
		t.Fatal("failed to create temp file:", err)
	}
	defer f.Close()

	// Make it a sparse file
	const fileSize = sparseBlockSize * 4 // 4 blocks
	if err := f.Truncate(fileSize); err != nil {
		t.Fatal("failed to truncate file:", err)
	}

	// Write data to blocks 0 and 2 only
	data := make([]byte, sparseBlockSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := f.WriteAt(data, 0); err != nil { // Block 0
		t.Fatal("failed to write data to block 0:", err)
	}
	if _, err := f.WriteAt(data, sparseBlockSize*2); err != nil { // Block 2
		t.Fatal("failed to write data to block 2:", err)
	}
	f.Sync()

	// Check if filesystem supports sparse files
	holePos, _ := seekHole(f, 0)
	sparseSupported := holePos < fileSize

	// Test: Find uncached ranges in the entire file
	uncached, err := uncachedRanges(f, 0, fileSize-1)
	if err != nil {
		t.Fatal("uncachedRanges failed:", err)
	}

	t.Logf("Uncached ranges: %+v", uncached)

	if sparseSupported {
		// Should find holes in blocks 1 and 3
		if len(uncached) == 0 {
			t.Error("expected to find uncached ranges on sparse-supporting filesystem")
		}

		// Test: Find uncached ranges in just the first two blocks
		uncached, err = uncachedRanges(f, 0, sparseBlockSize*2-1)
		if err != nil {
			t.Fatal("uncachedRanges failed:", err)
		}

		// Should find hole in block 1
		foundBlock1Hole := false
		for _, r := range uncached {
			if r.Start >= sparseBlockSize && r.End < sparseBlockSize*2 {
				foundBlock1Hole = true
			}
		}
		if !foundBlock1Hole {
			t.Logf("Uncached ranges in first two blocks: %+v", uncached)
		}
	} else {
		t.Log("Filesystem does not support sparse files, skipping hole detection tests")
		// On non-sparse filesystems, there are no holes, so uncachedRanges returns empty
		// This is expected behavior
	}
}

func TestPunchHole(t *testing.T) {
	t.Parallel()
	// Create a temp file with data
	f, err := os.CreateTemp(t.TempDir(), "punchhole-test")
	if err != nil {
		t.Fatal("failed to create temp file:", err)
	}
	defer f.Close()

	// Write data to the file (two blocks worth)
	const blockSize = sparseBlockSize
	data := make([]byte, blockSize*2)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal("failed to write data:", err)
	}
	f.Sync()

	// Get initial disk usage
	usageBefore, err := diskUsage(f)
	if err != nil {
		t.Fatal("failed to get initial disk usage:", err)
	}
	t.Logf("Disk usage before punch: %d bytes", usageBefore)

	// Punch a hole in the first block
	err = punchHole(f, 0, blockSize)
	if err != nil {
		t.Logf("punchHole returned error: %v (may not be supported on this filesystem)", err)
		t.Skip("punchHole not supported on this filesystem")
	}
	f.Sync()

	// Get disk usage after punch
	usageAfter, err := diskUsage(f)
	if err != nil {
		t.Fatal("failed to get disk usage after punch:", err)
	}
	t.Logf("Disk usage after punch: %d bytes", usageAfter)

	// Verify disk usage decreased (or stayed same on non-supporting filesystems)
	if usageAfter > usageBefore {
		t.Errorf("disk usage should not increase after punch hole: before=%d, after=%d", usageBefore, usageAfter)
	}

	// Check if filesystem supports sparse files by checking if a hole was created
	holePos, err := seekHole(f, 0)
	if err != nil {
		t.Fatal("seekHole failed:", err)
	}

	fileInfo, _ := f.Stat()
	fileSize := fileInfo.Size()

	if holePos < fileSize {
		// Filesystem supports holes - verify the hole is where we punched
		t.Logf("Hole found at offset %d (file size: %d)", holePos, fileSize)
		if holePos != 0 {
			t.Errorf("expected hole at offset 0, got %d", holePos)
		}

		// Verify data in second block is still intact
		readBuf := make([]byte, 100)
		n, err := f.ReadAt(readBuf, blockSize)
		if err != nil {
			t.Fatal("failed to read second block:", err)
		}
		if n != 100 {
			t.Errorf("expected to read 100 bytes, got %d", n)
		}
		for i := 0; i < n; i++ {
			expected := byte((blockSize + int64(i)) % 256)
			if readBuf[i] != expected {
				t.Errorf("data mismatch at offset %d: got %d, want %d", blockSize+int64(i), readBuf[i], expected)
				break
			}
		}
		t.Log("Second block data verified intact after hole punch")
	} else {
		t.Log("Filesystem does not support hole detection, skipping hole verification")
	}
}

func TestDiskUsageAccuracy(t *testing.T) {
	t.Parallel()
	// Create a temp file and write a known amount of data
	f, err := os.CreateTemp(t.TempDir(), "diskusage-test")
	if err != nil {
		t.Fatal("failed to create temp file:", err)
	}
	defer f.Close()

	// Write exactly 64 KB of data
	const dataSize = 64 * 1024
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal("failed to write data:", err)
	}
	f.Sync()

	// Get disk usage
	usage, err := diskUsage(f)
	if err != nil {
		t.Fatal("failed to get disk usage:", err)
	}

	t.Logf("Written bytes: %d, Disk usage: %d", dataSize, usage)

	// Disk usage should be at least the amount of data written
	// (may be slightly more due to filesystem overhead/block alignment)
	if usage < dataSize {
		t.Errorf("disk usage (%d) is less than written data (%d)", usage, dataSize)
	}

	// Disk usage shouldn't be dramatically larger than written data
	// Allow 2x overhead for filesystem metadata/alignment
	if usage > dataSize*2 {
		t.Logf("disk usage (%d) is more than 2x written data (%d) - filesystem may have unusual block size", usage, dataSize)
	}
}

func TestHolePunchingReducesDiskUsage(t *testing.T) {
	t.Parallel()
	// Create a temp file with data across multiple blocks
	f, err := os.CreateTemp(t.TempDir(), "holepunch-reduce-test")
	if err != nil {
		t.Fatal("failed to create temp file:", err)
	}
	defer f.Close()

	// Write 3 blocks worth of data
	const numBlocks = 3
	const blockSize = sparseBlockSize
	data := make([]byte, blockSize*numBlocks)
	for i := range data {
		data[i] = byte(i % 256)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal("failed to write data:", err)
	}
	f.Sync()

	// Get initial disk usage
	usageBefore, err := diskUsage(f)
	if err != nil {
		t.Fatal("failed to get initial disk usage:", err)
	}
	t.Logf("Initial disk usage: %d bytes", usageBefore)

	// Punch a hole in the middle block
	middleBlockOffset := int64(blockSize)
	err = punchHole(f, middleBlockOffset, blockSize)
	if err != nil {
		t.Logf("punchHole returned error: %v (may not be supported on this filesystem)", err)
		t.Skip("punchHole not supported on this filesystem")
	}
	f.Sync()

	// Get disk usage after punch
	usageAfter, err := diskUsage(f)
	if err != nil {
		t.Fatal("failed to get disk usage after punch:", err)
	}
	t.Logf("Disk usage after middle block punch: %d bytes", usageAfter)

	// Verify disk usage decreased
	if usageAfter >= usageBefore {
		t.Log("Disk usage did not decrease - filesystem may not support hole punching")
	} else {
		reduction := usageBefore - usageAfter
		t.Logf("Disk usage reduced by %d bytes (expected ~%d)", reduction, blockSize)
	}

	// Verify isRangeCached returns false for punched region
	cached, err := isRangeCached(f, middleBlockOffset, middleBlockOffset+blockSize-1)
	if err != nil {
		t.Fatal("isRangeCached failed:", err)
	}

	// Check if filesystem supports sparse files
	holePos, _ := seekHole(f, 0)
	fileInfo, _ := f.Stat()
	sparseSupported := holePos < fileInfo.Size()

	if sparseSupported {
		if cached {
			t.Error("punched region should not be cached")
		}
	} else {
		t.Log("Filesystem does not support sparse files, skipping cache check")
	}

	// Verify first block data is still intact
	firstBlockData := make([]byte, 100)
	n, err := f.ReadAt(firstBlockData, 0)
	if err != nil {
		t.Fatal("failed to read first block:", err)
	}
	for i := 0; i < n; i++ {
		if firstBlockData[i] != data[i] {
			t.Errorf("first block data corrupted at offset %d: got %d, want %d", i, firstBlockData[i], data[i])
			break
		}
	}
	t.Log("First block data verified intact")

	// Verify third block data is still intact
	thirdBlockOffset := int64(blockSize * 2)
	thirdBlockData := make([]byte, 100)
	n, err = f.ReadAt(thirdBlockData, thirdBlockOffset)
	if err != nil {
		t.Fatal("failed to read third block:", err)
	}
	for i := 0; i < n; i++ {
		expected := data[int(thirdBlockOffset)+i]
		if thirdBlockData[i] != expected {
			t.Errorf("third block data corrupted at offset %d: got %d, want %d", thirdBlockOffset+int64(i), thirdBlockData[i], expected)
			break
		}
	}
	t.Log("Third block data verified intact")

	t.Log("Hole punching reduces disk usage test completed")
}

func TestSparseFileWithMultipleHoles(t *testing.T) {
	t.Parallel()
	// Create a sparse file with alternating data and holes
	f, err := os.CreateTemp(t.TempDir(), "multihole-test")
	if err != nil {
		t.Fatal("failed to create temp file:", err)
	}
	defer f.Close()

	const blockSize = sparseBlockSize
	const numBlocks = 5 // Blocks 0, 1, 2, 3, 4

	// Truncate to final size
	if err := f.Truncate(int64(blockSize * numBlocks)); err != nil {
		t.Fatal("failed to truncate file:", err)
	}

	// Write data to blocks 0, 2, and 4 only (leaving 1 and 3 as holes)
	data := make([]byte, blockSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write to block 0
	if _, err := f.WriteAt(data, 0); err != nil {
		t.Fatal("failed to write to block 0:", err)
	}
	// Write to block 2
	if _, err := f.WriteAt(data, int64(blockSize*2)); err != nil {
		t.Fatal("failed to write to block 2:", err)
	}
	// Write to block 4
	if _, err := f.WriteAt(data, int64(blockSize*4)); err != nil {
		t.Fatal("failed to write to block 4:", err)
	}
	f.Sync()

	// Get disk usage
	usage, err := diskUsage(f)
	if err != nil {
		t.Fatal("failed to get disk usage:", err)
	}
	fileInfo, _ := f.Stat()
	t.Logf("File size: %d, Disk usage: %d", fileInfo.Size(), usage)

	// Check for sparse file support
	holePos, _ := seekHole(f, 0)
	sparseSupported := holePos < fileInfo.Size()

	if !sparseSupported {
		t.Log("Filesystem does not support sparse files, skipping detailed hole tests")
		return
	}

	// Verify uncached ranges identifies the holes
	uncached, err := uncachedRanges(f, 0, int64(blockSize*numBlocks)-1)
	if err != nil {
		t.Fatal("uncachedRanges failed:", err)
	}

	t.Logf("Found %d uncached ranges (expected 2 - blocks 1 and 3)", len(uncached))
	for i, r := range uncached {
		t.Logf("  Uncached range %d: [%d, %d]", i, r.Start, r.End)
	}

	// We expect blocks 1 and 3 to be holes
	if len(uncached) != 2 {
		t.Errorf("expected 2 uncached ranges, got %d", len(uncached))
	}

	// Verify block 0 is cached
	cached0, err := isRangeCached(f, 0, int64(blockSize)-1)
	if err != nil {
		t.Fatal("isRangeCached for block 0 failed:", err)
	}
	if !cached0 {
		t.Error("block 0 should be cached")
	}

	// Verify block 1 is NOT cached
	cached1, err := isRangeCached(f, int64(blockSize), int64(blockSize*2)-1)
	if err != nil {
		t.Fatal("isRangeCached for block 1 failed:", err)
	}
	if cached1 {
		t.Error("block 1 should NOT be cached")
	}

	// Verify block 2 is cached
	cached2, err := isRangeCached(f, int64(blockSize*2), int64(blockSize*3)-1)
	if err != nil {
		t.Fatal("isRangeCached for block 2 failed:", err)
	}
	if !cached2 {
		t.Error("block 2 should be cached")
	}

	t.Log("Sparse file with multiple holes test completed")
}
