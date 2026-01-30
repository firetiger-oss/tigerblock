//go:build linux

package file

import (
	"os"

	"golang.org/x/sys/unix"
)

const sparseBlockSize = 256 * 1024 // 256 KiB

// seekHole returns offset of next hole at or after offset (or file size if none)
func seekHole(f *os.File, offset int64) (int64, error) {
	return unix.Seek(int(f.Fd()), offset, unix.SEEK_HOLE)
}

// seekData returns offset of next data region at or after offset
func seekData(f *os.File, offset int64) (int64, error) {
	return unix.Seek(int(f.Fd()), offset, unix.SEEK_DATA)
}

// punchHole deallocates storage in range, creating a hole
func punchHole(f *os.File, offset, length int64) error {
	return unix.Fallocate(int(f.Fd()),
		unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
		offset, length)
}

// diskUsage returns actual bytes allocated on disk
func diskUsage(f *os.File) (int64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return 0, err
	}
	return stat.Blocks * 512, nil
}
