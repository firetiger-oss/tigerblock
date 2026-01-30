//go:build darwin

package file

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

const sparseBlockSize = 256 * 1024 // 256 KiB

const (
	// SEEK_HOLE and SEEK_DATA constants for macOS (not exported by x/sys/unix)
	seekHoleWhence = 3
	seekDataWhence = 4
)

// seekHole returns offset of next hole at or after offset (or file size if none)
func seekHole(f *os.File, offset int64) (int64, error) {
	return unix.Seek(int(f.Fd()), offset, seekHoleWhence)
}

// seekData returns offset of next data region at or after offset
func seekData(f *os.File, offset int64) (int64, error) {
	return unix.Seek(int(f.Fd()), offset, seekDataWhence)
}

// fpunchhole is the struct for F_PUNCHHOLE fcntl on macOS
type fpunchhole struct {
	Flags  uint32
	_      uint32
	Offset int64
	Length int64
}

// punchHole deallocates storage using F_PUNCHHOLE fcntl on macOS
func punchHole(f *os.File, offset, length int64) error {
	arg := fpunchhole{Offset: offset, Length: length}
	_, err := unix.FcntlInt(f.Fd(), unix.F_PUNCHHOLE, int(uintptr(unsafe.Pointer(&arg))))
	return err
}

// diskUsage returns actual bytes allocated on disk
func diskUsage(f *os.File) (int64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return 0, err
	}
	return stat.Blocks * 512, nil
}
