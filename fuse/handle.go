package fuse

import (
	"context"
	"io"
	"os"
	"syscall"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	storage "github.com/firetiger-oss/storage"
)

// readHandle is returned for O_RDONLY opens. Each Read call issues a ranged
// GetObject directly to the bucket — no in-memory buffering.
type readHandle struct {
	bucket storage.Bucket
	key    string
}

var _ gofs.FileReader = (*readHandle)(nil)

func (h *readHandle) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	end := off + int64(len(dest)) - 1
	rc, _, err := h.bucket.GetObject(ctx, h.key, storage.BytesRange(off, end))
	if err != nil {
		return nil, storageErr(err)
	}
	defer rc.Close()
	n, err := io.ReadFull(rc, dest)
	if err != nil && err != io.ErrUnexpectedEOF {
		return nil, syscall.EIO
	}
	return gofuse.ReadResultData(dest[:n]), gofs.OK
}

// writeHandle is returned for O_WRONLY / O_RDWR / O_CREAT opens. It uses a
// temporary file so that both reads and writes are consistent (copy-on-open
// model), and the file is uploaded to the bucket on Flush.
type writeHandle struct {
	bucket storage.Bucket
	key    string
	tmp    *os.File
	dirty  bool
}

var _ gofs.FileReader = (*writeHandle)(nil)
var _ gofs.FileWriter = (*writeHandle)(nil)
var _ gofs.FileFlusher = (*writeHandle)(nil)
var _ gofs.FileReleaser = (*writeHandle)(nil)

// newWriteHandle creates a write handle for key. If the open flags do not
// include O_TRUNC and the object already exists, its current content is
// downloaded into the temp file first so that reads and partial writes are
// consistent.
func newWriteHandle(ctx context.Context, bucket storage.Bucket, key string, flags uint32) (*writeHandle, error) {
	tmp, err := os.CreateTemp("", "storage-fuse-*")
	if err != nil {
		return nil, err
	}

	truncating := flags&uint32(syscall.O_TRUNC) != 0 || flags&uint32(syscall.O_CREAT) != 0
	if !truncating {
		rc, _, err := bucket.GetObject(ctx, key)
		if err == nil {
			if _, err = io.Copy(tmp, rc); err != nil {
				rc.Close()
				tmp.Close()
				os.Remove(tmp.Name())
				return nil, err
			}
			rc.Close()
			if _, err = tmp.Seek(0, io.SeekStart); err != nil {
				tmp.Close()
				os.Remove(tmp.Name())
				return nil, err
			}
		}
		// Object not found is fine for a new file; any other error is surfaced.
	}

	return &writeHandle{bucket: bucket, key: key, tmp: tmp}, nil
}

func (h *writeHandle) Read(ctx context.Context, dest []byte, off int64) (gofuse.ReadResult, syscall.Errno) {
	n, err := h.tmp.ReadAt(dest, off)
	if err != nil && err != io.EOF {
		return nil, syscall.EIO
	}
	return gofuse.ReadResultData(dest[:n]), gofs.OK
}

func (h *writeHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	n, err := h.tmp.WriteAt(data, off)
	if err != nil {
		return 0, syscall.EIO
	}
	h.dirty = true
	return uint32(n), gofs.OK
}

func (h *writeHandle) Flush(ctx context.Context) syscall.Errno {
	if !h.dirty {
		return gofs.OK
	}
	if _, err := h.tmp.Seek(0, io.SeekStart); err != nil {
		return syscall.EIO
	}
	if _, err := h.bucket.PutObject(ctx, h.key, h.tmp); err != nil {
		return storageErr(err)
	}
	h.dirty = false
	return gofs.OK
}

func (h *writeHandle) Release(ctx context.Context) syscall.Errno {
	errno := h.Flush(ctx)
	h.tmp.Close()
	os.Remove(h.tmp.Name())
	return errno
}

// truncate resizes the temp file to the given size.
func (h *writeHandle) truncate(size int64) error {
	return h.tmp.Truncate(size)
}
