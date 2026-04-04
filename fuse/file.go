package fuse

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	storage "github.com/firetiger-oss/storage"
)

// fileNode represents a regular file in the bucket. Its bucket is already
// scoped to the parent directory via storage.Prefix, so key is the local
// name within that directory.
type fileNode struct {
	gofs.Inode
	bucket storage.Bucket
	key    string
	mu     sync.RWMutex
	info   storage.ObjectInfo
}

var _ gofs.NodeGetattrer = (*fileNode)(nil)
var _ gofs.NodeOpener = (*fileNode)(nil)
var _ gofs.NodeSetattrer = (*fileNode)(nil)

func (f *fileNode) Getattr(ctx context.Context, fh gofs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	info, err := f.bucket.HeadObject(ctx, f.key)
	if err != nil {
		return storageErr(err)
	}
	f.mu.Lock()
	f.info = info
	f.mu.Unlock()
	fillFileAttr(&out.Attr, info)
	return gofs.OK
}

func (f *fileNode) Open(ctx context.Context, flags uint32) (gofs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return &readHandle{bucket: f.bucket, key: f.key}, gofuse.FOPEN_KEEP_CACHE, gofs.OK
	}
	wh, err := newWriteHandle(ctx, f.bucket, f.key, flags)
	if err != nil {
		return nil, 0, storageErr(err)
	}
	return wh, gofuse.FOPEN_DIRECT_IO, gofs.OK
}

func (f *fileNode) Setattr(ctx context.Context, fh gofs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	if size, ok := in.GetSize(); ok {
		if wh, ok := fh.(*writeHandle); ok {
			// Route all truncations through the write handle so the temp file
			// stays consistent and dirty is set appropriately.
			if err := wh.truncate(int64(size)); err != nil {
				return storageErr(err)
			}
			f.mu.Lock()
			f.info.Size = int64(size)
			f.mu.Unlock()
		} else if size == 0 {
			// No open write handle — update bucket directly.
			if _, err := f.bucket.PutObject(ctx, f.key, bytes.NewReader(nil)); err != nil {
				return storageErr(err)
			}
		} else {
			// No open write handle, size > 0: download, resize, and re-upload.
			if errno := f.truncateRemote(ctx, int64(size)); errno != gofs.OK {
				return errno
			}
		}
	}
	// Update f.info from the bucket only when no write handle is open (i.e. the
	// object exists in the bucket). When a write handle is open the object may
	// not be in the bucket yet, so we use the locally tracked f.info instead.
	if fh == nil {
		info, err := f.bucket.HeadObject(ctx, f.key)
		if err != nil {
			return storageErr(err)
		}
		f.mu.Lock()
		f.info = info
		f.mu.Unlock()
		fillFileAttr(&out.Attr, info)
	} else {
		f.mu.RLock()
		info := f.info
		f.mu.RUnlock()
		fillFileAttr(&out.Attr, info)
	}
	return gofs.OK
}

// truncateRemote downloads the object content, resizes it in a temporary file,
// and re-uploads it preserving the original object metadata. It handles both
// shrinking (keeping only the first size bytes) and extending (zero-filling the
// remainder). Called from Setattr when there is no open write handle.
func (f *fileNode) truncateRemote(ctx context.Context, size int64) syscall.Errno {
	tmp, err := os.CreateTemp("", "storage-fuse-truncate-*")
	if err != nil {
		return syscall.EIO
	}
	defer tmp.Close()
	defer os.Remove(tmp.Name())

	rc, info, err := f.bucket.GetObject(ctx, f.key)
	if err != nil {
		return storageErr(err)
	}
	// Copy at most size bytes. io.EOF means the source was shorter than size;
	// the remainder will be zero-filled by Truncate below.
	if _, err := io.CopyN(tmp, rc, size); err != nil && err != io.EOF {
		rc.Close()
		return syscall.EIO
	}
	rc.Close()

	// Zero-fill if size extends beyond existing content.
	if err := tmp.Truncate(size); err != nil {
		return syscall.EIO
	}
	if _, err := tmp.Seek(0, io.SeekStart); err != nil {
		return syscall.EIO
	}

	if _, err := f.bucket.PutObject(ctx, f.key, tmp, objectInfoToPutOptions(info)...); err != nil {
		return storageErr(err)
	}
	return gofs.OK
}

// objectInfoToPutOptions converts the metadata fields of an ObjectInfo into the
// corresponding PutOption values, used to preserve metadata on re-upload.
func objectInfoToPutOptions(info storage.ObjectInfo) []storage.PutOption {
	var opts []storage.PutOption
	if info.ContentType != "" {
		opts = append(opts, storage.ContentType(info.ContentType))
	}
	if info.ContentEncoding != "" {
		opts = append(opts, storage.ContentEncoding(info.ContentEncoding))
	}
	if info.CacheControl != "" {
		opts = append(opts, storage.CacheControl(info.CacheControl))
	}
	for k, v := range info.Metadata {
		opts = append(opts, storage.Metadata(k, v))
	}
	return opts
}

func fillFileAttr(a *gofuse.Attr, info storage.ObjectInfo) {
	a.Mode = syscall.S_IFREG | 0644
	a.Size = uint64(info.Size)
	a.Nlink = 1
	if !info.LastModified.IsZero() {
		t := info.LastModified
		a.SetTimes(nil, &t, nil)
	}
}

func fillDirAttr(a *gofuse.Attr) {
	a.Mode = syscall.S_IFDIR | 0755
	a.Nlink = 2
	now := time.Now()
	a.SetTimes(&now, &now, &now)
}
