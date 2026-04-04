package fuse

import (
	"bytes"
	"context"
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
	f.info = info
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
		if size == 0 {
			if _, err := f.bucket.PutObject(ctx, f.key, bytes.NewReader(nil)); err != nil {
				return storageErr(err)
			}
			f.info.Size = 0
		} else if wh, ok := fh.(*writeHandle); ok {
			if err := wh.truncate(int64(size)); err != nil {
				return storageErr(err)
			}
			f.info.Size = int64(size)
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
		f.info = info
	}
	fillFileAttr(&out.Attr, f.info)
	return gofs.OK
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
