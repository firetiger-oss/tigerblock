package fuse

import (
	"bytes"
	"context"
	"io"
	"os"
	"syscall"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	storage "github.com/firetiger-oss/tigerblock/storage"
)

// fileNode represents a regular file in the bucket. Its bucket is already
// scoped to the parent directory via storage.Prefix, so key is the local
// name within that directory.
type fileNode struct {
	gofs.Inode
	bucket storage.Bucket
	key    string
	perms  *permissions
}

var _ gofs.NodeGetattrer = (*fileNode)(nil)
var _ gofs.NodeOpener = (*fileNode)(nil)
var _ gofs.NodeSetattrer = (*fileNode)(nil)

func (f *fileNode) Getattr(ctx context.Context, fh gofs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	info, err := f.bucket.HeadObject(ctx, f.key)
	if err != nil {
		return makeErrno(err)
	}
	fillFileAttr(&out.Attr, info, f.perms)
	return gofs.OK
}

func (f *fileNode) Open(ctx context.Context, flags uint32) (gofs.FileHandle, uint32, syscall.Errno) {
	if flags&syscall.O_ACCMODE == syscall.O_RDONLY {
		return &readHandle{bucket: f.bucket, key: f.key}, gofuse.FOPEN_KEEP_CACHE, gofs.OK
	}
	wh, err := newWriteHandle(ctx, f.bucket, f.key, flags)
	if err != nil {
		return nil, 0, makeErrno(err)
	}
	return wh, gofuse.FOPEN_DIRECT_IO, gofs.OK
}

func (f *fileNode) Setattr(ctx context.Context, fh gofs.FileHandle, in *gofuse.SetAttrIn, out *gofuse.AttrOut) syscall.Errno {
	var permOpts []storage.PutOption
	if mode, ok := in.GetMode(); ok {
		permOpts = append(permOpts, modePutOption(mode))
	}
	if uid, ok := in.GetUID(); ok {
		permOpts = append(permOpts, uidPutOption(uid))
	}
	if gid, ok := in.GetGID(); ok {
		permOpts = append(permOpts, gidPutOption(gid))
	}

	wh, hasHandle := fh.(*writeHandle)

	if size, sized := in.GetSize(); sized {
		switch {
		case hasHandle:
			// Route size changes through the write handle so the temp file stays
			// consistent; fold permission overrides into the pending options so
			// they're applied at Flush alongside the existing metadata.
			if err := wh.truncate(int64(size)); err != nil {
				return makeErrno(err)
			}
			wh.opts = append(wh.opts, permOpts...)
			permOpts = nil

		case size == 0:
			// Fast path: no content to preserve, but keep metadata + apply any
			// permission overrides in a single PUT.
			info, err := f.bucket.HeadObject(ctx, f.key)
			if err != nil {
				return makeErrno(err)
			}
			opts := append(objectInfoToPutOptions(info), permOpts...)
			if _, err := f.bucket.PutObject(ctx, f.key, bytes.NewReader(nil), opts...); err != nil {
				return makeErrno(err)
			}
			permOpts = nil

		default:
			// size > 0, no open write handle: download, resize, re-upload with
			// preserved metadata plus any permission overrides.
			if errno := f.truncateRemote(ctx, int64(size), permOpts); errno != gofs.OK {
				return errno
			}
			permOpts = nil
		}
	}

	if len(permOpts) > 0 {
		if hasHandle {
			wh.opts = append(wh.opts, permOpts...)
		} else if err := f.bucket.CopyObject(ctx, f.key, f.key, permOpts...); err != nil {
			return makeErrno(err)
		}
	}

	// Fill response attributes. When a write handle is open the object may not
	// be in the bucket yet, so read the size directly from the temp file.
	// Otherwise fetch fresh attributes from the bucket.
	if hasHandle {
		fi, err := wh.tmp.Stat()
		if err != nil {
			return syscall.EIO
		}
		// Synthesize an ObjectInfo so mode/uid/gid from the pending handle opts
		// aren't visible until Flush — Getattr on the open handle would see the
		// same thing. We fall back to defaults.
		fillFileAttr(&out.Attr, storage.ObjectInfo{Size: fi.Size()}, f.perms)
	} else {
		info, err := f.bucket.HeadObject(ctx, f.key)
		if err != nil {
			return makeErrno(err)
		}
		fillFileAttr(&out.Attr, info, f.perms)
	}
	return gofs.OK
}

// truncateRemote downloads the object content, resizes it in a temporary file,
// and re-uploads it preserving the original object metadata (plus any extra
// PutOptions, typically permission overrides from Setattr). Handles both
// shrinking (keeping only the first size bytes) and extending (zero-filling
// the remainder). Called from Setattr when there is no open write handle.
func (f *fileNode) truncateRemote(ctx context.Context, size int64, extra []storage.PutOption) syscall.Errno {
	tmp, err := os.CreateTemp("", ".fuse.*")
	if err != nil {
		return syscall.EIO
	}
	defer tmp.Close()
	defer os.Remove(tmp.Name())

	rc, info, err := f.bucket.GetObject(ctx, f.key)
	if err != nil {
		return makeErrno(err)
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

	opts := append(objectInfoToPutOptions(info), extra...)
	if _, err := f.bucket.PutObject(ctx, f.key, tmp, opts...); err != nil {
		return makeErrno(err)
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

func fillFileAttr(a *gofuse.Attr, info storage.ObjectInfo, perms *permissions) {
	a.Mode = syscall.S_IFREG | readMode(info.Metadata, perms.fileMode)
	a.Size = uint64(info.Size)
	a.Nlink = 1
	a.Owner.Uid = readID(info.Metadata, metadataKeyUID, perms.uid)
	a.Owner.Gid = readID(info.Metadata, metadataKeyGID, perms.gid)
	if !info.LastModified.IsZero() {
		t := info.LastModified
		a.SetTimes(nil, &t, nil)
	}
}

func fillDirAttr(a *gofuse.Attr, info storage.ObjectInfo, perms *permissions) {
	a.Mode = syscall.S_IFDIR | readMode(info.Metadata, perms.dirMode)
	a.Nlink = 2
	a.Owner.Uid = readID(info.Metadata, metadataKeyUID, perms.uid)
	a.Owner.Gid = readID(info.Metadata, metadataKeyGID, perms.gid)
	if !info.LastModified.IsZero() {
		t := info.LastModified
		a.SetTimes(nil, &t, nil)
	} else {
		now := time.Now()
		a.SetTimes(&now, &now, &now)
	}
}
