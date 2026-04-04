package fuse

import (
	"context"
	"strings"
	"syscall"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	storage "github.com/firetiger-oss/storage"
)

// dirNode represents a virtual directory backed by a key prefix in the bucket.
// The bucket is scoped to this directory via storage.Prefix, so all operations
// use local (unprefixed) names.
type dirNode struct {
	gofs.Inode
	bucket storage.Bucket
}

var _ gofs.NodeLookuper = (*dirNode)(nil)
var _ gofs.NodeReaddirer = (*dirNode)(nil)
var _ gofs.NodeGetattrer = (*dirNode)(nil)
var _ gofs.NodeCreater = (*dirNode)(nil)
var _ gofs.NodeUnlinker = (*dirNode)(nil)

func (d *dirNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	// First check if name is a real object (file).
	info, err := d.bucket.HeadObject(ctx, name)
	if err == nil {
		child := d.NewInode(ctx, &fileNode{bucket: d.bucket, key: name},
			gofs.StableAttr{Mode: syscall.S_IFREG, Ino: pathIno(d.bucket, name)})
		fillFileAttr(&out.Attr, info)
		return child, gofs.OK
	}
	if makeErrno(err) != syscall.ENOENT {
		return nil, makeErrno(err)
	}

	// Not a file. Check whether it's a virtual directory by probing for any
	// object whose key starts with "name/".
	found := false
	for _, listErr := range d.bucket.ListObjects(ctx,
		storage.KeyPrefix(name+"/"),
		storage.KeyDelimiter("/"),
		storage.MaxKeys(1),
	) {
		if listErr != nil {
			return nil, makeErrno(listErr)
		}
		found = true
		break
	}
	if !found {
		return nil, syscall.ENOENT
	}

	sub := storage.Prefix(d.bucket, name+"/")
	child := d.NewInode(ctx, &dirNode{bucket: sub},
		gofs.StableAttr{Mode: syscall.S_IFDIR, Ino: pathIno(d.bucket, name+"/")})
	fillDirAttr(&out.Attr)
	return child, gofs.OK
}

func (d *dirNode) Readdir(ctx context.Context) (gofs.DirStream, syscall.Errno) {
	var entries []gofuse.DirEntry
	for obj, err := range d.bucket.ListObjects(ctx, storage.KeyDelimiter("/")) {
		if err != nil {
			return nil, makeErrno(err)
		}
		name := obj.Key
		if name, ok := strings.CutSuffix(name, "/"); ok {
			// Virtual directory (common prefix).
			entries = append(entries, gofuse.DirEntry{
				Mode: syscall.S_IFDIR,
				Name: name,
				Ino:  pathIno(d.bucket, name+"/"),
			})
		} else {
			entries = append(entries, gofuse.DirEntry{
				Mode: syscall.S_IFREG,
				Name: name,
				Ino:  pathIno(d.bucket, name),
			})
		}
	}
	return gofs.NewListDirStream(entries), gofs.OK
}

func (d *dirNode) Getattr(ctx context.Context, fh gofs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	fillDirAttr(&out.Attr)
	return gofs.OK
}

func (d *dirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*gofs.Inode, gofs.FileHandle, uint32, syscall.Errno) {
	wh, err := newWriteHandle(ctx, d.bucket, name, flags|syscall.O_CREAT|syscall.O_TRUNC)
	if err != nil {
		return nil, nil, 0, makeErrno(err)
	}
	node := &fileNode{bucket: d.bucket, key: name}
	child := d.NewInode(ctx, node, gofs.StableAttr{Mode: syscall.S_IFREG, Ino: pathIno(d.bucket, name)})
	fillFileAttr(&out.Attr, storage.ObjectInfo{})
	return child, wh, gofuse.FOPEN_DIRECT_IO, gofs.OK
}

func (d *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return makeErrno(d.bucket.DeleteObject(ctx, name))
}
