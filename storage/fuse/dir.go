package fuse

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"syscall"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	storage "github.com/firetiger-oss/tigerblock/storage"
)

// dirNode represents a directory in the bucket, backed by a key prefix.
// bucket is scoped to this directory via storage.Prefix so child operations
// use local (unprefixed) names. root is the unprefixed bucket passed to Mount
// along with dirKey (this directory's own "foo/bar/" path) so Getattr can
// look up the directory's marker object without needing to pass an empty key
// through the prefix adapter. Persistent permissions for a directory live
// on the "foo/" marker object — the same convention S3, GCS and s3fs use.
type dirNode struct {
	gofs.Inode
	root   storage.Bucket // unprefixed bucket passed to Mount
	bucket storage.Bucket // prefix-scoped view for child operations
	dirKey string         // this directory's full key in root ("" for root dir)
	perms  *permissions
}

var _ gofs.NodeLookuper = (*dirNode)(nil)
var _ gofs.NodeReaddirer = (*dirNode)(nil)
var _ gofs.NodeGetattrer = (*dirNode)(nil)
var _ gofs.NodeCreater = (*dirNode)(nil)
var _ gofs.NodeUnlinker = (*dirNode)(nil)
var _ gofs.NodeMkdirer = (*dirNode)(nil)
var _ gofs.NodeRmdirer = (*dirNode)(nil)

func (d *dirNode) Lookup(ctx context.Context, name string, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	// First check if name is a real object (file).
	info, err := d.bucket.HeadObject(ctx, name)
	if err == nil {
		child := d.NewInode(ctx, &fileNode{bucket: d.bucket, key: name, perms: d.perms},
			gofs.StableAttr{Mode: syscall.S_IFREG, Ino: pathIno(d.bucket, name)})
		fillFileAttr(&out.Attr, info, d.perms)
		return child, gofs.OK
	}
	if makeErrno(err) != syscall.ENOENT {
		return nil, makeErrno(err)
	}

	// Not a file. Try the explicit directory marker first; if it exists we
	// can take its permissions as persistent state and skip the list probe.
	if markerInfo, err := d.bucket.HeadObject(ctx, name+"/"); err == nil {
		return d.newDirInode(ctx, name, markerInfo, out), gofs.OK
	} else if makeErrno(err) != syscall.ENOENT {
		return nil, makeErrno(err)
	}

	// No marker — fall back to detecting a virtual directory by probing for
	// any child under "name/".
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
	return d.newDirInode(ctx, name, storage.ObjectInfo{}, out), gofs.OK
}

func (d *dirNode) newDirInode(ctx context.Context, name string, markerInfo storage.ObjectInfo, out *gofuse.EntryOut) *gofs.Inode {
	sub := storage.Prefix(d.bucket, name+"/")
	childKey := d.dirKey + name + "/"
	child := d.NewInode(ctx, &dirNode{root: d.root, bucket: sub, dirKey: childKey, perms: d.perms},
		gofs.StableAttr{Mode: syscall.S_IFDIR, Ino: pathIno(d.bucket, name+"/")})
	fillDirAttr(&out.Attr, markerInfo, d.perms)
	return child
}

func (d *dirNode) Readdir(ctx context.Context) (gofs.DirStream, syscall.Errno) {
	var entries []gofuse.DirEntry
	seen := make(map[string]struct{})
	for obj, err := range d.bucket.ListObjects(ctx, storage.KeyDelimiter("/")) {
		if err != nil {
			return nil, makeErrno(err)
		}
		name := obj.Key
		if dirName, ok := strings.CutSuffix(name, "/"); ok {
			if _, ok := seen[dirName]; ok {
				continue
			}
			seen[dirName] = struct{}{}
			entries = append(entries, gofuse.DirEntry{
				Mode: syscall.S_IFDIR,
				Name: dirName,
				Ino:  pathIno(d.bucket, dirName+"/"),
			})
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		entries = append(entries, gofuse.DirEntry{
			Mode: syscall.S_IFREG,
			Name: name,
			Ino:  pathIno(d.bucket, name),
		})
	}
	return gofs.NewListDirStream(entries), gofs.OK
}

func (d *dirNode) Getattr(ctx context.Context, fh gofs.FileHandle, out *gofuse.AttrOut) syscall.Errno {
	// Look up this directory's marker via the unprefixed root bucket. The
	// root directory has no marker (its key is ""); sub-directories carry
	// their full "foo/bar/" path in dirKey.
	var info storage.ObjectInfo
	if d.dirKey != "" {
		if got, err := d.root.HeadObject(ctx, d.dirKey); err == nil {
			info = got
		}
	}
	fillDirAttr(&out.Attr, info, d.perms)
	return gofs.OK
}

func (d *dirNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *gofuse.EntryOut) (*gofs.Inode, gofs.FileHandle, uint32, syscall.Errno) {
	opts := createPermOpts(ctx, mode)
	wh, err := newWriteHandle(ctx, d.bucket, name, flags|syscall.O_CREAT|syscall.O_TRUNC, opts...)
	if err != nil {
		return nil, nil, 0, makeErrno(err)
	}
	node := &fileNode{bucket: d.bucket, key: name, perms: d.perms}
	child := d.NewInode(ctx, node, gofs.StableAttr{Mode: syscall.S_IFREG, Ino: pathIno(d.bucket, name)})
	fillFileAttr(&out.Attr, storage.ObjectInfo{Metadata: permMetadataFromOpts(opts)}, d.perms)
	return child, wh, gofuse.FOPEN_DIRECT_IO, gofs.OK
}

func (d *dirNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return makeErrno(d.bucket.DeleteObject(ctx, name))
}

// Mkdir persists a directory by writing a zero-byte marker object at "name/"
// carrying mode/uid/gid metadata.
func (d *dirNode) Mkdir(ctx context.Context, name string, mode uint32, out *gofuse.EntryOut) (*gofs.Inode, syscall.Errno) {
	// Refuse if a regular object or marker with that name already exists.
	if _, err := d.bucket.HeadObject(ctx, name); err == nil {
		return nil, syscall.EEXIST
	} else if makeErrno(err) != syscall.ENOENT {
		return nil, makeErrno(err)
	}
	if _, err := d.bucket.HeadObject(ctx, name+"/"); err == nil {
		return nil, syscall.EEXIST
	} else if makeErrno(err) != syscall.ENOENT {
		return nil, makeErrno(err)
	}
	// Also refuse if a virtual directory already exists (children under the
	// prefix without an explicit marker).
	for _, listErr := range d.bucket.ListObjects(ctx,
		storage.KeyPrefix(name+"/"),
		storage.KeyDelimiter("/"),
		storage.MaxKeys(1),
	) {
		if listErr != nil {
			return nil, makeErrno(listErr)
		}
		return nil, syscall.EEXIST
	}

	opts := createPermOpts(ctx, mode)
	if _, err := d.bucket.PutObject(ctx, name+"/", bytes.NewReader(nil), opts...); err != nil {
		return nil, makeErrno(err)
	}

	sub := storage.Prefix(d.bucket, name+"/")
	childKey := d.dirKey + name + "/"
	child := d.NewInode(ctx, &dirNode{root: d.root, bucket: sub, dirKey: childKey, perms: d.perms},
		gofs.StableAttr{Mode: syscall.S_IFDIR, Ino: pathIno(d.bucket, name+"/")})
	fillDirAttr(&out.Attr, storage.ObjectInfo{Metadata: permMetadataFromOpts(opts)}, d.perms)
	return child, gofs.OK
}

// Rmdir removes a directory. The directory must be empty apart from its
// marker — any other child returns ENOTEMPTY. The marker (if present) is
// removed. Rmdir on a path that is not an existing directory returns ENOENT.
func (d *dirNode) Rmdir(ctx context.Context, name string) syscall.Errno {
	sawAny := false
	for obj, err := range d.bucket.ListObjects(ctx,
		storage.KeyPrefix(name+"/"),
		storage.MaxKeys(2),
	) {
		if err != nil {
			return makeErrno(err)
		}
		sawAny = true
		if obj.Key != name+"/" {
			return syscall.ENOTEMPTY
		}
	}
	if !sawAny {
		return syscall.ENOENT
	}
	if err := d.bucket.DeleteObject(ctx, name+"/"); err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
		return makeErrno(err)
	}
	return gofs.OK
}

// createPermOpts builds PutOptions carrying the caller-supplied mode and the
// caller's uid/gid (from the FUSE context) if available.
func createPermOpts(ctx context.Context, mode uint32) []storage.PutOption {
	opts := []storage.PutOption{modePutOption(mode)}
	if caller, ok := gofuse.FromContext(ctx); ok {
		opts = append(opts, uidPutOption(caller.Uid), gidPutOption(caller.Gid))
	}
	return opts
}

// permMetadataFromOpts reverse-engineers the permissions-related metadata map
// from a slice of PutOptions so we can synthesize an ObjectInfo that mirrors
// what the next PutObject will persist. Only mode/uid/gid keys survive this
// round trip, which is all fillFileAttr/fillDirAttr look at.
func permMetadataFromOpts(opts []storage.PutOption) map[string]string {
	put := storage.NewPutOptions(opts...)
	return put.Metadata()
}
