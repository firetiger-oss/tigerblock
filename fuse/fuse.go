// Package fuse provides a FUSE filesystem backed by a storage.Bucket.
//
// Object keys are mapped to POSIX filesystem paths: a key like "dir/sub/file.txt"
// appears as the file /dir/sub/file.txt with virtual directory nodes for "dir"
// and "dir/sub" synthesized from the key prefix structure.
//
// Permissions (mode, uid, gid) are persisted per-object in user metadata under
// the keys "mode", "uid" and "gid" — the same convention s3fs/goofys/rclone
// use — so files created by those tools are interoperable. Directories can
// persist their permissions by carrying a zero-byte marker object at the
// "foo/" key; without a marker, directories fall back to the Mount-level
// defaults.
package fuse

import (
	"hash/fnv"
	"io"
	"io/fs"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	storage "github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/uri"
)

// MountConfig is the resolved configuration built up by MountOption values
// passed to Mount. It embeds go-fuse's Options struct so every field there
// remains reachable — tweak it via FuseOption.
type MountConfig struct {
	gofs.Options

	// FileMode is the default permission mode applied to regular files whose
	// metadata does not specify one. Only the permission + sticky/setuid/setgid
	// bits are honored. Defaults to 0o644.
	FileMode fs.FileMode

	// DirMode is the default permission mode applied to directories whose
	// metadata (or marker object) does not specify one. Only the permission +
	// sticky/setuid/setgid bits are honored. Defaults to 0o755.
	DirMode fs.FileMode

	// UID is the default owner applied to objects whose metadata does not
	// specify one. Defaults to 0.
	UID uint32

	// GID is the default group applied to objects whose metadata does not
	// specify one. Defaults to 0.
	GID uint32
}

// MountOption mutates a MountConfig.
type MountOption func(*MountConfig)

// FileMode sets the default permission mode for regular files.
func FileMode(mode fs.FileMode) MountOption {
	return func(c *MountConfig) { c.FileMode = mode }
}

// DirMode sets the default permission mode for directories.
func DirMode(mode fs.FileMode) MountOption {
	return func(c *MountConfig) { c.DirMode = mode }
}

// UID sets the default owner UID for objects.
func UID(uid uint32) MountOption {
	return func(c *MountConfig) { c.UID = uid }
}

// GID sets the default group GID for objects.
func GID(gid uint32) MountOption {
	return func(c *MountConfig) { c.GID = gid }
}

// FuseOption injects a go-fuse Options mutation, so callers don't lose access
// to fields like MountOptions.Debug, MountOptions.AllowOther, etc.
func FuseOption(opt func(*gofs.Options)) MountOption {
	return func(c *MountConfig) { opt(&c.Options) }
}

// Mount mounts bucket at dir. The returned server controls the lifecycle;
// call server.Unmount() followed by server.Wait() to tear down.
func Mount(dir string, bucket storage.Bucket, opts ...MountOption) (*gofuse.Server, error) {
	cfg := &MountConfig{
		FileMode: 0o644,
		DirMode:  0o755,
	}
	for _, opt := range opts {
		opt(cfg)
	}
	perms := &permissions{
		fileMode: modeBitsFromFS(cfg.FileMode),
		dirMode:  modeBitsFromFS(cfg.DirMode),
		uid:      cfg.UID,
		gid:      cfg.GID,
	}
	return gofs.Mount(dir, &dirNode{root: bucket, bucket: bucket, perms: perms}, &cfg.Options)
}

// pathIno produces a stable inode number from the fully-qualified object URI.
// Using uri.Split + uri.Join ensures the hash input is canonical regardless of
// how the bucket's Location string is formatted (e.g. trailing slashes), so
// two nodes that refer to the same object always get the same inode number.
func pathIno(bucket storage.Bucket, name string) uint64 {
	scheme, location, path := uri.Split(bucket.Location())
	objectURI := uri.Join(scheme, location, path, name)
	h := fnv.New64a()
	io.WriteString(h, objectURI)
	return h.Sum64()
}
