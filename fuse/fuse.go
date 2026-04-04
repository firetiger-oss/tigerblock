// Package fuse provides a FUSE filesystem backed by a storage.Bucket.
//
// Object keys are mapped to POSIX filesystem paths: a key like "dir/sub/file.txt"
// appears as the file /dir/sub/file.txt with virtual directory nodes for "dir"
// and "dir/sub" synthesized from the key prefix structure.
package fuse

import (
	"hash/fnv"
	"io"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	gofuse "github.com/hanwen/go-fuse/v2/fuse"

	storage "github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/uri"
)

// Mount mounts bucket at dir. The returned server controls the lifecycle;
// call server.Unmount() followed by server.Wait() to tear down.
func Mount(dir string, bucket storage.Bucket, opts *gofs.Options) (*gofuse.Server, error) {
	return gofs.Mount(dir, &dirNode{bucket: bucket}, opts)
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
