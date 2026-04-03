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
)

// Mount mounts bucket at dir. The returned server controls the lifecycle;
// call server.Unmount() followed by server.Wait() to tear down.
func Mount(dir string, bucket storage.Bucket, opts *gofs.Options) (*gofuse.Server, error) {
	return gofs.Mount(dir, &dirNode{bucket: bucket}, opts)
}

// pathIno produces a stable inode number by hashing the full URI of the
// scoped bucket plus the entry name. bucket.Location() for a prefixed bucket
// returns the full URI including accumulated prefix, so the result is unique
// across nested directories even when filenames repeat.
func pathIno(bucket storage.Bucket, name string) uint64 {
	h := fnv.New64a()
	io.WriteString(h, bucket.Location())
	io.WriteString(h, name)
	return h.Sum64()
}
