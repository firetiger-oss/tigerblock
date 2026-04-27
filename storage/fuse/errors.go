package fuse

import (
	"errors"
	"syscall"

	gofs "github.com/hanwen/go-fuse/v2/fs"

	storage "github.com/firetiger-oss/tigerblock/storage"
)

func makeErrno(err error) syscall.Errno {
	switch {
	case err == nil:
		return gofs.OK
	case errors.Is(err, storage.ErrObjectNotFound):
		return syscall.ENOENT
	case errors.Is(err, storage.ErrBucketReadOnly):
		return syscall.EROFS
	case errors.Is(err, storage.ErrInvalidObjectKey):
		return syscall.EINVAL
	default:
		return syscall.EIO
	}
}
