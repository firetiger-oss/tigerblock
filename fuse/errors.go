package fuse

import (
	"errors"
	"syscall"

	storage "github.com/firetiger-oss/storage"
)

func storageErr(err error) syscall.Errno {
	switch {
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
