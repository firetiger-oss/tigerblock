package file

import (
	"errors"

	"golang.org/x/sys/unix"
)

func isErrAttrNotExist(err error) bool {
	return errors.Is(err, unix.ENOATTR)
}

func renameIfNotExist(oldpath, newpath string) error {
	return unix.RenamexNp(oldpath, newpath, unix.RENAME_EXCL)
}
