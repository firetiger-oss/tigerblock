package file

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"golang.org/x/sys/unix"
)

func isErrAttrNotExist(err error) bool {
	return errors.Is(err, unix.ENODATA)
}

// renameIfNotExist atomically moves oldpath to newpath and fails if newpath
// already exists. It prefers renameat2(RENAME_NOREPLACE) and falls back to
// link(2) + unlink(2) when the filesystem or kernel rejects the flag
// (notably CephFS, and kernels older than 3.15).
//
// On target-exists the returned error satisfies errors.Is(err, fs.ErrExist).
// Any other error is returned as-is so callers can report it.
func renameIfNotExist(oldpath, newpath string) error {
	err := unix.Renameat2(unix.AT_FDCWD, oldpath, unix.AT_FDCWD, newpath, unix.RENAME_NOREPLACE)
	if err == nil {
		return nil
	}
	if !isRenameFlagUnsupported(err) {
		return err
	}
	return linkThenUnlink(oldpath, newpath)
}

// linkThenUnlink is the fallback for filesystems that reject
// renameat2(RENAME_NOREPLACE). link(2) is atomic and returns EEXIST when
// newpath already exists on every POSIX filesystem we support.
//
// This is not as strong as the renameat2 path: if the process dies between
// the link and the unlink the temp file remains on disk under its
// tempFilePattern suffix. When unlink fails after the new link is created,
// we best-effort remove newpath again so callers still see all-or-error
// semantics.
func linkThenUnlink(oldpath, newpath string) error {
	return linkThenUnlinkWithOps(oldpath, newpath, os.Link, os.Remove)
}

func linkThenUnlinkWithOps(oldpath, newpath string, link func(string, string) error, remove func(string) error) error {
	if err := link(oldpath, newpath); err != nil {
		return err
	}
	if err := remove(oldpath); err != nil {
		rollbackErr := remove(newpath)
		if rollbackErr != nil {
			return errors.Join(
				fmt.Errorf("unlink temp source after link: %w", err),
				fmt.Errorf("rollback published path: %w", rollbackErr),
			)
		}
		return fmt.Errorf("unlink temp source after link: %w", err)
	}
	return nil
}

func isRenameFlagUnsupported(err error) bool {
	return errors.Is(err, unix.EINVAL) ||
		errors.Is(err, unix.ENOSYS) ||
		errors.Is(err, unix.EOPNOTSUPP) ||
		errors.Is(err, fs.ErrInvalid)
}
