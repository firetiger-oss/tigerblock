package fuse

import (
	"io/fs"
	"strconv"

	storage "github.com/firetiger-oss/storage"
)

// Metadata keys used to persist POSIX permissions on objects. The same names
// are used by s3fs, goofys and rclone, so files created by any of those tools
// carry their permissions over into mounts of this package (and vice versa).
const (
	metadataKeyMode = "mode"
	metadataKeyUID  = "uid"
	metadataKeyGID  = "gid"
)

// permissions is the resolved, read-only view of Mount-level defaults. Nodes
// carry a pointer to the same value, so there's no copy per lookup.
type permissions struct {
	fileMode uint32 // 12-bit (0o7777) permission bits
	dirMode  uint32 // 12-bit (0o7777) permission bits
	uid      uint32
	gid      uint32
}

// modeBitsFromFS collapses an io/fs.FileMode's permission + sticky/setuid/setgid
// flags into the single 12-bit syscall representation.
func modeBitsFromFS(m fs.FileMode) uint32 {
	bits := uint32(m.Perm())
	if m&fs.ModeSetuid != 0 {
		bits |= 0o4000
	}
	if m&fs.ModeSetgid != 0 {
		bits |= 0o2000
	}
	if m&fs.ModeSticky != 0 {
		bits |= 0o1000
	}
	return bits
}

// readMode returns the 12-bit permission value to use for an object, falling
// back to the default when metadata is missing or malformed.
func readMode(m map[string]string, fallback uint32) uint32 {
	if v, ok := m[metadataKeyMode]; ok {
		if parsed, err := strconv.ParseUint(v, 8, 32); err == nil {
			return uint32(parsed) & 0o7777
		}
	}
	return fallback & 0o7777
}

// readID parses a decimal uid/gid from metadata, falling back to the default
// on missing or malformed values.
func readID(m map[string]string, key string, fallback uint32) uint32 {
	if v, ok := m[key]; ok {
		if parsed, err := strconv.ParseUint(v, 10, 32); err == nil {
			return uint32(parsed)
		}
	}
	return fallback
}

// modePutOption encodes a 12-bit permission value as a PutOption.
func modePutOption(mode uint32) storage.PutOption {
	return storage.Metadata(metadataKeyMode, strconv.FormatUint(uint64(mode&0o7777), 8))
}

// uidPutOption encodes a uid as a PutOption.
func uidPutOption(uid uint32) storage.PutOption {
	return storage.Metadata(metadataKeyUID, strconv.FormatUint(uint64(uid), 10))
}

// gidPutOption encodes a gid as a PutOption.
func gidPutOption(gid uint32) storage.PutOption {
	return storage.Metadata(metadataKeyGID, strconv.FormatUint(uint64(gid), 10))
}
