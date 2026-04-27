// Package file registers a secret manager backed by the local file system.
//
// The file backend uses the "file://" prefix to identify secrets.
// Format: file:///path/to/secrets/secret-name
//
// Example usage:
//
//	import _ "github.com/firetiger-oss/tigerblock/secret/file"
//
//	value, info, err := secret.Get(ctx, "file:///var/secrets/database-password")
package file

import (
	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/secret/bucket"

	_ "github.com/firetiger-oss/tigerblock/storage/file" // register file storage backend
)

func init() {
	secret.Register("file:", bucket.Registry{})
}
