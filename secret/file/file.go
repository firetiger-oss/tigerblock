// Package file registers a secret manager backed by the local file system.
//
// The file backend uses the "file://" prefix to identify secrets.
// Format: file:///path/to/secrets/secret-name
//
// Example usage:
//
//	import _ "github.com/firetiger-oss/storage/secret/file"
//
//	value, info, err := secret.Get(ctx, "file:///var/secrets/database-password")
package file

import (
	"context"
	"strings"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/secret"

	_ "github.com/firetiger-oss/storage/file" // register file storage backend
)

type registry struct{}

func init() {
	secret.Register(`^file://`, registry{})
}

func (registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	bucket, err := storage.LoadBucket(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return secret.NewManager(bucket), nil
}

func (registry) ParseSecret(identifier string) (managerID, secretName string, err error) {
	// Format: file:///path/to/secrets/secret-name
	// Split at the last slash to separate directory from secret name
	if idx := strings.LastIndex(identifier, "/"); idx > len("file://") {
		managerID = identifier[:idx]
		secretName = identifier[idx+1:]
	} else {
		managerID = identifier
	}
	return
}
