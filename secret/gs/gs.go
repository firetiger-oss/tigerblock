// Package gs registers a secret manager backed by Google Cloud Storage buckets.
//
// The gs backend uses the "gs://" prefix to identify secrets.
// Format: gs://bucket-name/secret-name
//
// Example usage:
//
//	import _ "github.com/firetiger-oss/storage/secret/gs"
//
//	value, info, err := secret.Get(ctx, "gs://my-secrets-bucket/database-password")
package gs

import (
	"context"
	"strings"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/secret"

	_ "github.com/firetiger-oss/storage/gs" // register gs storage backend
)

type registry struct{}

func init() {
	secret.Register(`^gs://`, registry{})
}

func (registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	bucket, err := storage.LoadBucket(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return secret.NewManager(bucket), nil
}

func (registry) ParseSecret(identifier string) (managerID, secretName string, err error) {
	// Format: gs://bucket-name/secret-name
	// Split at the last slash to separate bucket from secret name
	if idx := strings.LastIndex(identifier, "/"); idx > len("gs://") {
		managerID = identifier[:idx]
		secretName = identifier[idx+1:]
	} else {
		managerID = identifier
	}
	return
}
