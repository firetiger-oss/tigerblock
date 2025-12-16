// Package s3 registers a secret manager backed by S3 storage buckets.
//
// The s3 backend uses the "s3://" prefix to identify secrets.
// Format: s3://bucket-name/secret-name
//
// Example usage:
//
//	import _ "github.com/firetiger-oss/storage/secret/s3"
//
//	value, info, err := secret.Get(ctx, "s3://my-secrets-bucket/database-password")
package s3

import (
	"context"
	"strings"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/secret"

	_ "github.com/firetiger-oss/storage/s3" // register s3 storage backend
)

type registry struct{}

func init() {
	secret.Register(`^s3://`, registry{})
}

func (registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	bucket, err := storage.LoadBucket(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return secret.NewManager(bucket), nil
}

func (registry) ParseSecret(identifier string) (managerID, secretName string, err error) {
	// Format: s3://bucket-name/secret-name
	// Split at the last slash to separate bucket from secret name
	if idx := strings.LastIndex(identifier, "/"); idx > len("s3://") {
		managerID = identifier[:idx]
		secretName = identifier[idx+1:]
	} else {
		managerID = identifier
	}
	return
}
