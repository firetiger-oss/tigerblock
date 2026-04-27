// Package s3 registers a secret manager backed by S3 storage buckets.
//
// The s3 backend uses the "s3://" prefix to identify secrets.
// Format: s3://bucket-name/secret-name
//
// Example usage:
//
//	import _ "github.com/firetiger-oss/tigerblock/secret/s3"
//
//	value, info, err := secret.Get(ctx, "s3://my-secrets-bucket/database-password")
package s3

import (
	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/secret/bucket"

	_ "github.com/firetiger-oss/tigerblock/storage/s3" // register s3 storage backend
)

func init() {
	secret.Register("s3:", bucket.Registry{})
}
