// Package gs registers a secret manager backed by Google Cloud Storage buckets.
//
// The gs backend uses the "gs://" prefix to identify secrets.
// Format: gs://bucket-name/secret-name
//
// Example usage:
//
//	import _ "github.com/firetiger-oss/tigerblock/secret/gs"
//
//	value, info, err := secret.Get(ctx, "gs://my-secrets-bucket/database-password")
package gs

import (
	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/secret/bucket"

	_ "github.com/firetiger-oss/tigerblock/storage/gs" // register gs storage backend
)

func init() {
	secret.Register("gs:", bucket.Registry{})
}
