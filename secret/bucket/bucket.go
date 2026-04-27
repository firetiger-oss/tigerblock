// Package bucket provides a secret.Registry implementation backed by storage buckets.
//
// This package is used by storage-backed secret backends (s3, gs, file, http) to
// provide a common implementation for loading secret managers from bucket URIs.
package bucket

import (
	"context"

	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/uri"
)

// Registry implements secret.Registry for storage bucket-backed secret managers.
type Registry struct{}

// LoadManager loads a secret manager backed by the storage bucket identified by
// the given URI. The URI path component is stripped; only scheme and location
// are used to load the bucket.
func (Registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	scheme, location, _ := uri.Split(identifier)
	bucket, err := storage.LoadBucket(ctx, uri.Join(scheme, location))
	if err != nil {
		return nil, err
	}
	return secret.NewManager(bucket), nil
}

// ParseSecret extracts the manager ID (bucket URI) and secret name from a full
// secret identifier.
//
// For example, "s3://my-bucket/my-secret" returns:
//   - managerID: "s3://my-bucket"
//   - secretName: "my-secret"
//   - version: "" (bucket-backed secrets don't support versions in identifiers)
func (Registry) ParseSecret(identifier string) (managerID, secretName, version string, err error) {
	scheme, location, secretName := uri.Split(identifier)
	managerID = uri.Join(scheme, location)
	return
}
