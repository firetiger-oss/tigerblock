// Package http registers a secret manager backed by HTTP/HTTPS endpoints.
//
// The http backend uses the "http://" or "https://" prefix to identify secrets.
// Format: https://host/path/secret-name
//
// Note: HTTP-backed secret managers are read-only since the HTTP storage backend
// does not support write operations.
//
// Example usage:
//
//	import _ "github.com/firetiger-oss/storage/secret/http"
//
//	value, info, err := secret.Get(ctx, "https://secrets.example.com/database-password")
package http

import (
	"context"
	"strings"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/secret"

	_ "github.com/firetiger-oss/storage/http" // register http storage backend
)

type registry struct{}

func init() {
	secret.Register(`^https?://`, registry{})
}

func (registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	bucket, err := storage.LoadBucket(ctx, identifier)
	if err != nil {
		return nil, err
	}
	return secret.NewManager(bucket), nil
}

func (registry) ParseSecret(identifier string) (managerID, secretName string, err error) {
	// Format: https://host/path/secret-name or http://host/path/secret-name
	// Split at the last slash to separate base URL from secret name
	if idx := strings.LastIndex(identifier, "/"); idx > 0 {
		// Make sure we don't split on the scheme's slashes
		scheme := "http://"
		if strings.HasPrefix(identifier, "https://") {
			scheme = "https://"
		}
		if idx > len(scheme) {
			managerID = identifier[:idx]
			secretName = identifier[idx+1:]
			return
		}
	}
	managerID = identifier
	return
}
