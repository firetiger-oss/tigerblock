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
//	import _ "github.com/firetiger-oss/tigerblock/secret/http"
//
//	value, info, err := secret.Get(ctx, "https://secrets.example.com/database-password")
package http

import (
	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/secret/bucket"

	_ "github.com/firetiger-oss/tigerblock/storage/http" // register http storage backend
)

func init() {
	secret.Register("http:", bucket.Registry{})
	secret.Register("https:", bucket.Registry{})
}
