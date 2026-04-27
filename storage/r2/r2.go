// Package r2 provides a Cloudflare R2 storage backend.
//
// R2 is S3-compatible, so this package uses the HTTP backend with
// SigV4 authentication for both request signing and presigned URLs.
//
// Usage:
//
//	import _ "github.com/firetiger-oss/tigerblock/storage/r2"
//
//	// Set credentials (R2 uses S3-compatible credentials)
//	// AWS_ACCESS_KEY_ID=<r2-access-key>
//	// AWS_SECRET_ACCESS_KEY=<r2-secret-key>
//	// CF_ACCOUNT_ID=<cloudflare-account-id>
//
//	bucket, err := storage.LoadBucket(ctx, "r2://my-bucket")
package r2

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/firetiger-oss/tigerblock/secret/authn/sigv4"
	"github.com/firetiger-oss/tigerblock/storage"
	storagehttp "github.com/firetiger-oss/tigerblock/storage/http"
	"github.com/firetiger-oss/tigerblock/uri"
)

func init() {
	storage.Register("r2", NewRegistry())
}

// ErrMissingAccountID is returned when neither CF_ACCOUNT_ID nor
// CLOUDFLARE_ACCOUNT_ID environment variable is set, and no account ID
// was provided via WithAccountID.
var ErrMissingAccountID = errors.New("r2: CF_ACCOUNT_ID or CLOUDFLARE_ACCOUNT_ID environment variable required")

// Option configures the R2 registry.
type Option func(*config)

type config struct {
	accountID string
}

// WithAccountID sets the Cloudflare account ID programmatically.
// If not set, falls back to CF_ACCOUNT_ID or CLOUDFLARE_ACCOUNT_ID env vars.
func WithAccountID(id string) Option {
	return func(c *config) {
		c.accountID = id
	}
}

// NewRegistry creates a storage registry for Cloudflare R2 buckets.
//
// The registry uses environment variables for configuration:
//   - CF_ACCOUNT_ID or CLOUDFLARE_ACCOUNT_ID: Cloudflare account ID (required)
//   - AWS_ACCESS_KEY_ID: R2 API token access key
//   - AWS_SECRET_ACCESS_KEY: R2 API token secret key
//
// Alternatively, use WithAccountID to set the account ID programmatically.
func NewRegistry(options ...Option) storage.Registry {
	var cfg config
	for _, opt := range options {
		opt(&cfg)
	}

	return storage.RegistryFunc(func(ctx context.Context, bucket string) (storage.Bucket, error) {
		accountID := cmp.Or(cfg.accountID, os.Getenv("CF_ACCOUNT_ID"), os.Getenv("CLOUDFLARE_ACCOUNT_ID"))
		if accountID == "" {
			return nil, ErrMissingAccountID
		}

		bucketName, prefix, _ := strings.Cut(bucket, "/")
		endpoint := fmt.Sprintf("https://%s.r2.cloudflarestorage.com/%s", accountID, bucketName)

		// Create SigV4 transport for request signing
		// R2 uses "s3" service and "auto" region for signing
		transport := sigv4.NewTransport(http.DefaultTransport,
			sigv4.WithService("s3"),
			sigv4.WithRegion("auto"),
		)

		// Create SigV4 signer for presigned URLs
		signer := sigv4.NewSigner(
			sigv4.WithService("s3"),
			sigv4.WithRegion("auto"),
		)

		var b storage.Bucket = &Bucket{
			inner: storagehttp.NewBucket(endpoint,
				storagehttp.WithClient(&http.Client{Transport: transport}),
				storagehttp.WithSigner(signer),
			),
			bucketName: bucketName,
		}
		if prefix != "" {
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
			return storage.Prefix(b, prefix), nil
		}
		return b, nil
	})
}

// Bucket wraps an HTTP bucket to provide the r2:// URI scheme.
type Bucket struct {
	inner      storage.Bucket
	bucketName string
}

// Location returns the bucket URI with the r2:// scheme.
func (b *Bucket) Location() string {
	return uri.Join("r2", b.bucketName)
}

// Access verifies that the bucket is accessible.
func (b *Bucket) Access(ctx context.Context) error {
	return b.inner.Access(ctx)
}

// Create creates a new bucket.
func (b *Bucket) Create(ctx context.Context) error {
	return b.inner.Create(ctx)
}

// HeadObject retrieves metadata about an object.
func (b *Bucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return b.inner.HeadObject(ctx, key)
}

// GetObject retrieves an object's contents and metadata.
func (b *Bucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	return b.inner.GetObject(ctx, key, options...)
}

// PutObject stores an object.
func (b *Bucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	return b.inner.PutObject(ctx, key, value, options...)
}

// DeleteObject removes an object.
func (b *Bucket) DeleteObject(ctx context.Context, key string) error {
	return b.inner.DeleteObject(ctx, key)
}

// DeleteObjects removes multiple objects.
func (b *Bucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return b.inner.DeleteObjects(ctx, objects)
}

// CopyObject copies an object within the bucket.
func (b *Bucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	return b.inner.CopyObject(ctx, from, to, options...)
}

// ListObjects lists objects in the bucket.
func (b *Bucket) ListObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return b.inner.ListObjects(ctx, options...)
}

// WatchObjects watches for object changes.
func (b *Bucket) WatchObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return b.inner.WatchObjects(ctx, options...)
}

// PresignGetObject generates a presigned URL for getting an object.
func (b *Bucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	return b.inner.PresignGetObject(ctx, key, expiration, options...)
}

// PresignPutObject generates a presigned URL for putting an object.
func (b *Bucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...storage.PutOption) (string, error) {
	return b.inner.PresignPutObject(ctx, key, expiration, options...)
}

// PresignHeadObject generates a presigned URL for getting object metadata.
func (b *Bucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return b.inner.PresignHeadObject(ctx, key, expiration)
}

// PresignDeleteObject generates a presigned URL for deleting an object.
func (b *Bucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return b.inner.PresignDeleteObject(ctx, key, expiration)
}

// Unwrap returns the underlying bucket.
func (b *Bucket) Unwrap() storage.Bucket {
	return b.inner
}
