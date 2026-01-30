package r2

import (
	"context"
	"os"
	"testing"

	"github.com/firetiger-oss/storage"
)

func TestBucketLocation(t *testing.T) {
	// Save and restore environment
	origCF := os.Getenv("CF_ACCOUNT_ID")
	origCloudflare := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	defer func() {
		os.Setenv("CF_ACCOUNT_ID", origCF)
		os.Setenv("CLOUDFLARE_ACCOUNT_ID", origCloudflare)
	}()

	os.Setenv("CF_ACCOUNT_ID", "test-account-id")
	os.Unsetenv("CLOUDFLARE_ACCOUNT_ID")

	bucket := &Bucket{
		bucketName: "my-bucket",
	}

	location := bucket.Location()
	if location != "r2://my-bucket" {
		t.Errorf("expected r2://my-bucket, got %s", location)
	}
}

func TestGetAccountID(t *testing.T) {
	// Save and restore environment
	origCF := os.Getenv("CF_ACCOUNT_ID")
	origCloudflare := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	defer func() {
		os.Setenv("CF_ACCOUNT_ID", origCF)
		os.Setenv("CLOUDFLARE_ACCOUNT_ID", origCloudflare)
	}()

	tests := []struct {
		name         string
		cfID         string
		cloudflareID string
		expectedID   string
	}{
		{
			name:         "CF_ACCOUNT_ID takes precedence",
			cfID:         "cf-account",
			cloudflareID: "cloudflare-account",
			expectedID:   "cf-account",
		},
		{
			name:         "CLOUDFLARE_ACCOUNT_ID as fallback",
			cfID:         "",
			cloudflareID: "cloudflare-account",
			expectedID:   "cloudflare-account",
		},
		{
			name:         "neither set returns empty",
			cfID:         "",
			cloudflareID: "",
			expectedID:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.cfID != "" {
				os.Setenv("CF_ACCOUNT_ID", tt.cfID)
			} else {
				os.Unsetenv("CF_ACCOUNT_ID")
			}
			if tt.cloudflareID != "" {
				os.Setenv("CLOUDFLARE_ACCOUNT_ID", tt.cloudflareID)
			} else {
				os.Unsetenv("CLOUDFLARE_ACCOUNT_ID")
			}

			id := getAccountID()
			if id != tt.expectedID {
				t.Errorf("expected %q, got %q", tt.expectedID, id)
			}
		})
	}
}

func TestRegistryMissingAccountID(t *testing.T) {
	// Save and restore environment
	origCF := os.Getenv("CF_ACCOUNT_ID")
	origCloudflare := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	defer func() {
		os.Setenv("CF_ACCOUNT_ID", origCF)
		os.Setenv("CLOUDFLARE_ACCOUNT_ID", origCloudflare)
	}()

	os.Unsetenv("CF_ACCOUNT_ID")
	os.Unsetenv("CLOUDFLARE_ACCOUNT_ID")

	registry := NewRegistry()
	_, err := registry.LoadBucket(context.Background(), "test-bucket")
	if err != ErrMissingAccountID {
		t.Errorf("expected ErrMissingAccountID, got %v", err)
	}
}

func TestRegistration(t *testing.T) {
	// Verify that the r2 scheme is registered
	// Save and restore environment
	origCF := os.Getenv("CF_ACCOUNT_ID")
	origCloudflare := os.Getenv("CLOUDFLARE_ACCOUNT_ID")
	defer func() {
		os.Setenv("CF_ACCOUNT_ID", origCF)
		os.Setenv("CLOUDFLARE_ACCOUNT_ID", origCloudflare)
	}()

	os.Unsetenv("CF_ACCOUNT_ID")
	os.Unsetenv("CLOUDFLARE_ACCOUNT_ID")

	// Try to load a bucket with r2:// scheme
	// This will fail due to missing account ID, but the error message
	// should not indicate that the scheme is unknown
	_, err := storage.LoadBucket(context.Background(), "r2://test-bucket")
	if err == nil {
		t.Error("expected error, got nil")
	}
	if err == ErrMissingAccountID {
		// Good - the scheme is registered and we're getting the expected error
		return
	}
	// Check if the error indicates the scheme is registered
	if err.Error() == "r2://test-bucket: bucket not found (did you forget the import?)" {
		t.Error("r2 scheme is not registered")
	}
}
