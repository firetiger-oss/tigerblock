// Package secret provides a unified interface for secret management across
// different cloud providers (AWS Secrets Manager, GCP Secret Manager) and
// local environments.
//
// The package follows the same architectural patterns as the storage package,
// using a Manager interface analogous to storage.Bucket, with support for
// multiple backends registered via the registry pattern.
//
// Example usage:
//
//	// Load a manager from a URI
//	mgr, err := secret.LoadManager(ctx, "secret://aws/us-east-1")
//	if err != nil {
//		return err
//	}
//
//	// Create a secret
//	info, err := mgr.Create(ctx, "db-password", secret.Value("secret-value"),
//		secret.Tag("env", "production"),
//		secret.Description("Database password"))
//	if err != nil {
//		return err
//	}
//
//	// Retrieve a secret
//	value, info, err := mgr.Get(ctx, "db-password")
//	if err != nil {
//		return err
//	}
//
//	// List secrets
//	for s, err := range mgr.List(ctx, secret.NamePrefix("db-")) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(s.Name)
//	}
package secret

import (
	"context"
	"errors"
	"iter"
	"time"
)

// Value holds secret data with a safe string representation to prevent
// accidental logging or printing of sensitive data.
type Value []byte

// String returns "REDACTED" to prevent accidental exposure of secret values.
func (v Value) String() string { return "REDACTED" }

// GoString returns a safe representation for %#v formatting.
func (v Value) GoString() string { return `secret.Value("REDACTED")` }

// Manager is the primary interface for secret management operations.
// It provides CRUD operations, listing, and versioning capabilities.
//
// Manager implementations are provided for:
//   - AWS Secrets Manager (secret://aws/REGION)
//   - GCP Secret Manager (secret://gcp/PROJECT_ID)
//   - Environment Variables (secret://env)
type Manager interface {
	// CreateSecret creates a new secret with the given name and value.
	// Returns ErrAlreadyExists if a secret with this name already exists.
	CreateSecret(ctx context.Context, name string, value Value, options ...CreateOption) (Info, error)

	// GetSecret retrieves the latest version of a secret by name.
	// Returns the secret value and metadata.
	// Returns ErrNotFound if the secret does not exist.
	GetSecret(ctx context.Context, name string, options ...GetOption) (Value, Info, error)

	// UpdateSecret updates an existing secret with a new value.
	// This may create a new version depending on the backend implementation.
	// Returns ErrNotFound if the secret does not exist.
	UpdateSecret(ctx context.Context, name string, value Value, options ...UpdateOption) (Info, error)

	// DeleteSecret deletes a secret.
	// Depending on the backend, this may be a soft delete with a recovery period.
	// Returns ErrNotFound if the secret does not exist.
	DeleteSecret(ctx context.Context, name string) error

	// ListSecrets returns an iterator of secrets matching the given options.
	// The iterator yields Secret structs containing minimal metadata.
	// Use filters like NamePrefix and FilterByTag to narrow results.
	ListSecrets(ctx context.Context, options ...ListOption) iter.Seq2[Secret, error]

	// ListSecretVersions returns an iterator of versions for a specific secret.
	// Not all backends support versioning; unsupported backends may return
	// only the current version or an empty iterator.
	ListSecretVersions(ctx context.Context, name string, options ...ListVersionOption) iter.Seq2[Version, error]

	// GetSecretVersion retrieves a specific version of a secret.
	// Returns ErrVersionNotFound if the version does not exist.
	// Returns ErrNotFound if the secret does not exist.
	GetSecretVersion(ctx context.Context, name string, version string) (Value, Info, error)

	// DestroySecretVersion permanently destroys a specific version of a secret.
	// This operation is irreversible.
	// Returns ErrVersionNotFound if the version does not exist.
	DestroySecretVersion(ctx context.Context, name string, version string) error
}

// Secret represents minimal metadata about a secret, suitable for listing
// operations. This type contains only the information typically available
// when enumerating secrets.
type Secret struct {
	Name      string            `json:"name"`
	CreatedAt time.Time         `json:"created-at,omitzero"`
	UpdatedAt time.Time         `json:"updated-at,omitzero"`
	Tags      map[string]string `json:"tags,omitempty"`
}

// Info represents detailed metadata about a secret.
// This type includes more information than Secret, and is returned by
// operations that work with a specific secret.
type Info struct {
	Name         string            `json:"name"`
	Version      string            `json:"version,omitempty"`
	CreatedAt    time.Time         `json:"created-at,omitzero"`
	UpdatedAt    time.Time         `json:"updated-at,omitzero"`
	ExpiresAt    time.Time         `json:"expires-at,omitzero"`
	Tags         map[string]string `json:"tags,omitempty"`
	Description  string            `json:"description,omitempty"`
	VersionCount int               `json:"version-count,omitzero"`
}

// Version represents metadata about a specific version of a secret.
type Version struct {
	ID        string       `json:"id"`
	CreatedAt time.Time    `json:"created-at,omitzero"`
	State     VersionState `json:"state"`
}

// VersionState represents the state of a secret version.
type VersionState string

const (
	// VersionStateEnabled indicates the version is active and can be accessed.
	VersionStateEnabled VersionState = "enabled"

	// VersionStateDisabled indicates the version is disabled and cannot be accessed.
	VersionStateDisabled VersionState = "disabled"

	// VersionStateDestroyed indicates the version has been permanently destroyed.
	VersionStateDestroyed VersionState = "destroyed"
)

// Standard errors returned by secret managers.
var (
	// ErrNotFound is returned when a secret does not exist.
	ErrNotFound = errors.New("secret not found")

	// ErrAlreadyExists is returned when attempting to create a secret that already exists.
	ErrAlreadyExists = errors.New("secret already exists")

	// ErrVersionNotFound is returned when a specific version does not exist.
	ErrVersionNotFound = errors.New("version not found")

	// ErrReadOnly is returned when attempting write operations on a read-only manager.
	ErrReadOnly = errors.New("read-only secret manager")

	// ErrInvalidName is returned when a secret name is invalid.
	ErrInvalidName = errors.New("invalid secret name")
)

// Adapter is an interface for adapting a Manager with additional functionality.
// Adapters can be used to add features like read-only enforcement, prefixing,
// logging, or instrumentation.
type Adapter interface {
	AdaptManager(Manager) Manager
}

// AdapterFunc is a function type that implements the Adapter interface.
type AdapterFunc func(Manager) Manager

// AdaptManager implements the Adapter interface for AdapterFunc.
func (a AdapterFunc) AdaptManager(m Manager) Manager { return a(m) }

// AdaptManager applies a sequence of adapters to a manager.
// Adapters are applied in order, so the first adapter wraps the original
// manager, the second adapter wraps the first, and so on.
func AdaptManager(manager Manager, adapters ...Adapter) Manager {
	for _, adapter := range adapters {
		manager = adapter.AdaptManager(manager)
	}
	return manager
}
