package secret

import "context"

// Store provides read-only access to secrets by name.
// Manager implements this interface.
type Store interface {
	// GetSecret retrieves a secret by name.
	// Use WithVersion to retrieve a specific version.
	// Returns ErrNotFound if the secret does not exist.
	// Returns ErrVersionNotFound if the version does not exist.
	GetSecret(ctx context.Context, name string, options ...GetOption) (Value, Info, error)
}

// StoreFunc is a function adapter for Store.
type StoreFunc func(ctx context.Context, name string, options ...GetOption) (Value, Info, error)

// GetSecret implements Store.
func (f StoreFunc) GetSecret(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
	return f(ctx, name, options...)
}
