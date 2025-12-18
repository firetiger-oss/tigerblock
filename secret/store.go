package secret

import "context"

// Provider provides read-only access to secret values by name.
// Manager implements this interface.
type Provider interface {
	// GetSecretValue retrieves a secret value by name.
	// Returns the value and version ID.
	// Use WithVersion to retrieve a specific version.
	// Returns ErrNotFound if the secret or version does not exist.
	GetSecretValue(ctx context.Context, name string, options ...GetOption) (Value, string, error)
}

// ProviderFunc is a function adapter for Provider.
type ProviderFunc func(ctx context.Context, name string, options ...GetOption) (Value, string, error)

// GetSecretValue implements Provider.
func (f ProviderFunc) GetSecretValue(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
	return f(ctx, name, options...)
}
