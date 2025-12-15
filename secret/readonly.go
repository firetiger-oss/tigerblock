package secret

import (
	"context"
	"iter"
)

// ReadOnly wraps a Manager to make it read-only.
// All write operations (Create, Update, Delete, Rotate) return ErrReadOnly.
func ReadOnly(manager Manager) Manager {
	return &readonlyManager{Manager: manager}
}

// WithReadOnly returns an Adapter that makes managers read-only.
func WithReadOnly() Adapter {
	return AdapterFunc(func(m Manager) Manager {
		return ReadOnly(m)
	})
}

type readonlyManager struct {
	Manager
}

func (m *readonlyManager) CreateSecret(ctx context.Context, name string, value Value, options ...CreateOption) (Info, error) {
	return Info{}, ErrReadOnly
}

func (m *readonlyManager) UpdateSecret(ctx context.Context, name string, value Value, options ...UpdateOption) (Info, error) {
	return Info{}, ErrReadOnly
}

func (m *readonlyManager) DeleteSecret(ctx context.Context, name string) error {
	return ErrReadOnly
}

func (m *readonlyManager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	return ErrReadOnly
}

// Read operations are passed through to the underlying manager
func (m *readonlyManager) GetSecret(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
	return m.Manager.GetSecret(ctx, name, options...)
}

func (m *readonlyManager) ListSecrets(ctx context.Context, options ...ListOption) iter.Seq2[Secret, error] {
	return m.Manager.ListSecrets(ctx, options...)
}

func (m *readonlyManager) ListSecretVersions(ctx context.Context, name string, options ...ListVersionOption) iter.Seq2[Version, error] {
	return m.Manager.ListSecretVersions(ctx, name, options...)
}
