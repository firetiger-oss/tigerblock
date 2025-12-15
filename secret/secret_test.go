package secret

import (
	"context"
	"iter"
	"strings"
)

// mockManager is a simple in-memory manager for testing
type mockManager struct {
	secrets map[string]Value
}

func (m *mockManager) CreateSecret(ctx context.Context, name string, value Value, options ...CreateOption) (Info, error) {
	if _, exists := m.secrets[name]; exists {
		return Info{}, ErrAlreadyExists
	}
	m.secrets[name] = value
	return Info{Name: name}, nil
}

func (m *mockManager) GetSecret(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
	value, exists := m.secrets[name]
	if !exists {
		return nil, Info{}, ErrNotFound
	}
	return value, Info{Name: name}, nil
}

func (m *mockManager) UpdateSecret(ctx context.Context, name string, value Value, options ...UpdateOption) (Info, error) {
	m.secrets[name] = value
	return Info{Name: name}, nil
}

func (m *mockManager) DeleteSecret(ctx context.Context, name string) error {
	delete(m.secrets, name)
	return nil
}

func (m *mockManager) ListSecrets(ctx context.Context, options ...ListOption) iter.Seq2[Secret, error] {
	return func(yield func(Secret, error) bool) {}
}

func (m *mockManager) ListSecretVersions(ctx context.Context, name string, options ...ListVersionOption) iter.Seq2[Version, error] {
	return func(yield func(Version, error) bool) {}
}

func (m *mockManager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	return ErrVersionNotFound
}

// mockManagerWithList extends mockManager to support List
type mockManagerWithList struct {
	secrets map[string]Value
}

func (m *mockManagerWithList) CreateSecret(ctx context.Context, name string, value Value, options ...CreateOption) (Info, error) {
	m.secrets[name] = value
	return Info{Name: name}, nil
}

func (m *mockManagerWithList) GetSecret(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
	value, exists := m.secrets[name]
	if !exists {
		return nil, Info{}, ErrNotFound
	}
	return value, Info{Name: name}, nil
}

func (m *mockManagerWithList) UpdateSecret(ctx context.Context, name string, value Value, options ...UpdateOption) (Info, error) {
	m.secrets[name] = value
	return Info{Name: name}, nil
}

func (m *mockManagerWithList) DeleteSecret(ctx context.Context, name string) error {
	delete(m.secrets, name)
	return nil
}

func (m *mockManagerWithList) ListSecrets(ctx context.Context, options ...ListOption) iter.Seq2[Secret, error] {
	return func(yield func(Secret, error) bool) {
		opts := NewListOptions(options...)
		prefix := opts.NamePrefix()

		for name := range m.secrets {
			if prefix != "" && !strings.HasPrefix(name, prefix) {
				continue
			}
			if !yield(Secret{Name: name}, nil) {
				return
			}
		}
	}
}

func (m *mockManagerWithList) ListSecretVersions(ctx context.Context, name string, options ...ListVersionOption) iter.Seq2[Version, error] {
	return func(yield func(Version, error) bool) {}
}

func (m *mockManagerWithList) DestroySecretVersion(ctx context.Context, name string, version string) error {
	return ErrVersionNotFound
}
