package secret

import (
	"context"
	"iter"
	"strings"
)

// Prefix wraps a Manager to add a prefix to all secret names.
// The prefix is prepended to secret names on write operations and
// stripped from secret names on read operations.
func Prefix(manager Manager, prefix string) Manager {
	return &prefixManager{Manager: manager, prefix: prefix}
}

// WithPrefix returns an Adapter that adds a prefix to all secret names.
func WithPrefix(prefix string) Adapter {
	return AdapterFunc(func(m Manager) Manager {
		return Prefix(m, prefix)
	})
}

type prefixManager struct {
	Manager
	prefix string
}

func (m *prefixManager) CreateSecret(ctx context.Context, name string, value Value, options ...CreateOption) (Info, error) {
	info, err := m.Manager.CreateSecret(ctx, m.prefix+name, value, options...)
	if err != nil {
		return info, err
	}
	info.Name = strings.TrimPrefix(info.Name, m.prefix)
	return info, nil
}

func (m *prefixManager) GetSecretValue(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
	return m.Manager.GetSecretValue(ctx, m.prefix+name, options...)
}

func (m *prefixManager) GetSecretInfo(ctx context.Context, name string) (Info, error) {
	info, err := m.Manager.GetSecretInfo(ctx, m.prefix+name)
	if err != nil {
		return info, err
	}
	info.Name = strings.TrimPrefix(info.Name, m.prefix)
	return info, nil
}

func (m *prefixManager) UpdateSecret(ctx context.Context, name string, value Value, options ...UpdateOption) (Info, error) {
	info, err := m.Manager.UpdateSecret(ctx, m.prefix+name, value, options...)
	if err != nil {
		return info, err
	}
	info.Name = strings.TrimPrefix(info.Name, m.prefix)
	return info, nil
}

func (m *prefixManager) DeleteSecret(ctx context.Context, name string) error {
	return m.Manager.DeleteSecret(ctx, m.prefix+name)
}

func (m *prefixManager) ListSecrets(ctx context.Context, options ...ListOption) iter.Seq2[Secret, error] {
	// Prepend the prefix to any NamePrefix filter
	opts := NewListOptions(options...)
	newOptions := []ListOption{
		NamePrefix(m.prefix + opts.NamePrefix()),
	}
	if opts.Tags() != nil {
		newOptions = append(newOptions, FilterByTags(opts.Tags()))
	}
	if opts.MaxResults() > 0 {
		newOptions = append(newOptions, MaxResults(opts.MaxResults()))
	}

	return func(yield func(Secret, error) bool) {
		for s, err := range m.Manager.ListSecrets(ctx, newOptions...) {
			if err != nil {
				yield(s, err)
				return
			}
			s.Name = strings.TrimPrefix(s.Name, m.prefix)
			if !yield(s, nil) {
				return
			}
		}
	}
}

func (m *prefixManager) ListSecretVersions(ctx context.Context, name string, options ...ListVersionOption) iter.Seq2[Version, error] {
	return m.Manager.ListSecretVersions(ctx, m.prefix+name, options...)
}

func (m *prefixManager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	return m.Manager.DestroySecretVersion(ctx, m.prefix+name, version)
}
