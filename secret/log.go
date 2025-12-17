package secret

import (
	"context"
	"iter"
	"log/slog"
)

// WithLogger returns an Adapter that adds safe logging to all operations.
// Secret values are NEVER logged - only metadata like names, versions, and sizes.
func WithLogger(logger *slog.Logger) Adapter {
	return AdapterFunc(func(m Manager) Manager {
		return &loggedManager{
			Manager: m,
			logger:  logger,
		}
	})
}

type loggedManager struct {
	Manager
	logger *slog.Logger
}

func (m *loggedManager) CreateSecret(ctx context.Context, name string, value Value, options ...CreateOption) (Info, error) {
	m.logger.InfoContext(ctx, "creating secret",
		slog.String("operation", "Create"),
		slog.String("name", name),
		slog.Int("value_size", len(value)),
	)

	info, err := m.Manager.CreateSecret(ctx, name, value, options...)
	if err != nil {
		m.logger.ErrorContext(ctx, "failed to create secret",
			slog.String("operation", "Create"),
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
		return info, err
	}

	m.logger.InfoContext(ctx, "created secret",
		slog.String("operation", "Create"),
		slog.String("name", name),
		slog.String("version", info.Version),
	)
	return info, nil
}

func (m *loggedManager) GetSecretValue(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
	m.logger.InfoContext(ctx, "getting secret value",
		slog.String("operation", "GetValue"),
		slog.String("name", name),
	)

	value, version, err := m.Manager.GetSecretValue(ctx, name, options...)
	if err != nil {
		m.logger.ErrorContext(ctx, "failed to get secret value",
			slog.String("operation", "GetValue"),
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
		return value, version, err
	}

	// NEVER log the actual value - only metadata
	m.logger.InfoContext(ctx, "got secret value",
		slog.String("operation", "GetValue"),
		slog.String("name", name),
		slog.String("version", version),
		slog.Int("value_size", len(value)),
	)
	return value, version, nil
}

func (m *loggedManager) GetSecretInfo(ctx context.Context, name string) (Info, error) {
	m.logger.InfoContext(ctx, "getting secret info",
		slog.String("operation", "GetInfo"),
		slog.String("name", name),
	)

	info, err := m.Manager.GetSecretInfo(ctx, name)
	if err != nil {
		m.logger.ErrorContext(ctx, "failed to get secret info",
			slog.String("operation", "GetInfo"),
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
		return info, err
	}

	m.logger.InfoContext(ctx, "got secret info",
		slog.String("operation", "GetInfo"),
		slog.String("name", name),
		slog.String("version", info.Version),
	)
	return info, nil
}

func (m *loggedManager) UpdateSecret(ctx context.Context, name string, value Value, options ...UpdateOption) (Info, error) {
	m.logger.InfoContext(ctx, "updating secret",
		slog.String("operation", "Update"),
		slog.String("name", name),
		slog.Int("value_size", len(value)),
	)

	info, err := m.Manager.UpdateSecret(ctx, name, value, options...)
	if err != nil {
		m.logger.ErrorContext(ctx, "failed to update secret",
			slog.String("operation", "Update"),
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
		return info, err
	}

	m.logger.InfoContext(ctx, "updated secret",
		slog.String("operation", "Update"),
		slog.String("name", name),
		slog.String("version", info.Version),
	)
	return info, nil
}

func (m *loggedManager) DeleteSecret(ctx context.Context, name string) error {
	m.logger.InfoContext(ctx, "deleting secret",
		slog.String("operation", "Delete"),
		slog.String("name", name),
	)

	err := m.Manager.DeleteSecret(ctx, name)
	if err != nil {
		m.logger.ErrorContext(ctx, "failed to delete secret",
			slog.String("operation", "Delete"),
			slog.String("name", name),
			slog.String("error", err.Error()),
		)
		return err
	}

	m.logger.InfoContext(ctx, "deleted secret",
		slog.String("operation", "Delete"),
		slog.String("name", name),
	)
	return nil
}

func (m *loggedManager) ListSecrets(ctx context.Context, options ...ListOption) iter.Seq2[Secret, error] {
	m.logger.InfoContext(ctx, "listing secrets",
		slog.String("operation", "List"),
	)

	count := 0
	return func(yield func(Secret, error) bool) {
		for secret, err := range m.Manager.ListSecrets(ctx, options...) {
			if err != nil {
				m.logger.ErrorContext(ctx, "error listing secrets",
					slog.String("operation", "List"),
					slog.Int("count", count),
					slog.String("error", err.Error()),
				)
				yield(secret, err)
				return
			}

			count++
			if !yield(secret, err) {
				break
			}
		}

		m.logger.InfoContext(ctx, "listed secrets",
			slog.String("operation", "List"),
			slog.Int("count", count),
		)
	}
}

func (m *loggedManager) ListSecretVersions(ctx context.Context, name string, options ...ListVersionOption) iter.Seq2[Version, error] {
	m.logger.InfoContext(ctx, "listing secret versions",
		slog.String("operation", "ListVersions"),
		slog.String("name", name),
	)

	count := 0
	return func(yield func(Version, error) bool) {
		for version, err := range m.Manager.ListSecretVersions(ctx, name, options...) {
			if err != nil {
				m.logger.ErrorContext(ctx, "error listing versions",
					slog.String("operation", "ListVersions"),
					slog.String("name", name),
					slog.Int("count", count),
					slog.String("error", err.Error()),
				)
				yield(version, err)
				return
			}

			count++
			if !yield(version, err) {
				break
			}
		}

		m.logger.InfoContext(ctx, "listed secret versions",
			slog.String("operation", "ListVersions"),
			slog.String("name", name),
			slog.Int("count", count),
		)
	}
}

func (m *loggedManager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	m.logger.InfoContext(ctx, "destroying secret version",
		slog.String("operation", "DestroyVersion"),
		slog.String("name", name),
		slog.String("version", version),
	)

	err := m.Manager.DestroySecretVersion(ctx, name, version)
	if err != nil {
		m.logger.ErrorContext(ctx, "failed to destroy secret version",
			slog.String("operation", "DestroyVersion"),
			slog.String("name", name),
			slog.String("version", version),
			slog.String("error", err.Error()),
		)
		return err
	}

	m.logger.InfoContext(ctx, "destroyed secret version",
		slog.String("operation", "DestroyVersion"),
		slog.String("name", name),
		slog.String("version", version),
	)
	return nil
}
