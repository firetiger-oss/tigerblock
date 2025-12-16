package secret

import (
	"context"
	"iter"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/firetiger-oss/storage/secret"

// Instrument wraps a Manager with OpenTelemetry instrumentation.
// All operations create spans with relevant attributes.
func Instrument(manager Manager) Manager {
	return &instrumentedManager{
		Manager: manager,
		tracer:  otel.Tracer(instrumentationName),
	}
}

// WithInstrumentation returns an Adapter that adds OpenTelemetry instrumentation.
func WithInstrumentation() Adapter {
	return AdapterFunc(func(m Manager) Manager {
		return Instrument(m)
	})
}

type instrumentedManager struct {
	Manager
	tracer trace.Tracer
}

func (m *instrumentedManager) CreateSecret(ctx context.Context, name string, value Value, options ...CreateOption) (Info, error) {
	ctx, span := m.tracer.Start(ctx, "secret.Create",
		trace.WithAttributes(
			attribute.String("secret.name", name),
			attribute.Int("secret.value_size", len(value)),
		),
	)
	defer span.End()

	info, err := m.Manager.CreateSecret(ctx, name, value, options...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return info, err
	}

	span.SetAttributes(attribute.String("secret.version", info.Version))
	return info, nil
}

func (m *instrumentedManager) GetSecret(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
	ctx, span := m.tracer.Start(ctx, "secret.Get",
		trace.WithAttributes(
			attribute.String("secret.name", name),
		),
	)
	defer span.End()

	value, info, err := m.Manager.GetSecret(ctx, name, options...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return value, info, err
	}

	span.SetAttributes(
		attribute.String("secret.version", info.Version),
		attribute.Int("secret.value_size", len(value)),
	)
	return value, info, nil
}

func (m *instrumentedManager) UpdateSecret(ctx context.Context, name string, value Value, options ...UpdateOption) (Info, error) {
	ctx, span := m.tracer.Start(ctx, "secret.Update",
		trace.WithAttributes(
			attribute.String("secret.name", name),
			attribute.Int("secret.value_size", len(value)),
		),
	)
	defer span.End()

	info, err := m.Manager.UpdateSecret(ctx, name, value, options...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return info, err
	}

	span.SetAttributes(attribute.String("secret.version", info.Version))
	return info, nil
}

func (m *instrumentedManager) DeleteSecret(ctx context.Context, name string) error {
	ctx, span := m.tracer.Start(ctx, "secret.Delete",
		trace.WithAttributes(
			attribute.String("secret.name", name),
		),
	)
	defer span.End()

	err := m.Manager.DeleteSecret(ctx, name)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}

func (m *instrumentedManager) ListSecrets(ctx context.Context, options ...ListOption) iter.Seq2[Secret, error] {
	ctx, span := m.tracer.Start(ctx, "secret.List",
		trace.WithAttributes(),
	)
	defer span.End()

	count := 0
	return func(yield func(Secret, error) bool) {
		for secret, err := range m.Manager.ListSecrets(ctx, options...) {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				yield(secret, err)
				return
			}

			count++
			if !yield(secret, err) {
				break
			}
		}
		span.SetAttributes(attribute.Int("secret.list_count", count))
	}
}

func (m *instrumentedManager) ListSecretVersions(ctx context.Context, name string, options ...ListVersionOption) iter.Seq2[Version, error] {
	ctx, span := m.tracer.Start(ctx, "secret.ListVersions",
		trace.WithAttributes(
			attribute.String("secret.name", name),
		),
	)
	defer span.End()

	count := 0
	return func(yield func(Version, error) bool) {
		for version, err := range m.Manager.ListSecretVersions(ctx, name, options...) {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				yield(version, err)
				return
			}

			count++
			if !yield(version, err) {
				break
			}
		}
		span.SetAttributes(attribute.Int("secret.version_count", count))
	}
}

func (m *instrumentedManager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	ctx, span := m.tracer.Start(ctx, "secret.DestroyVersion",
		trace.WithAttributes(
			attribute.String("secret.name", name),
			attribute.String("secret.version", version),
		),
	)
	defer span.End()

	err := m.Manager.DestroySecretVersion(ctx, name, version)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	return nil
}
