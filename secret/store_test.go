package secret

import (
	"context"
	"testing"
)

func TestProviderFunc(t *testing.T) {
	ctx := t.Context()

	t.Run("delegates to function", func(t *testing.T) {
		called := false
		expectedValue := Value("secret-value")
		expectedVersion := "v1"

		provider := ProviderFunc(func(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
			called = true
			if name != "test-secret" {
				t.Errorf("expected name 'test-secret', got %q", name)
			}
			return expectedValue, expectedVersion, nil
		})

		value, version, err := provider.GetSecretValue(ctx, "test-secret")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Error("expected function to be called")
		}
		if string(value) != string(expectedValue) {
			t.Errorf("expected value %q, got %q", expectedValue, value)
		}
		if version != expectedVersion {
			t.Errorf("expected version %q, got %q", expectedVersion, version)
		}
	})

	t.Run("passes options to function", func(t *testing.T) {
		var receivedOpts *GetOptions

		provider := ProviderFunc(func(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
			receivedOpts = NewGetOptions(options...)
			return nil, "", nil
		})

		provider.GetSecretValue(ctx, "test", WithVersion("v2"))

		if receivedOpts == nil {
			t.Fatal("expected options to be passed")
		}
		if receivedOpts.Version() != "v2" {
			t.Errorf("expected version 'v2', got %q", receivedOpts.Version())
		}
	})

	t.Run("returns error from function", func(t *testing.T) {
		provider := ProviderFunc(func(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
			return nil, "", ErrNotFound
		})

		_, _, err := provider.GetSecretValue(ctx, "missing")
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestManagerImplementsProvider(t *testing.T) {
	// Compile-time check that Manager implements Provider
	var _ Provider = (Manager)(nil)
}
