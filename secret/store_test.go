package secret

import (
	"context"
	"testing"
)

func TestStoreFunc(t *testing.T) {
	ctx := t.Context()

	t.Run("delegates to function", func(t *testing.T) {
		called := false
		expectedValue := Value("secret-value")
		expectedInfo := Info{Name: "test-secret"}

		store := StoreFunc(func(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
			called = true
			if name != "test-secret" {
				t.Errorf("expected name 'test-secret', got %q", name)
			}
			return expectedValue, expectedInfo, nil
		})

		value, info, err := store.GetSecret(ctx, "test-secret")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Error("expected function to be called")
		}
		if string(value) != string(expectedValue) {
			t.Errorf("expected value %q, got %q", expectedValue, value)
		}
		if info.Name != expectedInfo.Name {
			t.Errorf("expected info.Name %q, got %q", expectedInfo.Name, info.Name)
		}
	})

	t.Run("passes options to function", func(t *testing.T) {
		var receivedOpts *GetOptions

		store := StoreFunc(func(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
			receivedOpts = NewGetOptions(options...)
			return nil, Info{}, nil
		})

		store.GetSecret(ctx, "test", WithVersion("v2"))

		if receivedOpts == nil {
			t.Fatal("expected options to be passed")
		}
		if receivedOpts.Version() != "v2" {
			t.Errorf("expected version 'v2', got %q", receivedOpts.Version())
		}
	})

	t.Run("returns error from function", func(t *testing.T) {
		store := StoreFunc(func(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
			return nil, Info{}, ErrNotFound
		})

		_, _, err := store.GetSecret(ctx, "missing")
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})
}

func TestManagerImplementsStore(t *testing.T) {
	// Compile-time check that Manager implements Store
	var _ Store = (Manager)(nil)
}
