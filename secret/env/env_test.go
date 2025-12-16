package env

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/firetiger-oss/storage/secret"
	"github.com/firetiger-oss/storage/test"
)

// TestManager runs the comprehensive test suite against the env backend.
// Since env is read-only, most write tests will be skipped.
func TestManager(t *testing.T) {
	// Set up some test environment variables
	os.Setenv("TEST_SECRET_1", "value1")
	os.Setenv("TEST_SECRET_2", "value2")
	os.Setenv("TEST_OTHER", "value3")
	defer func() {
		os.Unsetenv("TEST_SECRET_1")
		os.Unsetenv("TEST_SECRET_2")
		os.Unsetenv("TEST_OTHER")
	}()

	test.TestManager(t, func(t *testing.T) (secret.Manager, error) {
		return NewManager(), nil
	})
}

func TestManagerGet(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()

	// Set a test environment variable
	os.Setenv("TEST_SECRET", "test-value")
	defer os.Unsetenv("TEST_SECRET")

	value, info, err := manager.GetSecret(ctx, "TEST_SECRET")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(value) != "test-value" {
		t.Errorf("expected value 'test-value', got %q", value)
	}

	if info.Name != "TEST_SECRET" {
		t.Errorf("expected name 'TEST_SECRET', got %q", info.Name)
	}
}

func TestManagerGetNotFound(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()

	_, _, err := manager.GetSecret(ctx, "NONEXISTENT_SECRET")
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, secret.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestManagerList(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()

	// Set some test environment variables
	os.Setenv("TEST_SECRET_1", "value1")
	os.Setenv("TEST_SECRET_2", "value2")
	os.Setenv("OTHER_VAR", "value3")
	defer func() {
		os.Unsetenv("TEST_SECRET_1")
		os.Unsetenv("TEST_SECRET_2")
		os.Unsetenv("OTHER_VAR")
	}()

	var secrets []secret.Secret
	for s, err := range manager.ListSecrets(ctx, secret.NamePrefix("TEST_SECRET_")) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		secrets = append(secrets, s)
	}

	if len(secrets) != 2 {
		t.Errorf("expected 2 secrets, got %d", len(secrets))
	}

	// Check that we got the right secrets
	names := make(map[string]bool)
	for _, s := range secrets {
		names[s.Name] = true
	}

	if !names["TEST_SECRET_1"] || !names["TEST_SECRET_2"] {
		t.Errorf("unexpected secrets: %v", secrets)
	}
}

func TestManagerListMaxResults(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()

	// Set some test environment variables
	os.Setenv("TEST_A", "value1")
	os.Setenv("TEST_B", "value2")
	os.Setenv("TEST_C", "value3")
	defer func() {
		os.Unsetenv("TEST_A")
		os.Unsetenv("TEST_B")
		os.Unsetenv("TEST_C")
	}()

	var secrets []secret.Secret
	for s, err := range manager.ListSecrets(ctx, secret.NamePrefix("TEST_"), secret.MaxResults(2)) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		secrets = append(secrets, s)
	}

	if len(secrets) != 2 {
		t.Errorf("expected 2 secrets (max results), got %d", len(secrets))
	}
}

func TestManagerWriteOperationsReturnReadOnly(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()

	tests := []struct {
		name string
		fn   func() error
	}{
		{
			name: "Create",
			fn: func() error {
				_, err := manager.CreateSecret(ctx, "test", []byte("value"))
				return err
			},
		},
		{
			name: "Update",
			fn: func() error {
				_, err := manager.UpdateSecret(ctx, "test", []byte("value"))
				return err
			},
		},
		{
			name: "Delete",
			fn: func() error {
				return manager.DeleteSecret(ctx, "test")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.fn()
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, secret.ErrReadOnly) {
				t.Errorf("expected ErrReadOnly, got %v", err)
			}
		})
	}
}

func TestManagerVersionOperationsNotSupported(t *testing.T) {
	ctx := context.Background()
	manager := NewManager()

	t.Run("GetVersion", func(t *testing.T) {
		_, _, err := manager.GetSecret(ctx, "test", secret.WithVersion("v1"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, secret.ErrVersionNotFound) {
			t.Errorf("expected ErrVersionNotFound, got %v", err)
		}
	})

	t.Run("DestroyVersion", func(t *testing.T) {
		err := manager.DestroySecretVersion(ctx, "test", "v1")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, secret.ErrReadOnly) {
			t.Errorf("expected ErrReadOnly, got %v", err)
		}
	})

	t.Run("ListVersions", func(t *testing.T) {
		var versions []secret.Version
		for v, err := range manager.ListSecretVersions(ctx, "test") {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			versions = append(versions, v)
		}
		if len(versions) != 0 {
			t.Errorf("expected empty list, got %d versions", len(versions))
		}
	})
}
