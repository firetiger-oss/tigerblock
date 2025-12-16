package secret_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/firetiger-oss/storage/memory"
	"github.com/firetiger-oss/storage/secret"
)

func TestPrefix(t *testing.T) {
	ctx := t.Context()
	base := secret.NewManager(memory.NewBucket())
	prefixed := secret.Prefix(base, "prod/")

	t.Run("Create adds prefix", func(t *testing.T) {
		info, err := prefixed.CreateSecret(ctx, "db-password", secret.Value("secret123"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info.Name != "db-password" {
			t.Errorf("expected name 'db-password', got %q", info.Name)
		}
		// Verify the underlying manager has the prefixed name
		_, _, err = base.GetSecret(ctx, "prod/db-password")
		if err != nil {
			t.Error("expected prefixed secret in base manager")
		}
	})

	t.Run("Get uses prefix", func(t *testing.T) {
		value, info, err := prefixed.GetSecret(ctx, "db-password")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(value) != "secret123" {
			t.Errorf("expected value 'secret123', got %q", value)
		}
		if info.Name != "db-password" {
			t.Errorf("expected name 'db-password', got %q", info.Name)
		}
	})

	t.Run("Get returns ErrNotFound for missing secret", func(t *testing.T) {
		_, _, err := prefixed.GetSecret(ctx, "nonexistent")
		if !errors.Is(err, secret.ErrNotFound) {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("Update uses prefix", func(t *testing.T) {
		info, err := prefixed.UpdateSecret(ctx, "db-password", secret.Value("newsecret"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info.Name != "db-password" {
			t.Errorf("expected name 'db-password', got %q", info.Name)
		}
		// Verify the value was updated
		value, _, _ := prefixed.GetSecret(ctx, "db-password")
		if string(value) != "newsecret" {
			t.Errorf("expected updated value 'newsecret', got %q", value)
		}
	})

	t.Run("List strips prefix from results", func(t *testing.T) {
		// Add another secret
		prefixed.CreateSecret(ctx, "api-key", secret.Value("key123"))

		var names []string
		for s, err := range prefixed.ListSecrets(ctx) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			names = append(names, s.Name)
		}

		if len(names) != 2 {
			t.Fatalf("expected 2 secrets, got %d", len(names))
		}
		if !slices.Contains(names, "db-password") {
			t.Error("expected 'db-password' in results")
		}
		if !slices.Contains(names, "api-key") {
			t.Error("expected 'api-key' in results")
		}
	})

	t.Run("List with NamePrefix combines prefixes", func(t *testing.T) {
		// Add a secret with different prefix pattern
		prefixed.CreateSecret(ctx, "other-secret", secret.Value("other"))

		var names []string
		for s, err := range prefixed.ListSecrets(ctx, secret.NamePrefix("db-")) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			names = append(names, s.Name)
		}

		if len(names) != 1 {
			t.Fatalf("expected 1 secret, got %d: %v", len(names), names)
		}
		if names[0] != "db-password" {
			t.Errorf("expected 'db-password', got %q", names[0])
		}
	})

	t.Run("Delete uses prefix", func(t *testing.T) {
		err := prefixed.DeleteSecret(ctx, "api-key")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Verify it's gone from base
		_, _, err = base.GetSecret(ctx, "prod/api-key")
		if !errors.Is(err, secret.ErrNotFound) {
			t.Error("expected secret to be deleted from base manager")
		}
	})

	t.Run("DestroySecretVersion uses prefix", func(t *testing.T) {
		// Destroy version 1 and verify it's gone
		err := prefixed.DestroySecretVersion(ctx, "db-password", "1")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// Verify we can't get version 1 anymore
		_, _, err = prefixed.GetSecret(ctx, "db-password", secret.WithVersion("1"))
		if !errors.Is(err, secret.ErrVersionNotFound) {
			t.Errorf("expected ErrVersionNotFound, got %v", err)
		}
	})
}

func TestWithPrefix(t *testing.T) {
	ctx := t.Context()
	base := secret.NewManager(memory.NewBucket())
	adapter := secret.WithPrefix("test/")

	prefixed := adapter.AdaptManager(base)

	if prefixed == base {
		t.Error("expected adapter to return a different manager")
	}

	_, err := prefixed.CreateSecret(ctx, "secret", secret.Value("value"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify prefix was applied
	_, _, err = base.GetSecret(ctx, "test/secret")
	if err != nil {
		t.Error("expected prefixed secret in base manager")
	}
}

func TestPrefixIsolation(t *testing.T) {
	ctx := t.Context()
	base := secret.NewManager(memory.NewBucket())

	prod := secret.Prefix(base, "prod/")
	staging := secret.Prefix(base, "staging/")

	// Create secrets in different namespaces
	prod.CreateSecret(ctx, "db-password", secret.Value("prod-secret"))
	staging.CreateSecret(ctx, "db-password", secret.Value("staging-secret"))

	// Verify isolation
	prodValue, _, _ := prod.GetSecret(ctx, "db-password")
	stagingValue, _, _ := staging.GetSecret(ctx, "db-password")

	if string(prodValue) != "prod-secret" {
		t.Errorf("expected 'prod-secret', got %q", prodValue)
	}
	if string(stagingValue) != "staging-secret" {
		t.Errorf("expected 'staging-secret', got %q", stagingValue)
	}

	// Verify base has both with full prefixed names
	_, _, err := base.GetSecret(ctx, "prod/db-password")
	if err != nil {
		t.Error("expected 'prod/db-password' in base")
	}
	_, _, err = base.GetSecret(ctx, "staging/db-password")
	if err != nil {
		t.Error("expected 'staging/db-password' in base")
	}
}
