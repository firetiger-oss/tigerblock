package secret

import (
	"context"
	"slices"
	"testing"
)

func TestPrefix(t *testing.T) {
	ctx := context.Background()
	base := &mockManagerWithList{secrets: make(map[string]Value)}
	prefixed := Prefix(base, "prod/")

	t.Run("Create adds prefix", func(t *testing.T) {
		info, err := prefixed.CreateSecret(ctx, "db-password", Value("secret123"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if info.Name != "db-password" {
			t.Errorf("expected name 'db-password', got %q", info.Name)
		}
		// Verify the underlying manager has the prefixed name
		if _, exists := base.secrets["prod/db-password"]; !exists {
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
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("Update uses prefix", func(t *testing.T) {
		info, err := prefixed.UpdateSecret(ctx, "db-password", Value("newsecret"))
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
		prefixed.CreateSecret(ctx, "api-key", Value("key123"))

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
		prefixed.CreateSecret(ctx, "other-secret", Value("other"))

		var names []string
		for s, err := range prefixed.ListSecrets(ctx, NamePrefix("db-")) {
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
		if _, exists := base.secrets["prod/api-key"]; exists {
			t.Error("expected secret to be deleted from base manager")
		}
	})

	t.Run("DestroySecretVersion uses prefix", func(t *testing.T) {
		// The mock returns ErrVersionNotFound, but we verify prefix is applied
		err := prefixed.DestroySecretVersion(ctx, "db-password", "v1")
		if err != ErrVersionNotFound {
			t.Errorf("expected ErrVersionNotFound, got %v", err)
		}
	})
}

func TestWithPrefix(t *testing.T) {
	base := &mockManager{secrets: make(map[string]Value)}
	adapter := WithPrefix("test/")

	prefixed := adapter.AdaptManager(base)

	if prefixed == base {
		t.Error("expected adapter to return a different manager")
	}

	ctx := context.Background()
	_, err := prefixed.CreateSecret(ctx, "secret", Value("value"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify prefix was applied
	if _, exists := base.secrets["test/secret"]; !exists {
		t.Error("expected prefixed secret in base manager")
	}
}

func TestPrefixIsolation(t *testing.T) {
	ctx := context.Background()
	base := &mockManagerWithList{secrets: make(map[string]Value)}

	prod := Prefix(base, "prod/")
	staging := Prefix(base, "staging/")

	// Create secrets in different namespaces
	prod.CreateSecret(ctx, "db-password", Value("prod-secret"))
	staging.CreateSecret(ctx, "db-password", Value("staging-secret"))

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
	if _, exists := base.secrets["prod/db-password"]; !exists {
		t.Error("expected 'prod/db-password' in base")
	}
	if _, exists := base.secrets["staging/db-password"]; !exists {
		t.Error("expected 'staging/db-password' in base")
	}
}
