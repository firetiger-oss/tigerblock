package secret_test

import (
	"errors"
	"testing"

	"github.com/firetiger-oss/tigerblock/storage/memory"
	"github.com/firetiger-oss/tigerblock/secret"
)

func TestReadOnly(t *testing.T) {
	ctx := t.Context()
	base := secret.NewManager(memory.NewBucket())
	base.CreateSecret(ctx, "existing", secret.Value("value"))

	ro := secret.ReadOnly(base)

	t.Run("GetValue allows read", func(t *testing.T) {
		value, _, err := ro.GetSecretValue(ctx, "existing")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if string(value) != "value" {
			t.Errorf("expected value 'value', got %q", value)
		}
	})

	t.Run("Create returns ErrReadOnly", func(t *testing.T) {
		_, err := ro.CreateSecret(ctx, "new", secret.Value("value"))
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, secret.ErrReadOnly) {
			t.Errorf("expected ErrReadOnly, got %v", err)
		}
	})

	t.Run("Update returns ErrReadOnly", func(t *testing.T) {
		_, err := ro.UpdateSecret(ctx, "existing", secret.Value("new-value"))
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, secret.ErrReadOnly) {
			t.Errorf("expected ErrReadOnly, got %v", err)
		}
	})

	t.Run("Delete returns ErrReadOnly", func(t *testing.T) {
		err := ro.DeleteSecret(ctx, "existing")
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, secret.ErrReadOnly) {
			t.Errorf("expected ErrReadOnly, got %v", err)
		}
	})

	t.Run("DestroyVersion returns ErrReadOnly", func(t *testing.T) {
		err := ro.DestroySecretVersion(ctx, "existing", "v1")
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, secret.ErrReadOnly) {
			t.Errorf("expected ErrReadOnly, got %v", err)
		}
	})

	t.Run("List allows read", func(t *testing.T) {
		count := 0
		for _, err := range ro.ListSecrets(ctx) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
		}
		if count == 0 {
			t.Error("expected at least one secret")
		}
	})
}

func TestWithReadOnly(t *testing.T) {
	ctx := t.Context()
	base := secret.NewManager(memory.NewBucket())
	adapter := secret.WithReadOnly()

	ro := adapter.AdaptManager(base)

	if ro == base {
		t.Error("expected adapter to return a different manager")
	}

	_, err := ro.CreateSecret(ctx, "test", secret.Value("value"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, secret.ErrReadOnly) {
		t.Errorf("expected ErrReadOnly, got %v", err)
	}
}
