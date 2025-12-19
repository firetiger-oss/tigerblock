package env

import (
	"os"
	"testing"

	"github.com/firetiger-oss/storage/secret"
)

func TestRegistry(t *testing.T) {
	if _, _, err := secret.Get(t.Context(), "env:FOO"); err == nil {
		t.Fatal("expected an error for non-existent env var")
	}
	os.Setenv("BAR", "my-value")
	if v, _, err := secret.Get(t.Context(), "env:BAR"); err != nil {
		t.Fatal("unexpected error", err)
	} else if string(v) != "my-value" {
		t.Fatal("unexpected value", string(v))
	}
}
