package secret_test

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/storage/memory"
)

func TestWithLogger(t *testing.T) {
	// Setup a buffer to capture logs
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	ctx := t.Context()
	base := secret.NewManager(memory.NewBucket())
	base.CreateSecret(ctx, "existing", secret.Value("secret-value"))

	logged := secret.WithLogger(logger).AdaptManager(base)

	t.Run("Create logs metadata, not value", func(t *testing.T) {
		buf.Reset()

		_, err := logged.CreateSecret(ctx, "test-secret", secret.Value("super-secret-value"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		logOutput := buf.String()

		// Should contain operation and name
		if !strings.Contains(logOutput, "Create") {
			t.Error("expected log to contain 'Create'")
		}
		if !strings.Contains(logOutput, "test-secret") {
			t.Error("expected log to contain secret name")
		}

		// Should NEVER contain the actual secret value
		if strings.Contains(logOutput, "super-secret-value") {
			t.Error("SECURITY VIOLATION: log contains secret value!")
		}

		// Should contain value size
		if !strings.Contains(logOutput, "value_size") {
			t.Error("expected log to contain value_size")
		}
	})

	t.Run("GetValue logs metadata, not value", func(t *testing.T) {
		buf.Reset()

		value, _, err := logged.GetSecretValue(ctx, "existing")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		logOutput := buf.String()

		// Should contain operation and name
		if !strings.Contains(logOutput, "GetValue") {
			t.Error("expected log to contain 'GetValue'")
		}
		if !strings.Contains(logOutput, "existing") {
			t.Error("expected log to contain secret name")
		}

		// Should NEVER contain the actual secret value
		if strings.Contains(logOutput, string(value)) {
			t.Error("SECURITY VIOLATION: log contains secret value!")
		}

		// Should contain value size
		if !strings.Contains(logOutput, "value_size") {
			t.Error("expected log to contain value_size")
		}
	})

	t.Run("Update logs metadata, not value", func(t *testing.T) {
		buf.Reset()

		_, err := logged.UpdateSecret(ctx, "existing", secret.Value("new-secret-value"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		logOutput := buf.String()

		// Should contain operation and name
		if !strings.Contains(logOutput, "Update") {
			t.Error("expected log to contain 'Update'")
		}

		// Should NEVER contain the actual secret value
		if strings.Contains(logOutput, "new-secret-value") {
			t.Error("SECURITY VIOLATION: log contains secret value!")
		}
	})

	t.Run("Delete logs operation", func(t *testing.T) {
		buf.Reset()

		err := logged.DeleteSecret(ctx, "existing")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		logOutput := buf.String()

		if !strings.Contains(logOutput, "Delete") {
			t.Error("expected log to contain 'Delete'")
		}
		if !strings.Contains(logOutput, "existing") {
			t.Error("expected log to contain secret name")
		}
	})

	t.Run("List logs count", func(t *testing.T) {
		buf.Reset()

		count := 0
		for _, err := range logged.ListSecrets(ctx) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			count++
		}

		logOutput := buf.String()

		if !strings.Contains(logOutput, "List") {
			t.Error("expected log to contain 'List'")
		}
		if !strings.Contains(logOutput, "count") {
			t.Error("expected log to contain count")
		}
	})

	t.Run("errors are logged", func(t *testing.T) {
		buf.Reset()

		_, _, err := logged.GetSecretValue(ctx, "nonexistent")
		if err == nil {
			t.Fatal("expected error")
		}

		logOutput := buf.String()

		if !strings.Contains(logOutput, "error") || !strings.Contains(logOutput, "failed") {
			t.Error("expected log to contain error information")
		}
	})
}

func TestLoggerAdapter(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	ctx := t.Context()
	base := secret.NewManager(memory.NewBucket())
	adapter := secret.WithLogger(logger)

	logged := adapter.AdaptManager(base)

	if logged == base {
		t.Error("expected adapter to return a different manager")
	}

	// Verify it works as a logged manager by checking that operations are logged
	buf.Reset()
	logged.CreateSecret(ctx, "test", secret.Value("value"))
	if !strings.Contains(buf.String(), "Create") {
		t.Error("expected log output from wrapped manager")
	}
}
