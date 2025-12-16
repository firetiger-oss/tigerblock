package authn

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

type simpleCredential struct {
	User string
	Role string
}

type jsonCredential struct {
	User string `json:"username"`
	Pass string `json:"password"`
	Role string `json:"role"`
}

type customString string

func TestAuthenticatorFunc(t *testing.T) {
	ctx := t.Context()
	req := httptest.NewRequest("GET", "/", nil)

	called := false
	auth := AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
		called = true
		return ctx, nil
	})

	_, err := auth.Authenticate(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected function to be called")
	}
}

func TestContextCredentials(t *testing.T) {
	t.Run("returns false when not present", func(t *testing.T) {
		ctx := t.Context()
		_, ok := CredentialFromContext[simpleCredential](ctx)
		if ok {
			t.Error("expected ok to be false")
		}
	})

	t.Run("round-trips credentials", func(t *testing.T) {
		original := simpleCredential{User: "alice", Role: "admin"}
		ctx := ContextWithCredential(t.Context(), original)

		retrieved, ok := CredentialFromContext[simpleCredential](ctx)
		if !ok {
			t.Fatal("expected credentials to be present")
		}
		if retrieved.User != original.User {
			t.Errorf("expected user %q, got %q", original.User, retrieved.User)
		}
		if retrieved.Role != original.Role {
			t.Errorf("expected role %q, got %q", original.Role, retrieved.Role)
		}
	})
}

func TestNewHandler(t *testing.T) {
	t.Run("success passes context to next handler", func(t *testing.T) {
		creds := simpleCredential{User: "alice", Role: "admin"}
		auth := AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
			return ContextWithCredential(ctx, creds), nil
		})

		var receivedCreds simpleCredential
		var credsFound bool
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedCreds, credsFound = CredentialFromContext[simpleCredential](r.Context())
			w.WriteHeader(http.StatusOK)
		})

		handler := NewHandler(next, auth)

		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
		if !credsFound {
			t.Fatal("expected credentials in context")
		}
		if receivedCreds.User != "alice" {
			t.Errorf("expected user 'alice', got %q", receivedCreds.User)
		}
	})

	t.Run("failure returns 401", func(t *testing.T) {
		auth := AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
			return nil, ErrUnauthorized
		})

		nextCalled := false
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
		})

		handler := NewHandler(next, auth)

		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", rec.Code)
		}
		if nextCalled {
			t.Error("expected next handler not to be called")
		}
	})

	t.Run("tries next authenticator on ErrNotFound", func(t *testing.T) {
		creds := simpleCredential{User: "bob", Role: "user"}

		auth1 := AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
			return nil, ErrNotFound
		})
		auth2 := AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
			return ContextWithCredential(ctx, creds), nil
		})

		var receivedCreds simpleCredential
		var credsFound bool
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedCreds, credsFound = CredentialFromContext[simpleCredential](r.Context())
			w.WriteHeader(http.StatusOK)
		})

		handler := NewHandler(next, auth1, auth2)

		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
		if !credsFound {
			t.Fatal("expected credentials in context")
		}
		if receivedCreds.User != "bob" {
			t.Errorf("expected user 'bob', got %q", receivedCreds.User)
		}
	})
}

func TestMarshalUnmarshal(t *testing.T) {
	t.Run("JSON struct round trip", func(t *testing.T) {
		original := jsonCredential{User: "alice", Pass: "secret123", Role: "admin"}

		value, err := Marshal(original)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}

		unmarshaled, err := Unmarshal[jsonCredential](value)
		if err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}

		if unmarshaled.User != original.User {
			t.Errorf("expected user %q, got %q", original.User, unmarshaled.User)
		}
		if unmarshaled.Pass != original.Pass {
			t.Errorf("expected pass %q, got %q", original.Pass, unmarshaled.Pass)
		}
		if unmarshaled.Role != original.Role {
			t.Errorf("expected role %q, got %q", original.Role, unmarshaled.Role)
		}
	})

	t.Run("string type round trip", func(t *testing.T) {
		original := customString("alice:secret123")

		value, err := Marshal(original)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}

		if string(value) != "alice:secret123" {
			t.Errorf("expected value 'alice:secret123', got %q", value)
		}

		unmarshaled, err := Unmarshal[customString](value)
		if err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}

		if string(unmarshaled) != "alice:secret123" {
			t.Errorf("expected 'alice:secret123', got %q", unmarshaled)
		}
	})

	t.Run("byte slice round trip", func(t *testing.T) {
		original := []byte("raw credentials data")

		value, err := Marshal(original)
		if err != nil {
			t.Fatalf("Marshal error: %v", err)
		}

		if string(value) != "raw credentials data" {
			t.Errorf("expected value 'raw credentials data', got %q", value)
		}

		unmarshaled, err := Unmarshal[[]byte](value)
		if err != nil {
			t.Fatalf("Unmarshal error: %v", err)
		}

		if string(unmarshaled) != "raw credentials data" {
			t.Errorf("expected 'raw credentials data', got %q", unmarshaled)
		}
	})
}
