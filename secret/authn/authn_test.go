package authn

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/firetiger-oss/storage/secret"
)

type testCredentials struct {
	User string `json:"username"`
	Pass string `json:"password"`
	Role string `json:"role"`
}

func (c testCredentials) Username() string { return c.User }
func (c testCredentials) Password() string { return c.Pass }

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
		_, ok := CredentialsFromContext[testCredentials](ctx)
		if ok {
			t.Error("expected ok to be false")
		}
	})

	t.Run("round-trips credentials", func(t *testing.T) {
		original := testCredentials{User: "alice", Pass: "secret", Role: "admin"}
		ctx := ContextWithCredentials(t.Context(), original)

		retrieved, ok := CredentialsFromContext[testCredentials](ctx)
		if !ok {
			t.Fatal("expected credentials to be present")
		}
		if retrieved.Username() != original.Username() {
			t.Errorf("expected username %q, got %q", original.Username(), retrieved.Username())
		}
		if retrieved.Role != original.Role {
			t.Errorf("expected role %q, got %q", original.Role, retrieved.Role)
		}
	})
}

func TestNewHandler(t *testing.T) {
	t.Run("success passes context to next handler", func(t *testing.T) {
		creds := testCredentials{User: "alice", Role: "admin"}
		auth := AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
			return ContextWithCredentials(ctx, creds), nil
		})

		var receivedCreds testCredentials
		var credsFound bool
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			receivedCreds, credsFound = CredentialsFromContext[testCredentials](r.Context())
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
		if receivedCreds.Username() != "alice" {
			t.Errorf("expected username 'alice', got %q", receivedCreds.Username())
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
}

func TestNewBasicAuthenticator(t *testing.T) {
	credsJSON := []byte(`{"username":"alice","password":"secret123","role":"admin"}`)

	makeStore := func(secrets map[string]secret.Value) secret.Store {
		return secret.StoreFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, secret.Info, error) {
			value, ok := secrets[name]
			if !ok {
				return nil, secret.Info{}, secret.ErrNotFound
			}
			return value, secret.Info{Name: name}, nil
		})
	}

	basicAuth := func(username, password string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	}

	t.Run("valid credentials", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"alice": credsJSON,
		})
		auth := NewBasicAuthenticator[testCredentials](store)

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", basicAuth("alice", "secret123"))

		ctx, err := auth.Authenticate(req.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		creds, ok := CredentialsFromContext[testCredentials](ctx)
		if !ok {
			t.Fatal("expected credentials in context")
		}
		if creds.Username() != "alice" {
			t.Errorf("expected username 'alice', got %q", creds.Username())
		}
		if creds.Role != "admin" {
			t.Errorf("expected role 'admin', got %q", creds.Role)
		}
	})

	t.Run("invalid password", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"alice": credsJSON,
		})
		auth := NewBasicAuthenticator[testCredentials](store)

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", basicAuth("alice", "wrongpassword"))

		_, err := auth.Authenticate(req.Context(), req)
		if err != ErrUnauthorized {
			t.Errorf("expected ErrUnauthorized, got %v", err)
		}
	})

	t.Run("unknown user", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"alice": credsJSON,
		})
		auth := NewBasicAuthenticator[testCredentials](store)

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", basicAuth("bob", "secret123"))

		_, err := auth.Authenticate(req.Context(), req)
		if err != ErrUnauthorized {
			t.Errorf("expected ErrUnauthorized, got %v", err)
		}
	})

	t.Run("missing auth header", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"alice": credsJSON,
		})
		auth := NewBasicAuthenticator[testCredentials](store)

		req := httptest.NewRequest("GET", "/", nil)

		_, err := auth.Authenticate(req.Context(), req)
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("invalid JSON in secret", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"alice": secret.Value("not json"),
		})
		auth := NewBasicAuthenticator[testCredentials](store)

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", basicAuth("alice", "secret123"))

		_, err := auth.Authenticate(req.Context(), req)
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
		// Should not be ErrUnauthorized - it's a server error
		if err == ErrUnauthorized {
			t.Error("expected non-auth error for invalid JSON")
		}
	})
}

func TestNewBasicAuthForwarder(t *testing.T) {
	t.Run("injects credentials when present", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder[testCredentials](transport)

		creds := testCredentials{User: "alice", Pass: "secret123"}
		ctx := ContextWithCredentials(t.Context(), creds)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq == nil {
			t.Fatal("expected request to be captured")
		}

		username, password, ok := capturedReq.BasicAuth()
		if !ok {
			t.Fatal("expected Basic Auth header to be set")
		}
		if username != "alice" {
			t.Errorf("expected username 'alice', got %q", username)
		}
		if password != "secret123" {
			t.Errorf("expected password 'secret123', got %q", password)
		}
	})

	t.Run("passes through without credentials", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder[testCredentials](transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq == nil {
			t.Fatal("expected request to be captured")
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("expected no Authorization header when no credentials in context")
		}
	})

	t.Run("does not modify original request", func(t *testing.T) {
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder[testCredentials](transport)

		creds := testCredentials{User: "alice", Pass: "secret123"}
		ctx := ContextWithCredentials(t.Context(), creds)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Original request should not have Authorization header
		if req.Header.Get("Authorization") != "" {
			t.Error("original request should not be modified")
		}
	})
}
