package authn

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
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
		_, _, ok := CredentialFromContext[simpleCredential](ctx)
		if ok {
			t.Error("expected ok to be false")
		}
	})

	t.Run("round-trips credentials and domain", func(t *testing.T) {
		original := simpleCredential{User: "alice", Role: "admin"}
		ctx := ContextWithCredential(t.Context(), "example.com", original)

		domain, retrieved, ok := CredentialFromContext[simpleCredential](ctx)
		if !ok {
			t.Fatal("expected credentials to be present")
		}
		if domain != "example.com" {
			t.Errorf("expected domain %q, got %q", "example.com", domain)
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
			return ContextWithCredential(ctx, "example.com", creds), nil
		})

		var receivedCreds simpleCredential
		var credsFound bool
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, receivedCreds, credsFound = CredentialFromContext[simpleCredential](r.Context())
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
			return ContextWithCredential(ctx, "example.com", creds), nil
		})

		var receivedCreds simpleCredential
		var credsFound bool
		next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, receivedCreds, credsFound = CredentialFromContext[simpleCredential](r.Context())
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

func TestIsConnectRPC(t *testing.T) {
	tests := []struct {
		name        string
		headers     map[string]string
		contentType string
		want        bool
	}{
		{
			name:    "Connect-Protocol-Version header",
			headers: map[string]string{"Connect-Protocol-Version": "1"},
			want:    true,
		},
		{
			name:        "application/connect+json content type",
			contentType: "application/connect+json",
			want:        true,
		},
		{
			name:        "application/connect+proto content type",
			contentType: "application/connect+proto",
			want:        true,
		},
		{
			name:        "application/grpc content type",
			contentType: "application/grpc",
			want:        true,
		},
		{
			name:        "application/grpc-web content type",
			contentType: "application/grpc-web",
			want:        true,
		},
		{
			name:        "application/json without Connect header",
			contentType: "application/json",
			want:        false,
		},
		{
			name:        "no headers",
			contentType: "",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			if tt.contentType != "" {
				req.Header.Set("Content-Type", tt.contentType)
			}

			if got := isConnectRPC(req); got != tt.want {
				t.Errorf("isConnectRPC() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsS3Request(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    bool
	}{
		{
			name:    "X-Amz-Date header",
			headers: map[string]string{"X-Amz-Date": "20230101T000000Z"},
			want:    true,
		},
		{
			name:    "X-Amz-Content-Sha256 header",
			headers: map[string]string{"X-Amz-Content-Sha256": "abc123"},
			want:    true,
		},
		{
			name:    "AWS4-HMAC-SHA256 authorization",
			headers: map[string]string{"Authorization": "AWS4-HMAC-SHA256 Credential=..."},
			want:    true,
		},
		{
			name:    "AWS authorization (v2)",
			headers: map[string]string{"Authorization": "AWS accesskey:signature"},
			want:    true,
		},
		{
			name:    "Bearer authorization",
			headers: map[string]string{"Authorization": "Bearer token"},
			want:    false,
		},
		{
			name:    "no AWS headers",
			headers: map[string]string{"Content-Type": "application/json"},
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			if got := isS3Request(req); got != tt.want {
				t.Errorf("isS3Request() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteUnauthorizedError(t *testing.T) {
	failingAuth := AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
		return nil, ErrUnauthorized
	})

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("next handler should not be called")
	})

	handler := NewHandler(next, failingAuth)

	t.Run("ConnectRPC request returns JSON error", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/package.Service/Method", nil)
		req.Header.Set("Connect-Protocol-Version", "1")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", ct)
		}
		body := rec.Body.String()
		if !strings.Contains(body, `"code":"unauthenticated"`) {
			t.Errorf("expected JSON with unauthenticated code, got %q", body)
		}
	})

	t.Run("S3 request returns XML error", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/bucket/key", nil)
		req.Header.Set("X-Amz-Date", "20230101T000000Z")
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/xml" {
			t.Errorf("expected Content-Type application/xml, got %q", ct)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "<Code>AccessDenied</Code>") {
			t.Errorf("expected XML with AccessDenied code, got %q", body)
		}
	})

	t.Run("plain request returns text error", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/resource", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected status 401, got %d", rec.Code)
		}
		body := rec.Body.String()
		if !strings.Contains(body, "Unauthorized") {
			t.Errorf("expected plain text Unauthorized, got %q", body)
		}
	})

	t.Run("WWW-Authenticate absent when only non-contributing authenticators", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/resource", nil)
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		if got := rec.Header().Get("WWW-Authenticate"); got != "" {
			t.Errorf("expected no WWW-Authenticate header, got %q", got)
		}
	})
}

func TestChallengeString(t *testing.T) {
	tests := []struct {
		name string
		c    Challenge
		want string
	}{
		{
			name: "zero value",
			c:    Challenge{},
			want: "",
		},
		{
			name: "scheme only",
			c:    Challenge{Scheme: "Bearer"},
			want: "Bearer",
		},
		{
			name: "single param",
			c:    Challenge{Scheme: "Basic", Params: map[string]string{"realm": "example.com"}},
			want: `Basic realm="example.com"`,
		},
		{
			name: "multiple params sorted alphabetically",
			c: Challenge{Scheme: "Bearer", Params: map[string]string{
				"realm": "example.com",
				"scope": "read write",
				"error": "invalid_token",
			}},
			want: `Bearer error="invalid_token", realm="example.com", scope="read write"`,
		},
		{
			name: "quote and backslash escaped",
			c:    Challenge{Scheme: "Basic", Params: map[string]string{"realm": `we"ird\value`}},
			want: `Basic realm="we\"ird\\value"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.String(); got != tt.want {
				t.Errorf("Challenge.String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestChallengeIsZero(t *testing.T) {
	if !(Challenge{}).IsZero() {
		t.Error("zero Challenge should report IsZero")
	}
	if (Challenge{Scheme: "Basic"}).IsZero() {
		t.Error("Challenge with scheme should not report IsZero")
	}
}

func TestNewHandlerWWWAuthenticate(t *testing.T) {
	challenging := func(scheme string) Authenticator {
		return challengeAuthenticator{scheme: scheme}
	}

	t.Run("single challenge", func(t *testing.T) {
		handler := NewHandler(
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("next handler should not be called")
			}),
			challenging("Basic"),
		)

		req := httptest.NewRequest("GET", "http://example.com:8080/resource", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401, got %d", rec.Code)
		}
		want := `Basic realm="example.com:8080"`
		if got := rec.Header().Get("WWW-Authenticate"); got != want {
			t.Errorf("WWW-Authenticate = %q, want %q", got, want)
		}
	})

	t.Run("multiple challenges composed in order", func(t *testing.T) {
		handler := NewHandler(
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("next handler should not be called")
			}),
			challenging("Basic"),
			challenging("Bearer"),
		)

		req := httptest.NewRequest("GET", "http://example.com/resource", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		want := `Basic realm="example.com", Bearer realm="example.com"`
		if got := rec.Header().Get("WWW-Authenticate"); got != want {
			t.Errorf("WWW-Authenticate = %q, want %q", got, want)
		}
	})

	t.Run("non-contributing authenticators skipped", func(t *testing.T) {
		handler := NewHandler(
			http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				t.Error("next handler should not be called")
			}),
			AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
				return nil, ErrNotFound
			}),
			challenging("Bearer"),
		)

		req := httptest.NewRequest("GET", "http://example.com/resource", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		want := `Bearer realm="example.com"`
		if got := rec.Header().Get("WWW-Authenticate"); got != want {
			t.Errorf("WWW-Authenticate = %q, want %q", got, want)
		}
	})
}

// challengeAuthenticator always fails with ErrNotFound and contributes a
// Basic- or Bearer-style challenge built from the request Host. It's a test
// double that avoids pulling the full Basic/Bearer credential-loading path.
type challengeAuthenticator struct {
	scheme string
}

func (a challengeAuthenticator) Authenticate(context.Context, *http.Request) (context.Context, error) {
	return nil, ErrNotFound
}

func (a challengeAuthenticator) Challenge(req *http.Request) Challenge {
	return Challenge{
		Scheme: a.scheme,
		Params: map[string]string{"realm": req.Host},
	}
}
