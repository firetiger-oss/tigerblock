package authn

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/firetiger-oss/storage/secret"
)

type testBearerCredential struct {
	APIKey string `json:"api_key"`
	Scope  string `json:"scope"`
}

func (c testBearerCredential) Token() string { return c.APIKey }

type stringBearerCredential string

func (c stringBearerCredential) Token() string { return string(c) }

func TestNewBearerAuthenticator(t *testing.T) {
	makeProvider := func(secrets map[string]secret.Value) secret.Provider {
		return secret.ProviderFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
			value, ok := secrets[name]
			if !ok {
				return nil, "", secret.ErrNotFound
			}
			return value, "", nil
		})
	}

	t.Run("valid token", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"api-token": secret.Value(`{"api_key":"secret-token-123","scope":"read:all"}`),
		})
		auth := NewBearerAuthenticator(NewLoader[testBearerCredential](provider), "api-token")

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer secret-token-123")

		ctx, err := auth.Authenticate(req.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, cred, ok := CredentialFromContext[testBearerCredential](ctx)
		if !ok {
			t.Fatal("expected credential in context")
		}
		if string(cred.Token()) != "secret-token-123" {
			t.Errorf("expected token 'secret-token-123', got %q", cred.Token())
		}
		if cred.Scope != "read:all" {
			t.Errorf("expected scope 'read:all', got %q", cred.Scope)
		}
	})

	t.Run("invalid token", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"api-token": secret.Value(`{"api_key":"secret-token-123","scope":"read:all"}`),
		})
		auth := NewBearerAuthenticator(NewLoader[testBearerCredential](provider), "api-token")

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer wrong-token")

		_, err := auth.Authenticate(req.Context(), req)
		if err != ErrUnauthorized {
			t.Errorf("expected ErrUnauthorized, got %v", err)
		}
	})

	t.Run("unknown token ID", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{})
		auth := NewBearerAuthenticator(NewLoader[testBearerCredential](provider), "missing-token")

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer any-token")

		_, err := auth.Authenticate(req.Context(), req)
		if err != ErrUnauthorized {
			t.Errorf("expected ErrUnauthorized, got %v", err)
		}
	})

	t.Run("missing auth header", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"api-token": secret.Value(`{"api_key":"secret-token-123","scope":"read:all"}`),
		})
		auth := NewBearerAuthenticator(NewLoader[testBearerCredential](provider), "api-token")

		req := httptest.NewRequest("GET", "/", nil)

		_, err := auth.Authenticate(req.Context(), req)
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound, got %v", err)
		}
	})

	t.Run("basic auth header instead of bearer", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"api-token": secret.Value(`{"api_key":"secret-token-123","scope":"read:all"}`),
		})
		auth := NewBearerAuthenticator(NewLoader[testBearerCredential](provider), "api-token")

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")

		_, err := auth.Authenticate(req.Context(), req)
		if err != ErrNotFound {
			t.Errorf("expected ErrNotFound for non-bearer auth, got %v", err)
		}
	})

	t.Run("invalid JSON in secret", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"api-token": secret.Value("not json"),
		})
		auth := NewBearerAuthenticator(NewLoader[testBearerCredential](provider), "api-token")

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", "Bearer any-token")

		_, err := auth.Authenticate(req.Context(), req)
		if err == nil {
			t.Error("expected error for invalid JSON")
		}
		if err == ErrUnauthorized {
			t.Error("expected non-auth error for invalid JSON")
		}
	})
}

func TestNewBearerAuthForwarder(t *testing.T) {
	contextWithCredAndDomain := func(cred BearerCredential, domain string) context.Context {
		return ContextWithCredential(t.Context(), domain, cred)
	}

	t.Run("injects token when domain matches", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		cred := stringBearerCredential("my-api-token")
		ctx := contextWithCredAndDomain(cred, "example.com")

		req := httptest.NewRequest("GET", "http://example.com/api", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq == nil {
			t.Fatal("expected request to be captured")
		}

		auth := capturedReq.Header.Get("Authorization")
		if auth != "Bearer my-api-token" {
			t.Errorf("expected 'Bearer my-api-token', got %q", auth)
		}
	})

	t.Run("injects token for subdomain of authenticated domain", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		cred := stringBearerCredential("my-api-token")
		ctx := contextWithCredAndDomain(cred, "example.com")

		req := httptest.NewRequest("GET", "http://api.example.com/data", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if auth != "Bearer my-api-token" {
			t.Errorf("expected 'Bearer my-api-token', got %q", auth)
		}
	})

	t.Run("does not inject token for different domain", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		cred := stringBearerCredential("my-api-token")
		ctx := contextWithCredAndDomain(cred, "example.com")

		req := httptest.NewRequest("GET", "http://other-site.com/api", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("token should not be forwarded to different domain")
		}
	})

	t.Run("does not inject token for partial domain match", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		cred := stringBearerCredential("my-api-token")
		ctx := contextWithCredAndDomain(cred, "example.com")

		req := httptest.NewRequest("GET", "http://notexample.com/api", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("token should not be forwarded to partial domain match")
		}
	})

	t.Run("passes through without credentials", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("expected no Authorization header when no credentials in context")
		}
	})

	t.Run("does not overwrite existing authorization", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		cred := stringBearerCredential("my-api-token")
		ctx := contextWithCredAndDomain(cred, "example.com")

		req := httptest.NewRequest("GET", "http://example.com/api", nil)
		req.Header.Set("Authorization", "Bearer existing-token")
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if auth != "Bearer existing-token" {
			t.Errorf("expected existing token to be preserved, got %q", auth)
		}
	})

	t.Run("does not modify original request", func(t *testing.T) {
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		cred := stringBearerCredential("my-api-token")
		ctx := contextWithCredAndDomain(cred, "example.com")

		req := httptest.NewRequest("GET", "http://example.com/api", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if req.Header.Get("Authorization") != "" {
			t.Error("original request should not be modified")
		}
	})
}

func TestNewBearerAuthTransport(t *testing.T) {
	makeProvider := func(secrets map[string]secret.Value) secret.Provider {
		return secret.ProviderFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
			value, ok := secrets[name]
			if !ok {
				return nil, "", secret.ErrNotFound
			}
			return value, "", nil
		})
	}

	t.Run("injects token from provider when domain matches", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"my-secret": secret.Value("api-token-xyz"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBearerAuthTransport(NewLoader[stringBearerCredential](provider), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if auth != "Bearer api-token-xyz" {
			t.Errorf("expected 'Bearer api-token-xyz', got %q", auth)
		}
	})

	t.Run("injects token for subdomain of configured domain", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"my-secret": secret.Value("api-token-xyz"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBearerAuthTransport(NewLoader[stringBearerCredential](provider), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://api.example.com/data", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if auth != "Bearer api-token-xyz" {
			t.Errorf("expected 'Bearer api-token-xyz', got %q", auth)
		}
	})

	t.Run("does not inject token for different domain", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"my-secret": secret.Value("api-token-xyz"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBearerAuthTransport(NewLoader[stringBearerCredential](provider), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://other-site.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("token should not be injected for different domain")
		}
	})

	t.Run("injects token for all domains with wildcard", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"my-secret": secret.Value("api-token-xyz"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBearerAuthTransport(NewLoader[stringBearerCredential](provider), "my-secret", "*", transport)

		req := httptest.NewRequest("GET", "http://any-domain.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if auth != "Bearer api-token-xyz" {
			t.Errorf("expected 'Bearer api-token-xyz', got %q", auth)
		}
	})

	t.Run("returns error when secret not found for matching domain", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{})

		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			t.Error("transport should not be called when secret lookup fails")
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBearerAuthTransport(NewLoader[stringBearerCredential](provider), "missing-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err == nil {
			t.Error("expected error when secret not found")
		}
	})

	t.Run("passes through without error for non-matching domain even with missing secret", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{})

		var called bool
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBearerAuthTransport(NewLoader[stringBearerCredential](provider), "missing-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://other-site.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Error("transport should be called for non-matching domain")
		}
	})

	t.Run("does not overwrite existing authorization", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"my-secret": secret.Value("api-token-xyz"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBearerAuthTransport(NewLoader[stringBearerCredential](provider), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)
		req.Header.Set("Authorization", "Bearer existing-token")

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if auth != "Bearer existing-token" {
			t.Errorf("expected existing token to be preserved, got %q", auth)
		}
	})

	t.Run("does not modify original request", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"my-secret": secret.Value("api-token-xyz"),
		})

		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBearerAuthTransport(NewLoader[stringBearerCredential](provider), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if req.Header.Get("Authorization") != "" {
			t.Error("original request should not be modified")
		}
	})
}

func TestBearerAuthenticatorStoresDomainInContext(t *testing.T) {
	makeProvider := func(secrets map[string]secret.Value) secret.Provider {
		return secret.ProviderFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
			value, ok := secrets[name]
			if !ok {
				return nil, "", secret.ErrNotFound
			}
			return value, "", nil
		})
	}

	t.Run("extracts domain from request host", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"api-token": secret.Value("secret-token"),
		})
		auth := NewBearerAuthenticator(NewLoader[stringBearerCredential](provider), "api-token")

		req := httptest.NewRequest("GET", "http://api.example.com/resource", nil)
		req.Host = "api.example.com"
		req.Header.Set("Authorization", "Bearer secret-token")

		ctx, err := auth.Authenticate(req.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		domain, _, _ := CredentialFromContext[stringBearerCredential](ctx)
		if domain != "example.com" {
			t.Errorf("expected domain 'example.com', got %q", domain)
		}
	})

	t.Run("extracts domain from host with port", func(t *testing.T) {
		provider := makeProvider(map[string]secret.Value{
			"api-token": secret.Value("secret-token"),
		})
		auth := NewBearerAuthenticator(NewLoader[stringBearerCredential](provider), "api-token")

		req := httptest.NewRequest("GET", "http://api.example.com:8080/resource", nil)
		req.Host = "api.example.com:8080"
		req.Header.Set("Authorization", "Bearer secret-token")

		ctx, err := auth.Authenticate(req.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		domain, _, _ := CredentialFromContext[stringBearerCredential](ctx)
		if domain != "example.com" {
			t.Errorf("expected domain 'example.com', got %q", domain)
		}
	})
}

func TestBearerAuthEndToEnd(t *testing.T) {
	provider := secret.ProviderFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
		if name == "api-token" {
			return secret.Value("secret-token-123"), "", nil
		}
		return nil, "", secret.ErrNotFound
	})

	t.Run("token forwarded to same domain", func(t *testing.T) {
		authenticator := NewBearerAuthenticator(NewLoader[stringBearerCredential](provider), "api-token")

		incomingReq := httptest.NewRequest("GET", "http://api.example.com/resource", nil)
		incomingReq.Host = "api.example.com"
		incomingReq.Header.Set("Authorization", "Bearer secret-token-123")

		ctx, err := authenticator.Authenticate(incomingReq.Context(), incomingReq)
		if err != nil {
			t.Fatalf("authentication failed: %v", err)
		}

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		outboundReq := httptest.NewRequest("GET", "http://backend.example.com/data", nil)
		outboundReq = outboundReq.WithContext(ctx)

		_, err = forwarder.RoundTrip(outboundReq)
		if err != nil {
			t.Fatalf("round trip failed: %v", err)
		}

		authHeader := capturedReq.Header.Get("Authorization")
		if authHeader != "Bearer secret-token-123" {
			t.Errorf("expected token to be forwarded, got %q", authHeader)
		}
	})

	t.Run("token not forwarded to different domain", func(t *testing.T) {
		authenticator := NewBearerAuthenticator(NewLoader[stringBearerCredential](provider), "api-token")

		incomingReq := httptest.NewRequest("GET", "http://api.example.com/resource", nil)
		incomingReq.Host = "api.example.com"
		incomingReq.Header.Set("Authorization", "Bearer secret-token-123")

		ctx, err := authenticator.Authenticate(incomingReq.Context(), incomingReq)
		if err != nil {
			t.Fatalf("authentication failed: %v", err)
		}

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBearerAuthForwarder(transport)

		outboundReq := httptest.NewRequest("GET", "http://evil.com/steal-token", nil)
		outboundReq = outboundReq.WithContext(ctx)

		_, err = forwarder.RoundTrip(outboundReq)
		if err != nil {
			t.Fatalf("round trip failed: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("token should not be forwarded to different domain")
		}
	})
}
