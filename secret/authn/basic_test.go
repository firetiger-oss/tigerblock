package authn

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/firetiger-oss/storage/secret"
)

type testCredential struct {
	User string `json:"username"`
	Pass string `json:"password"`
	Role string `json:"role"`
}

func (c testCredential) Username() string { return c.User }
func (c testCredential) Password() string { return c.Pass }

type stringCredential string

func (c stringCredential) Username() string {
	username, _, _ := strings.Cut(string(c), ":")
	return username
}

func (c stringCredential) Password() string {
	_, password, _ := strings.Cut(string(c), ":")
	return password
}

func TestNewBasicAuthenticator(t *testing.T) {
	credsJSON := []byte(`{"username":"alice","password":"secret123","role":"admin"}`)

	makeStore := func(secrets map[string]secret.Value) secret.Provider {
		return secret.ProviderFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
			value, ok := secrets[name]
			if !ok {
				return nil, "", secret.ErrNotFound
			}
			return value, "", nil
		})
	}

	basicAuth := func(username, password string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	}

	t.Run("valid credentials", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"alice": credsJSON,
		})
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("Authorization", basicAuth("alice", "secret123"))

		ctx, err := auth.Authenticate(req.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, creds, ok := CredentialFromContext[testCredential](ctx)
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
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

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
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

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
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

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
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

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
	// Helper to create context with credentials and domain
	contextWithCredsAndDomain := func(creds BasicAuthCredential, domain string) context.Context {
		return ContextWithCredential(t.Context(), domain, creds)
	}

	t.Run("injects credentials when domain matches", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder(transport)

		creds := testCredential{User: "alice", Pass: "secret123"}
		ctx := contextWithCredsAndDomain(creds, "example.com")

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

	t.Run("injects credentials for subdomain of authenticated domain", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder(transport)

		creds := testCredential{User: "alice", Pass: "secret123"}
		ctx := contextWithCredsAndDomain(creds, "example.com")

		req := httptest.NewRequest("GET", "http://api.example.com/data", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		username, password, ok := capturedReq.BasicAuth()
		if !ok {
			t.Fatal("expected Basic Auth header to be set for subdomain")
		}
		if username != "alice" || password != "secret123" {
			t.Error("credentials not properly forwarded to subdomain")
		}
	})

	t.Run("does not inject credentials for different domain", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder(transport)

		creds := testCredential{User: "alice", Pass: "secret123"}
		ctx := contextWithCredsAndDomain(creds, "example.com")

		req := httptest.NewRequest("GET", "http://other-site.com/api", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("credentials should not be forwarded to different domain")
		}
	})

	t.Run("does not inject credentials for partial domain match", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder(transport)

		creds := testCredential{User: "alice", Pass: "secret123"}
		ctx := contextWithCredsAndDomain(creds, "example.com")

		// "notexample.com" ends with "example.com" but is not a subdomain
		req := httptest.NewRequest("GET", "http://notexample.com/api", nil)
		req = req.WithContext(ctx)

		_, err := forwarder.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("credentials should not be forwarded to partial domain match")
		}
	})

	t.Run("passes through without credentials", func(t *testing.T) {
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder(transport)

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

		forwarder := NewBasicAuthForwarder(transport)

		creds := testCredential{User: "alice", Pass: "secret123"}
		ctx := contextWithCredsAndDomain(creds, "example.com")

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

func TestNewBasicAuthTransport(t *testing.T) {
	makeStore := func(secrets map[string]secret.Value) secret.Provider {
		return secret.ProviderFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
			value, ok := secrets[name]
			if !ok {
				return nil, "", secret.ErrNotFound
			}
			return value, "", nil
		})
	}

	t.Run("injects credentials from store when domain matches", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"my-secret": secret.Value("alice:secret123"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBasicAuthTransport(NewLoader[stringCredential](store), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := authTransport.RoundTrip(req)
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

	t.Run("injects credentials for subdomain of configured domain", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"my-secret": secret.Value("alice:secret123"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBasicAuthTransport(NewLoader[stringCredential](store), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://api.example.com/data", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		username, password, ok := capturedReq.BasicAuth()
		if !ok {
			t.Fatal("expected Basic Auth header to be set for subdomain")
		}
		if username != "alice" || password != "secret123" {
			t.Error("credentials not properly injected for subdomain")
		}
	})

	t.Run("does not inject credentials for different domain", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"my-secret": secret.Value("alice:secret123"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBasicAuthTransport(NewLoader[stringCredential](store), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://other-site.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("credentials should not be injected for different domain")
		}
	})

	t.Run("does not inject credentials for partial domain match", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"my-secret": secret.Value("alice:secret123"),
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBasicAuthTransport(NewLoader[stringCredential](store), "my-secret", "example.com", transport)

		// "notexample.com" ends with "example.com" but is not a subdomain
		req := httptest.NewRequest("GET", "http://notexample.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("credentials should not be injected for partial domain match")
		}
	})

	t.Run("returns error when secret not found for matching domain", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{})

		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			t.Error("transport should not be called when secret lookup fails")
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBasicAuthTransport(NewLoader[stringCredential](store), "missing-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err == nil {
			t.Error("expected error when secret not found")
		}
	})

	t.Run("passes through without error for non-matching domain even with missing secret", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{})

		var called bool
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			called = true
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBasicAuthTransport(NewLoader[stringCredential](store), "missing-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://other-site.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !called {
			t.Error("transport should be called for non-matching domain")
		}
	})

	t.Run("does not modify original request", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"my-secret": secret.Value("alice:secret123"),
		})

		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBasicAuthTransport(NewLoader[stringCredential](store), "my-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if req.Header.Get("Authorization") != "" {
			t.Error("original request should not be modified")
		}
	})

	t.Run("works with JSON credentials", func(t *testing.T) {
		credsJSON := []byte(`{"username":"bob","password":"pass456","role":"user"}`)
		store := makeStore(map[string]secret.Value{
			"json-secret": credsJSON,
		})

		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		authTransport := NewBasicAuthTransport(NewLoader[testCredential](store), "json-secret", "example.com", transport)

		req := httptest.NewRequest("GET", "http://example.com/api", nil)

		_, err := authTransport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		username, password, ok := capturedReq.BasicAuth()
		if !ok {
			t.Fatal("expected Basic Auth header to be set")
		}
		if username != "bob" {
			t.Errorf("expected username 'bob', got %q", username)
		}
		if password != "pass456" {
			t.Errorf("expected password 'pass456', got %q", password)
		}
	})
}

func TestDomainContains(t *testing.T) {
	tests := []struct {
		name     string
		domain   string
		hostname string
		want     bool
	}{
		{
			name:     "exact match",
			domain:   "example.com",
			hostname: "example.com",
			want:     true,
		},
		{
			name:     "subdomain match",
			domain:   "example.com",
			hostname: "api.example.com",
			want:     true,
		},
		{
			name:     "deep subdomain match",
			domain:   "example.com",
			hostname: "a.b.c.example.com",
			want:     true,
		},
		{
			name:     "different domain",
			domain:   "example.com",
			hostname: "other.com",
			want:     false,
		},
		{
			name:     "partial match not subdomain",
			domain:   "example.com",
			hostname: "notexample.com",
			want:     false,
		},
		{
			name:     "partial match with dash",
			domain:   "example.com",
			hostname: "not-example.com",
			want:     false,
		},
		{
			name:     "empty domain",
			domain:   "",
			hostname: "example.com",
			want:     false,
		},
		{
			name:     "empty hostname",
			domain:   "example.com",
			hostname: "",
			want:     false,
		},
		{
			name:     "domain longer than hostname",
			domain:   "subdomain.example.com",
			hostname: "example.com",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := domainContains(tt.domain, tt.hostname)
			if got != tt.want {
				t.Errorf("domainContains(%q, %q) = %v, want %v", tt.domain, tt.hostname, got, tt.want)
			}
		})
	}
}

func TestBasicAuthenticatorStoresDomainInContext(t *testing.T) {
	credsJSON := []byte(`{"username":"alice","password":"secret123","role":"admin"}`)

	makeStore := func(secrets map[string]secret.Value) secret.Provider {
		return secret.ProviderFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
			value, ok := secrets[name]
			if !ok {
				return nil, "", secret.ErrNotFound
			}
			return value, "", nil
		})
	}

	basicAuth := func(username, password string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	}

	t.Run("extracts domain from request host", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"alice": credsJSON,
		})
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

		req := httptest.NewRequest("GET", "http://api.example.com/resource", nil)
		req.Host = "api.example.com"
		req.Header.Set("Authorization", basicAuth("alice", "secret123"))

		ctx, err := auth.Authenticate(req.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		domain, _, _ := CredentialFromContext[testCredential](ctx)
		if domain != "example.com" {
			t.Errorf("expected domain 'example.com', got %q", domain)
		}
	})

	t.Run("extracts domain from host with port", func(t *testing.T) {
		store := makeStore(map[string]secret.Value{
			"alice": credsJSON,
		})
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

		req := httptest.NewRequest("GET", "http://api.example.com:8080/resource", nil)
		req.Host = "api.example.com:8080"
		req.Header.Set("Authorization", basicAuth("alice", "secret123"))

		ctx, err := auth.Authenticate(req.Context(), req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		domain, _, _ := CredentialFromContext[testCredential](ctx)
		if domain != "example.com" {
			t.Errorf("expected domain 'example.com', got %q", domain)
		}
	})
}

func TestBasicAuthEndToEnd(t *testing.T) {
	// Test the full flow: authenticate on one domain, forward credentials to same domain
	credsJSON := []byte(`{"username":"alice","password":"secret123","role":"admin"}`)

	store := secret.ProviderFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
		if name == "alice" {
			return credsJSON, "", nil
		}
		return nil, "", secret.ErrNotFound
	})

	basicAuth := func(username, password string) string {
		return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
	}

	t.Run("credentials forwarded to same domain", func(t *testing.T) {
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

		// Simulate incoming request to api.example.com
		incomingReq := httptest.NewRequest("GET", "http://api.example.com/resource", nil)
		incomingReq.Host = "api.example.com"
		incomingReq.Header.Set("Authorization", basicAuth("alice", "secret123"))

		ctx, err := auth.Authenticate(incomingReq.Context(), incomingReq)
		if err != nil {
			t.Fatalf("authentication failed: %v", err)
		}

		// Now make an outbound request to the same domain
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder(transport)

		outboundReq := httptest.NewRequest("GET", "http://backend.example.com/data", nil)
		outboundReq = outboundReq.WithContext(ctx)

		_, err = forwarder.RoundTrip(outboundReq)
		if err != nil {
			t.Fatalf("round trip failed: %v", err)
		}

		// Credentials should be forwarded since backend.example.com is under example.com
		username, password, ok := capturedReq.BasicAuth()
		if !ok {
			t.Fatal("expected credentials to be forwarded to same domain")
		}
		if username != "alice" || password != "secret123" {
			t.Error("incorrect credentials forwarded")
		}
	})

	t.Run("credentials not forwarded to different domain", func(t *testing.T) {
		auth := NewBasicAuthenticator(NewLoader[testCredential](store))

		// Simulate incoming request to api.example.com
		incomingReq := httptest.NewRequest("GET", "http://api.example.com/resource", nil)
		incomingReq.Host = "api.example.com"
		incomingReq.Header.Set("Authorization", basicAuth("alice", "secret123"))

		ctx, err := auth.Authenticate(incomingReq.Context(), incomingReq)
		if err != nil {
			t.Fatalf("authentication failed: %v", err)
		}

		// Now make an outbound request to a different domain
		var capturedReq *http.Request
		transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK}, nil
		})

		forwarder := NewBasicAuthForwarder(transport)

		outboundReq := httptest.NewRequest("GET", "http://evil.com/steal-creds", nil)
		outboundReq = outboundReq.WithContext(ctx)

		_, err = forwarder.RoundTrip(outboundReq)
		if err != nil {
			t.Fatalf("round trip failed: %v", err)
		}

		// Credentials should NOT be forwarded to different domain
		if capturedReq.Header.Get("Authorization") != "" {
			t.Error("credentials should not be forwarded to different domain")
		}
	})
}
