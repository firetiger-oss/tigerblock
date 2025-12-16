package authn

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/firetiger-oss/storage/secret"
)

type mockStore struct {
	secrets map[string]mockSecret
}

type mockSecret struct {
	value   secret.Value
	version string
}

func (m *mockStore) GetSecret(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, secret.Info, error) {
	s, ok := m.secrets[name]
	if !ok {
		return nil, secret.Info{}, secret.ErrNotFound
	}
	opts := secret.NewGetOptions(options...)
	if v := opts.Version(); v != "" && v != s.version {
		return nil, secret.Info{}, secret.ErrVersionNotFound
	}
	return s.value, secret.Info{Name: name, Version: s.version}, nil
}

func TestSignedURLAuthenticator(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   secret.Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := secret.NewHMAC256(store, "my-secret")
	ctx := t.Context()

	originalURL, _ := url.Parse("https://example.com/path/to/object")
	signedURL, err := signer.Sign(ctx, http.MethodGet, originalURL, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, _ := url.Parse(signedURL)
	req := httptest.NewRequest(http.MethodGet, parsedURL.String(), nil)
	req.URL = parsedURL

	auth := NewSignedURLAuthenticator(store)
	resultCtx, err := auth.Authenticate(ctx, req)
	if err != nil {
		t.Fatalf("expected success, got error: %v", err)
	}

	_, creds, ok := CredentialFromContext[SignedURLCredential](resultCtx)
	if !ok {
		t.Fatal("expected credentials in context")
	}

	if creds.Method != http.MethodGet {
		t.Errorf("expected method GET, got %s", creds.Method)
	}

	if creds.Path != "/path/to/object" {
		t.Errorf("expected path /path/to/object, got %s", creds.Path)
	}

	if creds.Expiration.IsZero() {
		t.Error("expected non-zero expiration")
	}
}

func TestSignedURLAuthenticatorNoSignature(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{},
	}

	req := httptest.NewRequest(http.MethodGet, "https://example.com/path/to/object", nil)

	auth := NewSignedURLAuthenticator(store)
	_, err := auth.Authenticate(t.Context(), req)

	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got: %v", err)
	}
}

func TestSignedURLAuthenticatorExpired(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   secret.Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := secret.NewHMAC256(store, "my-secret")
	ctx := t.Context()

	originalURL, _ := url.Parse("https://example.com/path/to/object")
	signedURL, err := signer.Sign(ctx, http.MethodGet, originalURL, time.Now().Add(-1*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, _ := url.Parse(signedURL)
	req := httptest.NewRequest(http.MethodGet, parsedURL.String(), nil)
	req.URL = parsedURL

	auth := NewSignedURLAuthenticator(store)
	_, err = auth.Authenticate(ctx, req)

	if !errors.Is(err, secret.ErrSignatureExpired) {
		t.Errorf("expected ErrSignatureExpired, got: %v", err)
	}
}

func TestSignedURLAuthenticatorInvalid(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   secret.Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := secret.NewHMAC256(store, "my-secret")
	ctx := t.Context()

	originalURL, _ := url.Parse("https://example.com/path/to/object")
	signedURL, err := signer.Sign(ctx, http.MethodGet, originalURL, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, _ := url.Parse(signedURL)
	q := parsedURL.Query()
	q.Set("sig", "tampered")
	parsedURL.RawQuery = q.Encode()

	req := httptest.NewRequest(http.MethodGet, parsedURL.String(), nil)
	req.URL = parsedURL

	auth := NewSignedURLAuthenticator(store)
	_, err = auth.Authenticate(ctx, req)

	if !errors.Is(err, secret.ErrSignatureInvalid) {
		t.Errorf("expected ErrSignatureInvalid, got: %v", err)
	}
}

func TestSignedURLAuthenticatorWrongMethod(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   secret.Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := secret.NewHMAC256(store, "my-secret")
	ctx := t.Context()

	originalURL, _ := url.Parse("https://example.com/path/to/object")
	signedURL, err := signer.Sign(ctx, http.MethodGet, originalURL, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, _ := url.Parse(signedURL)
	req := httptest.NewRequest(http.MethodPost, parsedURL.String(), nil)
	req.URL = parsedURL

	auth := NewSignedURLAuthenticator(store)
	_, err = auth.Authenticate(ctx, req)

	if !errors.Is(err, secret.ErrSignatureInvalid) {
		t.Errorf("expected ErrSignatureInvalid for wrong method, got: %v", err)
	}
}

func TestNewHandlerChaining(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   secret.Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	var handlerCalled bool
	handler := NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handlerCalled = true
			w.WriteHeader(http.StatusOK)
		}),
		NewSignedURLAuthenticator(store),
	)

	signer := secret.NewHMAC256(store, "my-secret")
	ctx := t.Context()

	originalURL, _ := url.Parse("https://example.com/path/to/object")
	signedURL, err := signer.Sign(ctx, http.MethodGet, originalURL, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, _ := url.Parse(signedURL)
	req := httptest.NewRequest(http.MethodGet, parsedURL.String(), nil)
	req.URL = parsedURL

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !handlerCalled {
		t.Error("expected handler to be called")
	}

	if rec.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec.Code)
	}
}

func TestNewHandlerNoCredentials(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{},
	}

	handler := NewHandler(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("handler should not be called")
		}),
		NewSignedURLAuthenticator(store),
	)

	req := httptest.NewRequest(http.MethodGet, "https://example.com/path", nil)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", rec.Code)
	}
}

func TestParseExpiration(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Time
	}{
		{"empty", "", time.Time{}},
		{"invalid", "not-a-number", time.Time{}},
		{"valid", "1702569600", time.Unix(1702569600, 0)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseExpiration(tt.input)
			if !result.Equal(tt.expected) {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}
