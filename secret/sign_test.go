package secret

import (
	"context"
	"net/http"
	"net/url"
	"testing"
	"time"
)

type mockStore struct {
	secrets map[string]mockSecret
}

type mockSecret struct {
	value   Value
	version string
}

func (m *mockStore) GetSecretValue(ctx context.Context, name string, options ...GetOption) (Value, string, error) {
	s, ok := m.secrets[name]
	if !ok {
		return nil, "", ErrNotFound
	}
	opts := NewGetOptions(options...)
	if v := opts.Version(); v != "" && v != s.version {
		return nil, "", ErrNotFound
	}
	return s.value, s.version, nil
}

func TestSignAndVerify(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := NewHMAC256(store, "my-secret")
	ctx := t.Context()

	u, err := url.Parse("https://example.com/path/to/object")
	if err != nil {
		t.Fatal(err)
	}

	signedURL, err := signer.Sign(ctx, http.MethodGet, u, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the signed URL
	err = Verify(ctx, store, http.MethodGet, parsedURL, time.Now())
	if err != nil {
		t.Errorf("expected valid signature, got error: %v", err)
	}
}

func TestSignAndVerifyWithVersion(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   Value("test-secret-key"),
				version: "v2",
			},
		},
	}

	signer := NewHMAC256(store, "my-secret")
	ctx := t.Context()

	u, err := url.Parse("https://example.com/path/to/object")
	if err != nil {
		t.Fatal(err)
	}

	signedURL, err := signer.Sign(ctx, http.MethodGet, u, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		t.Fatal(err)
	}

	// Check that version is in the URL
	if v := parsedURL.Query().Get("v"); v != "v2" {
		t.Errorf("expected version v2, got %s", v)
	}

	// Verify the signed URL
	err = Verify(ctx, store, http.MethodGet, parsedURL, time.Now())
	if err != nil {
		t.Errorf("expected valid signature, got error: %v", err)
	}
}

func TestVerifyExpiredSignature(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := NewHMAC256(store, "my-secret")
	ctx := t.Context()

	u, err := url.Parse("https://example.com/path/to/object")
	if err != nil {
		t.Fatal(err)
	}

	// Sign with negative expiration (already expired)
	signedURL, err := signer.Sign(ctx, http.MethodGet, u, time.Now().Add(-1*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		t.Fatal(err)
	}

	// Verify should fail with expired error
	err = Verify(ctx, store, http.MethodGet, parsedURL, time.Now())
	if err != ErrSignatureExpired {
		t.Errorf("expected ErrSignatureExpired, got: %v", err)
	}
}

func TestVerifyInvalidSignature(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := NewHMAC256(store, "my-secret")
	ctx := t.Context()

	u, err := url.Parse("https://example.com/path/to/object")
	if err != nil {
		t.Fatal(err)
	}

	signedURL, err := signer.Sign(ctx, http.MethodGet, u, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		t.Fatal(err)
	}

	// Tamper with the signature
	q := parsedURL.Query()
	q.Set("sig", "tampered_signature")
	parsedURL.RawQuery = q.Encode()

	// Verify should fail with invalid signature error
	err = Verify(ctx, store, http.MethodGet, parsedURL, time.Now())
	if err != ErrSignatureInvalid {
		t.Errorf("expected ErrSignatureInvalid, got: %v", err)
	}
}

func TestVerifyMissingParameters(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	ctx := t.Context()

	tests := []struct {
		name string
		url  string
	}{
		{"missing all params", "https://example.com/path"},
		{"missing sig", "https://example.com/path?s=my-secret&expires=9999999999"},
		{"missing expires", "https://example.com/path?s=my-secret&sig=abc"},
		{"missing s", "https://example.com/path?expires=9999999999&sig=abc"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			if err != nil {
				t.Fatal(err)
			}

			err = Verify(ctx, store, http.MethodGet, u, time.Now())
			if err != ErrSignatureMissing {
				t.Errorf("expected ErrSignatureMissing, got: %v", err)
			}
		})
	}
}

func TestVerifyWrongMethod(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := NewHMAC256(store, "my-secret")
	ctx := t.Context()

	u, err := url.Parse("https://example.com/path/to/object")
	if err != nil {
		t.Fatal(err)
	}

	// Sign for GET
	signedURL, err := signer.Sign(ctx, http.MethodGet, u, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		t.Fatal(err)
	}

	// Verify with POST should fail
	err = Verify(ctx, store, http.MethodPost, parsedURL, time.Now())
	if err != ErrSignatureInvalid {
		t.Errorf("expected ErrSignatureInvalid for wrong method, got: %v", err)
	}
}

func TestVerifyVersionMismatch(t *testing.T) {
	// Start with version v1
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   Value("test-secret-key-v1"),
				version: "v1",
			},
		},
	}

	signer := NewHMAC256(store, "my-secret")
	ctx := t.Context()

	u, err := url.Parse("https://example.com/path/to/object")
	if err != nil {
		t.Fatal(err)
	}

	// Sign with v1
	signedURL, err := signer.Sign(ctx, http.MethodGet, u, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		t.Fatal(err)
	}

	// Update store to v2 with different key
	store.secrets["my-secret"] = mockSecret{
		value:   Value("test-secret-key-v2"),
		version: "v2",
	}

	// Verification should fail because v1 no longer exists
	err = Verify(ctx, store, http.MethodGet, parsedURL, time.Now())
	if err != ErrSignatureInvalid {
		t.Errorf("expected ErrSignatureInvalid for version mismatch, got: %v", err)
	}
}

func TestVerifyUnknownSecret(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{},
	}

	ctx := t.Context()

	u, err := url.Parse("https://example.com/path?s=unknown&expires=9999999999&sig=abc123")
	if err != nil {
		t.Fatal(err)
	}

	err = Verify(ctx, store, http.MethodGet, u, time.Now())
	if err != ErrSignatureInvalid {
		t.Errorf("expected ErrSignatureInvalid for unknown secret, got: %v", err)
	}
}

func TestHasSignature(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		expected bool
	}{
		{"no params", "https://example.com/path", false},
		{"partial params", "https://example.com/path?s=id&expires=123", false},
		{"all params", "https://example.com/path?s=id&expires=123&sig=abc", true},
		{"with version", "https://example.com/path?s=id&expires=123&v=v1&sig=abc", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			u, err := url.Parse(tt.url)
			if err != nil {
				t.Fatal(err)
			}

			if got := HasSignature(u); got != tt.expected {
				t.Errorf("HasSignature() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestSignPreservesExistingQueryParams(t *testing.T) {
	store := &mockStore{
		secrets: map[string]mockSecret{
			"my-secret": {
				value:   Value("test-secret-key"),
				version: "v1",
			},
		},
	}

	signer := NewHMAC256(store, "my-secret")
	ctx := t.Context()

	u, err := url.Parse("https://example.com/path?existing=param")
	if err != nil {
		t.Fatal(err)
	}

	signedURL, err := signer.Sign(ctx, http.MethodGet, u, time.Now().Add(15*time.Minute))
	if err != nil {
		t.Fatal(err)
	}

	parsedURL, err := url.Parse(signedURL)
	if err != nil {
		t.Fatal(err)
	}

	// Check existing param is preserved
	if got := parsedURL.Query().Get("existing"); got != "param" {
		t.Errorf("existing param not preserved, got: %s", got)
	}

	// Check signing params are present
	if !parsedURL.Query().Has("s") || !parsedURL.Query().Has("expires") || !parsedURL.Query().Has("sig") {
		t.Error("signing params missing")
	}
}
