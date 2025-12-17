package secret

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// Signing errors.
var (
	// ErrSignatureExpired is returned when the signature has expired.
	ErrSignatureExpired = errors.New("signature expired")
	// ErrSignatureInvalid is returned when the signature does not match.
	ErrSignatureInvalid = errors.New("invalid signature")
	// ErrSignatureMissing is returned when required signature parameters are missing.
	ErrSignatureMissing = errors.New("signature missing")
)

// Signer generates signed URLs using HMAC signatures.
type Signer interface {
	// Sign adds signature parameters to the URL and returns the signed URL string.
	// The expiration time determines when the signed URL becomes invalid.
	Sign(ctx context.Context, method string, u *url.URL, expiration time.Time) (string, error)
}

// NewHMAC256 creates a Signer that uses HMAC-SHA256 for URL signing.
// The provider is used to fetch the secret value on each Sign call.
// The secretID is embedded in the URL as the 's' parameter.
func NewHMAC256(provider Provider, secretID string) Signer {
	return &hmac256Signer{
		provider: provider,
		secretID: secretID,
	}
}

type hmac256Signer struct {
	provider Provider
	secretID string
}

func (s *hmac256Signer) Sign(ctx context.Context, method string, u *url.URL, expiration time.Time) (string, error) {
	value, version, err := s.provider.GetSecretValue(ctx, s.secretID)
	if err != nil {
		return "", err
	}

	expires := expiration.Unix()

	mac := hmac.New(sha256.New, value)
	fmt.Fprintf(mac, "%s\n%s\n%d", method, u.Path, expires)
	signature := mac.Sum(nil)

	signed := *u
	q := signed.Query()
	q.Set("s", s.secretID)
	q.Set("expires", strconv.FormatInt(expires, 10))
	if version != "" {
		q.Set("v", version)
	}
	q.Set("sig", base64.RawURLEncoding.EncodeToString(signature))
	signed.RawQuery = q.Encode()

	return signed.String(), nil
}

// Verify validates the signature on a URL.
// It extracts the 's' (secret ID), 'expires', 'v' (version), and 'sig' parameters
// from the URL query string and verifies the signature.
//
// Returns nil if the signature is valid, or an error:
//   - ErrSignatureMissing: required parameters are missing
//   - ErrSignatureExpired: the signature has expired
//   - ErrSignatureInvalid: the signature does not match
func Verify(ctx context.Context, provider Provider, method string, u *url.URL, now time.Time) error {
	q := u.Query()

	secretID := q.Get("s")
	expiresStr := q.Get("expires")
	sig := q.Get("sig")

	if secretID == "" || expiresStr == "" || sig == "" {
		return ErrSignatureMissing
	}

	expires, err := strconv.ParseInt(expiresStr, 10, 64)
	if err != nil {
		return ErrSignatureMissing
	}

	if now.Unix() > expires {
		return ErrSignatureExpired
	}

	expectedSig, err := base64.RawURLEncoding.DecodeString(sig)
	if err != nil {
		return ErrSignatureInvalid
	}

	value, _, err := provider.GetSecretValue(ctx, secretID, WithVersion(q.Get("v")))
	if err != nil {
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrVersionNotFound) {
			return ErrSignatureInvalid
		}
		return err
	}

	mac := hmac.New(sha256.New, value)
	fmt.Fprintf(mac, "%s\n%s\n%d", method, u.Path, expires)
	computedSig := mac.Sum(nil)

	if subtle.ConstantTimeCompare(expectedSig, computedSig) != 1 {
		return ErrSignatureInvalid
	}

	return nil
}

// HasSignature returns true if the URL contains signature parameters.
func HasSignature(u *url.URL) bool {
	q := u.Query()
	return q.Has("s") && q.Has("expires") && q.Has("sig")
}
