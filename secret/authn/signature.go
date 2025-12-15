package authn

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/firetiger-oss/storage/secret"
)

// SignedURLCredentials contains information extracted from a signed URL.
type SignedURLCredentials struct {
	Method     string
	Path       string
	Expiration time.Time
}

// NewSignedURLAuthenticator returns an Authenticator that validates URL signatures.
// It uses secret.Verify to validate the signature and extracts credentials from the URL.
// On success, injects SignedURLCredentials into context via ContextWithCredentials.
// Returns ErrNotFound if the URL has no signature parameters.
func NewSignedURLAuthenticator(store secret.Store) Authenticator {
	return AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
		if !secret.HasSignature(req.URL) {
			return nil, ErrNotFound
		}
		if err := secret.Verify(ctx, store, req.Method, req.URL, time.Now()); err != nil {
			return nil, err
		}
		credentials := SignedURLCredentials{
			Method:     req.Method,
			Path:       req.URL.Path,
			Expiration: parseExpiration(req.URL.Query().Get("expires")),
		}
		return ContextWithCredentials(ctx, credentials), nil
	})
}

func parseExpiration(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	unix, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(unix, 0)
}
