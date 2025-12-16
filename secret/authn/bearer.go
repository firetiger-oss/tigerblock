package authn

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/firetiger-oss/storage/secret"
	"golang.org/x/net/publicsuffix"
)

// BearerCredential provides a token for HTTP Bearer Authentication.
type BearerCredential interface {
	Token() string
}

type bearerAuthDomain struct{}

// NewBearerAuthenticator returns an Authenticator that uses HTTP Bearer Authentication.
// C must implement BearerCredential and be loadable via the provided Loader.
// Uses tokenID as the credential identifier to load the expected token.
// Injects credential into context via ContextWithCredential[C].
func NewBearerAuthenticator[C BearerCredential](loader Loader[C], tokenID string) Authenticator {
	return AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
		token, ok := bearerToken(req)
		if !ok {
			return nil, ErrNotFound
		}

		credential, err := loader.Load(ctx, tokenID)
		if err != nil {
			if errors.Is(err, secret.ErrNotFound) {
				return nil, ErrUnauthorized
			}
			return nil, err
		}
		if subtle.ConstantTimeCompare([]byte(credential.Token()), []byte(token)) != 1 {
			return nil, ErrUnauthorized
		}

		domain, _ := publicsuffix.EffectiveTLDPlusOne(hostname(req))
		ctx = context.WithValue(ctx, bearerAuthDomain{}, domain)
		ctx = ContextWithCredential(ctx, credential)
		return ctx, nil
	})
}

// NewBearerAuthForwarder returns an http.RoundTripper that injects Bearer
// credential from the context into outbound requests. If the context has no
// credential or the request already has an Authorization header, requests pass
// through unchanged.
func NewBearerAuthForwarder(t http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if !hasAuthorization(req) {
			if cred, ok := CredentialFromContext[BearerCredential](req.Context()); ok {
				if domainContains(bearerDomainFromContext(req.Context()), hostname(req)) {
					req = req.Clone(req.Context())
					req.Header.Set("Authorization", "Bearer "+cred.Token())
				}
			}
		}
		return t.RoundTrip(req)
	})
}

// NewBearerAuthTransport returns an http.RoundTripper that loads Bearer
// credential and injects it into outbound requests. If the request already has
// an Authorization header, it passes through unchanged.
// The credential is loaded on each request using the provided secret name.
func NewBearerAuthTransport[Credential BearerCredential](loader Loader[Credential], secretName, domain string, transport http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if !hasAuthorization(req) && domainContains(domain, hostname(req)) {
			cred, err := loader.Load(req.Context(), secretName)
			if err != nil {
				return nil, err
			}
			req = req.Clone(req.Context())
			req.Header.Set("Authorization", "Bearer "+cred.Token())
		}
		return transport.RoundTrip(req)
	})
}

func bearerDomainFromContext(ctx context.Context) string {
	domain, _ := ctx.Value(bearerAuthDomain{}).(string)
	return domain
}

func bearerToken(req *http.Request) (string, bool) {
	auth := req.Header.Get("Authorization")
	if auth == "" {
		return "", false
	}
	token, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok {
		return "", false
	}
	return token, true
}
