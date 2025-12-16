package authn

import (
	"cmp"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"

	"github.com/firetiger-oss/storage/secret"
	"golang.org/x/net/publicsuffix"
)

// Scheme defines an HTTP authentication scheme (Bearer, Basic, etc.).
// Implementations handle extracting credentials from requests and injecting
// them into outbound requests.
type Scheme[C any] interface {
	// Extract extracts the identifier and secret from an incoming request.
	// Returns (identifier, secret, true) if auth header present and valid format.
	// Returns ("", "", false) if auth header missing or wrong scheme.
	Extract(req *http.Request) (identifier, secret string, ok bool)

	// Verify compares the extracted secret with the credential's secret.
	Verify(credential C, secret string) bool

	// Inject adds authentication to an outbound request.
	Inject(req *http.Request, credential C)
}

// NewAuthenticator returns an Authenticator using the given scheme.
// C must be loadable via the provided Loader.
// On success, injects credential into context via ContextWithCredential[C].
func NewAuthenticator[C any, S Scheme[C]](loader Loader[C], scheme S) Authenticator {
	return AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
		identifier, s, ok := scheme.Extract(req)
		if !ok {
			return nil, ErrNotFound
		}

		credential, err := loader.Load(ctx, identifier)
		if err != nil {
			if errors.Is(err, secret.ErrNotFound) {
				return nil, ErrUnauthorized
			}
			return nil, err
		}

		if !scheme.Verify(credential, s) {
			return nil, ErrUnauthorized
		}

		domain, _ := publicsuffix.EffectiveTLDPlusOne(hostname(req))
		return ContextWithCredential(ctx, domain, credential), nil
	})
}

// NewAuthForwarder returns an http.RoundTripper that injects credentials
// from the context into outbound requests using the given scheme.
// If the context has no credential or the request already has an Authorization
// header, requests pass through unchanged.
func NewAuthForwarder[C any, S Scheme[C]](transport http.RoundTripper, scheme S) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if !hasAuthorization(req) {
			if domain, cred, ok := CredentialFromContext[C](req.Context()); ok {
				if domainContains(domain, hostname(req)) {
					req = req.Clone(req.Context())
					scheme.Inject(req, cred)
				}
			}
		}
		return transport.RoundTrip(req)
	})
}

// NewAuthTransport returns an http.RoundTripper that loads credentials
// and injects them into outbound requests using the given scheme.
// If the request already has an Authorization header, it passes through unchanged.
func NewAuthTransport[C any, S Scheme[C]](loader Loader[C], secretName, domain string, transport http.RoundTripper, scheme S) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if !hasAuthorization(req) && domainContains(domain, hostname(req)) {
			cred, err := loader.Load(req.Context(), secretName)
			if err != nil {
				return nil, err
			}
			req = req.Clone(req.Context())
			scheme.Inject(req, cred)
		}
		return transport.RoundTrip(req)
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func domainContains(domain, hostname string) bool {
	return domain == "*" ||
		(strings.HasSuffix(hostname, domain) &&
			(len(hostname) == len(domain) || hostname[len(hostname)-len(domain)-1] == '.'))
}

func hostname(req *http.Request) string {
	if req.Host != "" {
		host, _, _ := net.SplitHostPort(req.Host)
		return cmp.Or(host, req.Host)
	}
	return req.URL.Hostname()
}

func hasAuthorization(req *http.Request) bool {
	_, ok := req.Header["Authorization"]
	return ok
}
