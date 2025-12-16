package authn

import (
	"cmp"
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"strings"

	"github.com/firetiger-oss/storage/secret"
	"golang.org/x/net/publicsuffix"
)

// BasicAuthCredential provides username and password for HTTP Basic Auth.
type BasicAuthCredential interface {
	Username() string
	Password() string
}

type basicAuthDomain struct{}

// NewBasicAuthenticator returns an Authenticator that uses HTTP Basic Authentication.
// C must implement BasicAuthCredential and be loadable via the provided Loader.
// Uses the username from Basic Auth as the credential identifier.
// Injects credential into context via ContextWithCredential[C].
func NewBasicAuthenticator[C BasicAuthCredential](loader Loader[C]) Authenticator {
	return AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
		username, password, ok := req.BasicAuth()
		if !ok {
			return nil, ErrNotFound
		}

		credential, err := loader.Load(ctx, username)
		if err != nil {
			if errors.Is(err, secret.ErrNotFound) {
				return nil, ErrUnauthorized
			}
			return nil, err
		}
		if subtle.ConstantTimeCompare([]byte(credential.Password()), []byte(password)) != 1 {
			return nil, ErrUnauthorized
		}

		domain, _ := publicsuffix.EffectiveTLDPlusOne(hostname(req))
		ctx = context.WithValue(ctx, basicAuthDomain{}, domain)
		ctx = ContextWithCredential(ctx, credential)
		return ctx, nil
	})
}

// NewBasicAuthForwarder returns an http.RoundTripper that injects Basic Auth
// credential from the context into outbound requests. If the context has no
// credential, requests pass through unchanged.
func NewBasicAuthForwarder(t http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if cred, ok := CredentialFromContext[BasicAuthCredential](req.Context()); ok {
			if domainContains(domainFromContext(req.Context()), hostname(req)) {
				req = req.Clone(req.Context())
				req.SetBasicAuth(cred.Username(), cred.Password())
			}
		}
		return t.RoundTrip(req)
	})
}

// NewBasicAuthTransport returns an http.RoundTripper that loads Basic Auth
// credential and injects it into outbound requests.
// The credential is loaded on each request using the provided secret name.
func NewBasicAuthTransport[Credential BasicAuthCredential](loader Loader[Credential], secretName, domain string, transport http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if domainContains(domain, hostname(req)) {
			cred, err := loader.Load(req.Context(), secretName)
			if err != nil {
				return nil, err
			}
			req = req.Clone(req.Context())
			req.SetBasicAuth(cred.Username(), cred.Password())
		}
		return transport.RoundTrip(req)
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func domainFromContext(ctx context.Context) string {
	domain, _ := ctx.Value(basicAuthDomain{}).(string)
	return domain
}

func domainContains(domain, hostname string) bool {
	return strings.HasSuffix(hostname, domain) &&
		(len(hostname) == len(domain) || hostname[len(hostname)-len(domain)-1] == '.')
}

func hostname(req *http.Request) string {
	if req.Host != "" {
		host, _, _ := net.SplitHostPort(req.Host)
		return cmp.Or(host, req.Host)
	}
	return req.URL.Hostname()
}
