package authn

import (
	"cmp"
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync/atomic"

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

	// Challenge returns the WWW-Authenticate challenge for this scheme,
	// used when a request is rejected with 401.
	Challenge(req *http.Request) Challenge
}

// NewAuthenticator returns an Authenticator using the given scheme.
// C must be loadable via the provided Loader.
// On success, injects credential into context via ContextWithCredential[C].
func NewAuthenticator[C any, S Scheme[C]](loader Loader[C], scheme S) Authenticator {
	return &schemeAuthenticator[C, S]{loader: loader, scheme: scheme}
}

type schemeAuthenticator[C any, S Scheme[C]] struct {
	loader Loader[C]
	scheme S
}

func (a *schemeAuthenticator[C, S]) Authenticate(ctx context.Context, req *http.Request) (context.Context, error) {
	identifier, s, ok := a.scheme.Extract(req)
	if !ok {
		return nil, ErrNotFound
	}

	credential, err := a.loader.Load(ctx, identifier)
	if err != nil {
		if errors.Is(err, secret.ErrNotFound) {
			return nil, ErrUnauthorized
		}
		return nil, err
	}

	if !a.scheme.Verify(credential, s) {
		return nil, ErrUnauthorized
	}

	domain, _ := publicsuffix.EffectiveTLDPlusOne(hostname(req))
	return ContextWithCredential(ctx, domain, credential), nil
}

func (a *schemeAuthenticator[C, S]) Challenge(req *http.Request) Challenge {
	return a.scheme.Challenge(req)
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
//
// Credentials are cached and reused across requests. On 401 responses,
// the transport reloads the credential and retries the request once.
func NewAuthTransport[C any, S Scheme[C]](loader Loader[C], secretName, domain string, transport http.RoundTripper, scheme S) http.RoundTripper {
	return &cachingTransport[C, S]{
		loader:     loader,
		secretName: secretName,
		domain:     domain,
		transport:  transport,
		scheme:     scheme,
		lock:       make(chan struct{}, 1),
	}
}

type cachingTransport[C any, S Scheme[C]] struct {
	loader     Loader[C]
	secretName string
	domain     string
	transport  http.RoundTripper
	scheme     S

	lock   chan struct{}
	cached atomic.Pointer[C]
}

func (t *cachingTransport[C, S]) RoundTrip(req *http.Request) (*http.Response, error) {
	if hasAuthorization(req) || !domainContains(t.domain, hostname(req)) {
		return t.transport.RoundTrip(req)
	}

	cred, err := t.credential(req.Context())
	if err != nil {
		return nil, err
	}

	req = req.Clone(req.Context())
	t.scheme.Inject(req, *cred)

	resp, err := t.transport.RoundTrip(req)
	if err != nil || resp.StatusCode != http.StatusUnauthorized {
		return resp, err
	}

	newCred, err := t.load(req.Context())
	if err != nil {
		return resp, nil
	}

	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			return resp, nil
		}
		req.Body = body
	}

	resp.Body.Close()
	t.scheme.Inject(req, *newCred)
	return t.transport.RoundTrip(req)
}

func (t *cachingTransport[C, S]) credential(ctx context.Context) (*C, error) {
	if p := t.cached.Load(); p != nil {
		return p, nil
	}
	return t.load(ctx)
}

func (t *cachingTransport[C, S]) load(ctx context.Context) (*C, error) {
	select {
	case t.lock <- struct{}{}:
		defer func() { <-t.lock }()
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}

	cred, err := t.loader.Load(ctx, t.secretName)
	if err != nil {
		return nil, err
	}

	t.cached.Store(&cred)
	return &cred, nil
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
