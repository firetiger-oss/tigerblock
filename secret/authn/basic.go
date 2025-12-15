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

// BasicAuthCredentials provides username and password for HTTP Basic Auth.
type BasicAuthCredentials interface {
	Username() string
	Password() string
}

type basicAuthDomain struct{}

// NewBasicAuthenticator returns an Authenticator that uses HTTP Basic Authentication.
// C must implement BasicAuthCredentials and be deserializable via Unmarshal.
// Uses the username from Basic Auth as the secret name.
// Injects credentials into context via ContextWithCredentials[C].
func NewBasicAuthenticator[C BasicAuthCredentials](store secret.Store) Authenticator {
	return AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
		username, password, ok := req.BasicAuth()
		if !ok {
			return nil, ErrNotFound
		}

		value, _, err := store.GetSecret(ctx, username)
		if err != nil {
			if errors.Is(err, secret.ErrNotFound) {
				return nil, ErrUnauthorized
			}
			return nil, err
		}

		credentials, err := Unmarshal[C](value)
		if err != nil {
			return nil, err
		}
		if subtle.ConstantTimeCompare([]byte(credentials.Password()), []byte(password)) != 1 {
			return nil, ErrUnauthorized
		}

		domain, _ := publicsuffix.EffectiveTLDPlusOne(hostname(req))
		ctx = context.WithValue(ctx, basicAuthDomain{}, domain)
		ctx = ContextWithCredentials(ctx, credentials)
		return ctx, nil
	})
}

// NewBasicAuthForwarder returns an http.RoundTripper that injects Basic Auth
// credentials from the context into outbound requests. If the context has no
// credentials, requests pass through unchanged.
func NewBasicAuthForwarder(t http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if creds, ok := CredentialsFromContext[BasicAuthCredentials](req.Context()); ok {
			if domainContains(domainFromContext(req.Context()), hostname(req)) {
				req = req.Clone(req.Context())
				req.SetBasicAuth(creds.Username(), creds.Password())
			}
		}
		return t.RoundTrip(req)
	})
}

// NewBasicAuthTransport returns an http.RoundTripper that loads Basic Auth
// credentials from a secret store and injects them into outbound requests.
// The credentials are loaded on each request using the provided secret name.
func NewBasicAuthTransport[Credentials BasicAuthCredentials](store secret.Store, secretName, domain string, transport http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if domainContains(domain, hostname(req)) {
			value, _, err := store.GetSecret(req.Context(), secretName)
			if err != nil {
				return nil, err
			}
			creds, err := Unmarshal[Credentials](value)
			if err != nil {
				return nil, err
			}
			req = req.Clone(req.Context())
			req.SetBasicAuth(creds.Username(), creds.Password())
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
