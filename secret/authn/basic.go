package authn

import (
	"crypto/subtle"
	"net/http"
)

// BasicAuthCredential provides username and password for HTTP Basic Auth.
type BasicAuthCredential interface {
	Username() string
	Password() string
}

// Basic is a static basic auth credential that implements BasicAuthCredential.
// Index 0 is the username, index 1 is the password.
type Basic [2]string

func (c Basic) Username() string { return c[0] }
func (c Basic) Password() string { return c[1] }

// BasicScheme implements Scheme for HTTP Basic authentication.
type BasicScheme[C BasicAuthCredential] struct{}

// NewBasicScheme creates a BasicScheme.
func NewBasicScheme[C BasicAuthCredential]() BasicScheme[C] {
	return BasicScheme[C]{}
}

// Extract extracts username and password from the Basic Auth header.
// Returns (username, password, true) if valid Basic Auth is present.
func (s BasicScheme[C]) Extract(req *http.Request) (identifier, secret string, ok bool) {
	username, password, ok := req.BasicAuth()
	if !ok {
		return "", "", false
	}
	return username, password, true
}

// Verify compares the extracted password with the credential's password.
func (s BasicScheme[C]) Verify(credential C, secret string) bool {
	return subtle.ConstantTimeCompare([]byte(credential.Password()), []byte(secret)) == 1
}

// Inject sets Basic Auth on the request.
func (s BasicScheme[C]) Inject(req *http.Request, credential C) {
	req.SetBasicAuth(credential.Username(), credential.Password())
}

// NewBasicAuthenticator returns an Authenticator that uses HTTP Basic Authentication.
// C must implement BasicAuthCredential and be loadable via the provided Loader.
// Uses the username from Basic Auth as the credential identifier.
// Injects credential into context via ContextWithCredential[C].
func NewBasicAuthenticator[C BasicAuthCredential](loader Loader[C]) Authenticator {
	return NewAuthenticator(loader, NewBasicScheme[C]())
}

// NewBasicAuthForwarder returns an http.RoundTripper that injects Basic Auth
// credential from the context into outbound requests. If the context has no
// credential or the request already has an Authorization header, requests pass
// through unchanged.
func NewBasicAuthForwarder(t http.RoundTripper) http.RoundTripper {
	return NewAuthForwarder[BasicAuthCredential](t, NewBasicScheme[BasicAuthCredential]())
}

// NewBasicAuthTransport returns an http.RoundTripper that loads Basic Auth
// credential and injects it into outbound requests. If the request already has
// an Authorization header, it passes through unchanged.
// Credentials are cached and refreshed on 401 responses.
func NewBasicAuthTransport[Credential BasicAuthCredential](loader Loader[Credential], secretName, domain string, transport http.RoundTripper) http.RoundTripper {
	return NewAuthTransport(loader, secretName, domain, transport, NewBasicScheme[Credential]())
}
