package authn

import (
	"crypto/subtle"
	"net/http"
	"strings"
)

// BearerCredential provides a token for HTTP Bearer Authentication.
type BearerCredential interface {
	Token() string
}

// Bearer is a static bearer token that implements BearerCredential.
type Bearer string

func (t Bearer) Token() string { return string(t) }

// BearerScheme implements Scheme for Bearer token authentication.
type BearerScheme[C BearerCredential] struct {
	tokenID string
}

// NewBearerScheme creates a BearerScheme with the given token identifier.
// The tokenID is used to load the credential for verification.
func NewBearerScheme[C BearerCredential](tokenID string) BearerScheme[C] {
	return BearerScheme[C]{tokenID: tokenID}
}

// Extract extracts the token from the Authorization header.
// Returns (tokenID, token, true) if a valid Bearer token is present.
func (s BearerScheme[C]) Extract(req *http.Request) (identifier, secret string, ok bool) {
	token, ok := bearerToken(req)
	if !ok {
		return "", "", false
	}
	return s.tokenID, token, true
}

// Verify compares the extracted token with the credential's token.
func (s BearerScheme[C]) Verify(credential C, secret string) bool {
	return subtle.ConstantTimeCompare([]byte(credential.Token()), []byte(secret)) == 1
}

// Inject sets the Bearer token in the Authorization header.
func (s BearerScheme[C]) Inject(req *http.Request, credential C) {
	req.Header.Set("Authorization", "Bearer "+credential.Token())
}

// Challenge returns a Bearer WWW-Authenticate challenge with realm defaulted
// to the request's Host.
func (s BearerScheme[C]) Challenge(req *http.Request) Challenge {
	return Challenge{
		Scheme: "Bearer",
		Params: map[string]string{"realm": req.Host},
	}
}

// NewBearerAuthenticator returns an Authenticator that uses HTTP Bearer Authentication.
// C must implement BearerCredential and be loadable via the provided Loader.
// Uses tokenID as the credential identifier to load the expected token.
// Injects credential into context via ContextWithCredential[C].
func NewBearerAuthenticator[C BearerCredential](loader Loader[C], tokenID string) Authenticator {
	return NewAuthenticator(loader, NewBearerScheme[C](tokenID))
}

// NewBearerAuthForwarder returns an http.RoundTripper that injects Bearer
// credential from the context into outbound requests. If the context has no
// credential or the request already has an Authorization header, requests pass
// through unchanged.
func NewBearerAuthForwarder(t http.RoundTripper) http.RoundTripper {
	return NewAuthForwarder[BearerCredential](t, NewBearerScheme[BearerCredential](""))
}

// NewBearerAuthTransport returns an http.RoundTripper that loads Bearer
// credential and injects it into outbound requests. If the request already has
// an Authorization header, it passes through unchanged.
// Credentials are cached and refreshed on 401 responses.
func NewBearerAuthTransport[Credential BearerCredential](loader Loader[Credential], secretName, domain string, transport http.RoundTripper) http.RoundTripper {
	return NewAuthTransport(loader, secretName, domain, transport, NewBearerScheme[Credential](""))
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
