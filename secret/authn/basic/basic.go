package basic

import (
	"net/http"

	"github.com/firetiger-oss/storage/secret/authn"
)

// NewTransport returns an http.RoundTripper that injects static Basic Auth
// credentials into every outbound request that lacks an Authorization header.
func NewTransport(base http.RoundTripper, username, password string) http.RoundTripper {
	return authn.NewBasicAuthTransport(authn.Static(authn.Basic{username, password}), "", "*", base)
}
