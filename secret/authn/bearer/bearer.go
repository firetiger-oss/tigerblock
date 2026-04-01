package bearer

import (
	"net/http"

	"github.com/firetiger-oss/storage/secret/authn"
)

// NewTransport returns an http.RoundTripper that injects a static Bearer token
// into every outbound request that lacks an Authorization header.
func NewTransport(base http.RoundTripper, token string) http.RoundTripper {
	return authn.NewBearerAuthTransport(authn.Static(authn.Bearer(token)), "", "*", base)
}
