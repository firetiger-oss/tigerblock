// Package authn provides HTTP authentication using secrets.
package authn

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"strings"

	"github.com/firetiger-oss/storage/secret"
)

// ErrUnauthorized is returned when authentication fails.
var ErrUnauthorized = errors.New("unauthorized")

// ErrNotFound indicates no credentials were found in the request.
// When returned by an authenticator, NewHandler tries the next authenticator.
var ErrNotFound = errors.New("credentials not found")

// Authenticator provides an interface for HTTP request authentication.
// Authenticate returns a context with credential injected (via ContextWithCredential).
// This design allows composing multiple authentication schemes.
type Authenticator interface {
	Authenticate(ctx context.Context, req *http.Request) (context.Context, error)

	// Challenge returns the WWW-Authenticate challenge this authenticator
	// contributes when a request is rejected with 401. Return the zero
	// value to contribute no challenge (e.g. signed-URL authentication).
	Challenge(req *http.Request) Challenge
}

// AuthenticatorFunc is a function adapter for Authenticator.
type AuthenticatorFunc func(ctx context.Context, req *http.Request) (context.Context, error)

// Authenticate implements Authenticator.
func (f AuthenticatorFunc) Authenticate(ctx context.Context, req *http.Request) (context.Context, error) {
	return f(ctx, req)
}

// Challenge implements Authenticator by contributing no challenge.
// Callers that need to contribute a WWW-Authenticate challenge should build
// an authenticator through NewAuthenticator with a Scheme implementation.
func (f AuthenticatorFunc) Challenge(*http.Request) Challenge { return Challenge{} }

// Challenge is a single WWW-Authenticate challenge entry (RFC 7235).
//
// Only auth-scheme plus quoted-string auth-params is supported; token68
// (Negotiate/NTLM) and token-valued params (e.g. Digest's algorithm, stale)
// are out of scope.
type Challenge struct {
	// Scheme is the authentication scheme, e.g. "Basic" or "Bearer".
	// RFC 7235 treats schemes case-insensitively; the value is transmitted
	// as-is.
	Scheme string

	// Params are the auth-params emitted as quoted-strings, e.g.
	// {"realm": "example.com", "scope": "read"}. Keys are sorted
	// alphabetically on serialization.
	Params map[string]string
}

// IsZero reports whether c is the zero value. Zero challenges are omitted
// when NewHandler composes the WWW-Authenticate header.
func (c Challenge) IsZero() bool { return c.Scheme == "" }

// String returns the RFC 7235 wire representation of the challenge.
// Param values are serialized as quoted-strings with `\` and `"` escaped.
func (c Challenge) String() string {
	if c.IsZero() {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(c.Scheme)
	for i, k := range slices.Sorted(maps.Keys(c.Params)) {
		if i == 0 {
			sb.WriteByte(' ')
		} else {
			sb.WriteString(", ")
		}
		sb.WriteString(k)
		sb.WriteString(`="`)
		writeQuoted(&sb, c.Params[k])
		sb.WriteByte('"')
	}
	return sb.String()
}

func writeQuoted(sb *strings.Builder, v string) {
	for i := 0; i < len(v); i++ {
		if v[i] == '\\' || v[i] == '"' {
			sb.WriteByte('\\')
		}
		sb.WriteByte(v[i])
	}
}

// Loader loads credentials by identifier.
type Loader[C any] interface {
	Load(ctx context.Context, id string) (C, error)
}

// LoaderFunc adapts a function to the Loader interface.
func LoaderFunc[C any](f func(ctx context.Context, id string) (C, error)) Loader[C] {
	return loaderFunc[C](f)
}

type loaderFunc[C any] func(ctx context.Context, id string) (C, error)

func (f loaderFunc[C]) Load(ctx context.Context, id string) (C, error) { return f(ctx, id) }

// Static returns a Loader that always returns the given credential value.
func Static[C any](c C) Loader[C] {
	return LoaderFunc[C](func(_ context.Context, _ string) (C, error) { return c, nil })
}

// NewLoader returns a Loader that loads credentials from a secret.Provider.
func NewLoader[C any](provider secret.Provider) Loader[C] {
	return LoaderFunc[C](func(ctx context.Context, id string) (C, error) {
		value, _, err := provider.GetSecretValue(ctx, id)
		if err != nil {
			var zero C
			return zero, err
		}
		return Unmarshal[C](value)
	})
}

type credentialsContextKey struct{}

type credentialContextValue interface {
	load() (domain string, credential any)
}

type credentialValue[Credential any] struct {
	domain     string
	credential Credential
}

func (cv *credentialValue[Credential]) load() (string, any) {
	return cv.domain, cv.credential
}

// CredentialFromContext retrieves a credential and its domain from the context.
// Returns the domain, credential, and true if present.
func CredentialFromContext[Credential any](ctx context.Context) (domain string, credential Credential, ok bool) {
	switch cv := ctx.Value(credentialsContextKey{}).(type) {
	case *credentialValue[Credential]: // fast path when types match
		return cv.domain, cv.credential, true
	case credentialContextValue:
		domain, value := cv.load()
		credential, ok = value.(Credential)
		return domain, credential, ok
	default:
		return
	}
}

// ContextWithCredential returns a new context with a credential and its domain.
func ContextWithCredential[Credential any](ctx context.Context, domain string, credential Credential) context.Context {
	return context.WithValue(ctx, credentialsContextKey{}, &credentialValue[Credential]{
		domain:     domain,
		credential: credential,
	})
}

// NewHandler creates an HTTP handler that authenticates requests.
// It tries each authenticator in order until one succeeds.
// If an authenticator returns ErrNotFound, it tries the next one.
// On success, calls next handler with the context returned by Authenticate.
// On failure (all authenticators fail), responds with 401 Unauthorized.
func NewHandler(next http.Handler, authenticators ...Authenticator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		for _, auth := range authenticators {
			newCtx, err := auth.Authenticate(ctx, r)
			if err == nil {
				next.ServeHTTP(w, r.WithContext(newCtx))
				return
			}
			if !errors.Is(err, ErrNotFound) {
				break
			}
		}
		writeUnauthorizedError(w, r, authenticators)
	})
}

func writeUnauthorizedError(w http.ResponseWriter, r *http.Request, authenticators []Authenticator) {
	var parts []string
	for _, auth := range authenticators {
		if c := auth.Challenge(r); !c.IsZero() {
			parts = append(parts, c.String())
		}
	}
	if len(parts) > 0 {
		w.Header().Set("WWW-Authenticate", strings.Join(parts, ", "))
	}
	if isConnectRPC(r) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		io.WriteString(w, `{"code":"unauthenticated","message":"Request unauthenticated: missing or invalid credentials"}`)
		return
	}
	if isS3Request(r) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprintf(w, `<Error><Code>AccessDenied</Code><Message>Request unauthenticated: missing or invalid credentials</Message><Resource>%s</Resource></Error>`, r.URL.Path)
		return
	}
	http.Error(w, "Unauthorized", http.StatusUnauthorized)
}

func isConnectRPC(r *http.Request) bool {
	if r.Header.Get("Connect-Protocol-Version") != "" {
		return true
	}
	contentType := r.Header.Get("Content-Type")
	return strings.HasPrefix(contentType, "application/connect+") ||
		strings.HasPrefix(contentType, "application/grpc")
}

func isS3Request(r *http.Request) bool {
	for key := range r.Header {
		if strings.HasPrefix(key, "X-Amz-") {
			return true
		}
	}
	auth := r.Header.Get("Authorization")
	return strings.HasPrefix(auth, "AWS4-HMAC-SHA256") || strings.HasPrefix(auth, "AWS ")
}

// Unmarshal decodes a secret value into a credential.
// It supports types that implement encoding.TextUnmarshaler, encoding.BinaryUnmarshaler,
// string-based types, []byte, or JSON-deserializable structs.
func Unmarshal[Credential any](value secret.Value) (Credential, error) {
	var credential Credential
	switch c := any(&credential).(type) {
	case encoding.TextUnmarshaler:
		if err := c.UnmarshalText(value); err != nil {
			return credential, err
		}
	case encoding.BinaryUnmarshaler:
		if err := c.UnmarshalBinary(value); err != nil {
			return credential, err
		}
	default:
		switch v := reflect.ValueOf(c).Elem(); v.Kind() {
		case reflect.String:
			v.SetString(string(value))
		case reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				v.SetBytes(value)
			} else {
				if err := json.Unmarshal(value, c); err != nil {
					return credential, err
				}
			}
		default:
			if err := json.Unmarshal(value, c); err != nil {
				return credential, err
			}
		}
	}
	return credential, nil
}

// Marshal encodes a credential into a secret value.
// It supports types that implement encoding.TextMarshaler, encoding.BinaryMarshaler,
// string-based types, []byte, or JSON-serializable structs.
func Marshal[Credential any](creds Credential) (secret.Value, error) {
	switch c := any(creds).(type) {
	case encoding.TextMarshaler:
		return c.MarshalText()
	case encoding.BinaryMarshaler:
		return c.MarshalBinary()
	default:
		switch v := reflect.ValueOf(creds); v.Kind() {
		case reflect.String:
			return secret.Value(v.String()), nil
		case reflect.Slice:
			if v.Type().Elem().Kind() == reflect.Uint8 {
				return secret.Value(v.Bytes()), nil
			}
		}
		return json.Marshal(creds)
	}
}
