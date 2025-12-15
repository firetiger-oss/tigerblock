// Package authn provides HTTP authentication using secrets.
package authn

import (
	"context"
	"crypto/subtle"
	"encoding"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"

	"github.com/firetiger-oss/storage/secret"
)

// ErrUnauthorized is returned when authentication fails.
var ErrUnauthorized = errors.New("unauthorized")

// ErrNotFound indicates no credentials were found in the request.
// When returned by an authenticator, NewHandler tries the next authenticator.
var ErrNotFound = errors.New("credentials not found")

// Authenticator provides an interface for HTTP request authentication.
// Authenticate returns a context with credentials injected (via ContextWithCredentials).
// This design allows composing multiple authentication schemes.
type Authenticator interface {
	Authenticate(ctx context.Context, req *http.Request) (context.Context, error)
}

// AuthenticatorFunc is a function adapter for Authenticator.
type AuthenticatorFunc func(ctx context.Context, req *http.Request) (context.Context, error)

// Authenticate implements Authenticator.
func (f AuthenticatorFunc) Authenticate(ctx context.Context, req *http.Request) (context.Context, error) {
	return f(ctx, req)
}

// credentialsContextKey is a generic context key type for credentials.
type credentialsContextKey struct{}

// CredentialsFromContext retrieves credentials from the context.
// Returns the credentials and true if present, zero value and false otherwise.
//
// To load any credentials from the context and perform type assertion later,
// use ContextWithCredentials[any].
func CredentialsFromContext[Credentials any](ctx context.Context) (Credentials, bool) {
	creds, ok := ctx.Value(credentialsContextKey{}).(Credentials)
	return creds, ok
}

// ContextWithCredentials returns a new context with credentials.
func ContextWithCredentials[Credentials any](ctx context.Context, creds Credentials) context.Context {
	return context.WithValue(ctx, credentialsContextKey{}, creds)
}

// NewHandler creates an HTTP handler that authenticates requests.
// It tries each authenticator in order until one succeeds.
// If an authenticator returns ErrNotFound, it tries the next one.
// On success, calls next handler with the context returned by Authenticate.
// On failure (all authenticators fail), responds with 401 Unauthorized.
func NewHandler(next http.Handler, authenticators ...Authenticator) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		var err error
		for _, auth := range authenticators {
			ctx, err = auth.Authenticate(ctx, r)
			if err == nil {
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			if !errors.Is(err, ErrNotFound) {
				break
			}
		}
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}

// BasicAuthCredentials provides username and password for HTTP Basic Auth.
type BasicAuthCredentials interface {
	Username() string
	Password() string
}

// NewBasicAuthenticator returns an Authenticator that uses HTTP Basic Authentication.
// C must implement BasicAuthCredentials and be JSON-deserializable.
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

		var credentials C
		switch c := any(&credentials).(type) {
		case encoding.TextUnmarshaler:
			err = c.UnmarshalText(value)
		case encoding.BinaryUnmarshaler:
			err = c.UnmarshalBinary(value)
		default:
			switch v := reflect.ValueOf(c).Elem(); v.Kind() {
			case reflect.String:
				v.SetString(string(value))
			case reflect.Slice:
				if v.Type().Elem().Kind() == reflect.Uint8 {
					v.SetBytes(value)
					err = nil
					break
				}
				fallthrough
			default:
				err = json.Unmarshal(value, c)
			}
		}
		if err != nil {
			return nil, err
		}
		if subtle.ConstantTimeCompare([]byte(credentials.Password()), []byte(password)) != 1 {
			return nil, ErrUnauthorized
		}
		return ContextWithCredentials(ctx, credentials), nil
	})
}

// NewBasicAuthForwarder returns an http.RoundTripper that injects Basic Auth
// credentials from the context into outbound requests. If the context has no
// credentials, requests pass through unchanged.
func NewBasicAuthForwarder[Credentials BasicAuthCredentials](t http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if creds, ok := CredentialsFromContext[Credentials](req.Context()); ok {
			req = req.Clone(req.Context())
			req.SetBasicAuth(creds.Username(), creds.Password())
		}
		return t.RoundTrip(req)
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
