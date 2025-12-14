// Package authn provides HTTP authentication using secrets.
package authn

import (
	"context"
	"encoding"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"

	"github.com/firetiger-oss/storage/secret"
)

// ErrUnauthorized is returned when authentication fails.
var ErrUnauthorized = errors.New("unauthorized")

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
type credentialsContextKey[Credentials any] struct{}

// CredentialsFromContext retrieves credentials from the context.
// Returns the credentials and true if present, zero value and false otherwise.
func CredentialsFromContext[Credentials any](ctx context.Context) (Credentials, bool) {
	creds, ok := ctx.Value(credentialsContextKey[Credentials]{}).(Credentials)
	return creds, ok
}

// ContextWithCredentials returns a new context with credentials.
func ContextWithCredentials[Credentials any](ctx context.Context, creds Credentials) context.Context {
	return context.WithValue(ctx, credentialsContextKey[Credentials]{}, creds)
}

// NewHandler creates an HTTP handler that authenticates requests.
// On success, calls next handler with the context returned by auth.Authenticate.
// On failure, responds with 401 Unauthorized.
func NewHandler(auth Authenticator, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, err := auth.Authenticate(r.Context(), r)
		if err != nil {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Credentials is a constraint interface for types that can validate a password.
// Implementations should store the password hash and perform secure comparison.
type Credentials interface {
	Validate(username, password string) bool
}

// NewBasicAuthenticator returns an Authenticator that uses HTTP Basic Authentication.
// C must implement Credentials and be JSON-deserializable.
// Uses the username from Basic Auth as the secret name.
// Injects credentials into context via ContextWithCredentials[C].
func NewBasicAuthenticator[C Credentials](store secret.Store) Authenticator {
	return AuthenticatorFunc(func(ctx context.Context, req *http.Request) (context.Context, error) {
		username, password, ok := req.BasicAuth()
		if !ok {
			return nil, ErrUnauthorized
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
		if !credentials.Validate(username, password) {
			return nil, ErrUnauthorized
		}
		return ContextWithCredentials(ctx, credentials), nil
	})
}
