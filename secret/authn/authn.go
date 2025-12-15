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
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	})
}

// Unmarshal decodes a secret value into credentials.
// It supports types that implement encoding.TextUnmarshaler, encoding.BinaryUnmarshaler,
// string-based types, []byte, or JSON-deserializable structs.
func Unmarshal[Credentials any](value secret.Value) (Credentials, error) {
	var credentials Credentials
	switch c := any(&credentials).(type) {
	case encoding.TextUnmarshaler:
		if err := c.UnmarshalText(value); err != nil {
			return credentials, err
		}
	case encoding.BinaryUnmarshaler:
		if err := c.UnmarshalBinary(value); err != nil {
			return credentials, err
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
					return credentials, err
				}
			}
		default:
			if err := json.Unmarshal(value, c); err != nil {
				return credentials, err
			}
		}
	}
	return credentials, nil
}

// Marshal encodes credentials into a secret value.
// It supports types that implement encoding.TextMarshaler, encoding.BinaryMarshaler,
// string-based types, []byte, or JSON-serializable structs.
func Marshal[Credentials any](creds Credentials) (secret.Value, error) {
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
