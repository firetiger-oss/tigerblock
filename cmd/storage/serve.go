package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/storage"
	storagehttp "github.com/firetiger-oss/storage/http"
	"github.com/firetiger-oss/storage/secret"
	"github.com/firetiger-oss/storage/secret/authn"

	_ "github.com/firetiger-oss/storage/secret/aws"
	_ "github.com/firetiger-oss/storage/secret/env"
	_ "github.com/firetiger-oss/storage/secret/gcp"
)

var serveCmd = &cobra.Command{
	Use:   "serve [bucket-uri]",
	Short: "Serve a storage bucket over HTTP",
	Long:  "Start an HTTP server that serves objects from a storage bucket.",
	Args:  cobra.ExactArgs(1),
	RunE:  runServe,
}

func init() {
	serveCmd.Flags().String("http", ":8184", "HTTP server address")
	serveCmd.Flags().String("basic-auth-username", "", "Username for basic auth")
	serveCmd.Flags().String("basic-auth-secret-id", "", "Secret store URI for basic auth credentials")
	serveCmd.Flags().String("presign-secret-id", "", "Secret ID for validating presigned URLs")
}

// basicAuthCredentials stores a password for basic auth validation.
type basicAuthCredentials string

func (c basicAuthCredentials) String() string {
	return secret.Value(c).String()
}

func (c basicAuthCredentials) GoString() string {
	return secret.Value(c).GoString()
}

func (c basicAuthCredentials) Username() string {
	username, _, _ := strings.Cut(string(c), ":")
	return username
}

func (c basicAuthCredentials) Password() string {
	_, password, _ := strings.Cut(string(c), ":")
	return password
}

func runServe(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cmd.SilenceUsage = true

	bucketURI := args[0]
	httpAddr, _ := cmd.Flags().GetString("http")
	basicAuthUsername, _ := cmd.Flags().GetString("basic-auth-username")
	basicAuthSecretID, _ := cmd.Flags().GetString("basic-auth-secret-id")
	presignSecretID, _ := cmd.Flags().GetString("presign-secret-id")

	bucket, err := storage.LoadBucket(ctx, bucketURI)
	if err != nil {
		return err
	}

	handler := storagehttp.BucketHandler(bucket)

	var authenticators []authn.Authenticator

	if presignSecretID != "" {
		store, _, err := secret.Load(ctx, presignSecretID)
		if err != nil {
			return err
		}
		authenticators = append(authenticators, authn.NewSignedURLAuthenticator(store))
	}

	if basicAuthSecretID != "" {
		http.DefaultTransport = authn.NewBasicAuthForwarder[basicAuthCredentials](http.DefaultTransport)

		authenticators = append(authenticators, authn.NewBasicAuthenticator[basicAuthCredentials](
			secret.StoreFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, secret.Info, error) {
				if name != basicAuthUsername {
					return nil, secret.Info{}, secret.ErrNotFound
				}
				value, info, err := secret.Get(ctx, basicAuthSecretID, options...)
				return secret.Value(basicAuthUsername + ":" + string(value)), info, err
			}),
		))
	}

	if len(authenticators) > 0 {
		handler = authn.NewHandler(handler, authenticators...)
	}

	server := &http.Server{Addr: httpAddr, Handler: handler}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}

	return nil
}
