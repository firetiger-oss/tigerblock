package main

import (
	"context"
	"net/http"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/tigerblock/storage"
	storagehttp "github.com/firetiger-oss/tigerblock/storage/http"
	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/secret/authn"

	_ "github.com/firetiger-oss/tigerblock/secret/aws"
	_ "github.com/firetiger-oss/tigerblock/secret/env"
	_ "github.com/firetiger-oss/tigerblock/secret/gcp"
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
	serveCmd.Flags().String("bearer-token-secret-id", "", "Secret store URI for bearer token auth")
	serveCmd.Flags().String("presign-secret-id", "", "Secret ID for validating presigned URLs")
}

func runServe(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cmd.SilenceUsage = true

	bucketURI := args[0]
	httpAddr, _ := cmd.Flags().GetString("http")
	basicAuthUsername, _ := cmd.Flags().GetString("basic-auth-username")
	basicAuthSecretID, _ := cmd.Flags().GetString("basic-auth-secret-id")
	bearerTokenSecretID, _ := cmd.Flags().GetString("bearer-token-secret-id")
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
		http.DefaultTransport = authn.NewBasicAuthForwarder(http.DefaultTransport)

		authenticators = append(authenticators, authn.NewBasicAuthenticator(
			authn.LoaderFunc[authn.Basic](func(ctx context.Context, name string) (authn.Basic, error) {
				if name != basicAuthUsername {
					return authn.Basic{}, secret.ErrNotFound
				}
				value, _, err := secret.Get(ctx, basicAuthSecretID)
				if err != nil {
					return authn.Basic{}, err
				}
				return authn.Basic{basicAuthUsername, string(value)}, nil
			}),
		))
	}

	if bearerTokenSecretID != "" {
		authenticators = append(authenticators, authn.NewBearerAuthenticator(
			authn.LoaderFunc[authn.Bearer](func(ctx context.Context, _ string) (authn.Bearer, error) {
				value, _, err := secret.Get(ctx, bearerTokenSecretID)
				if err != nil {
					return "", err
				}
				return authn.Bearer(value), nil
			}),
			"",
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
