package main

import (
	"context"
	"net/http"

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
	serveCmd.Flags().String("http", ":8080", "HTTP server address")
	serveCmd.Flags().String("basic-auth-username", "", "Username for basic auth")
	serveCmd.Flags().String("basic-auth-secret-id", "", "Secret store URI for basic auth credentials")
}

// basicAuthCredentials stores a password for basic auth validation.
// The secret value must be JSON: {"password": "..."}
type basicAuthCredentials struct {
	username string
	password string
}

func (c basicAuthCredentials) Validate(username, password string) bool {
	return c.username == username && c.password == password
}

func (c *basicAuthCredentials) UnmarshalText(data []byte) error {
	c.password = string(data)
	return nil
}

func runServe(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cmd.SilenceUsage = true

	bucketURI := args[0]
	httpAddr, _ := cmd.Flags().GetString("http")
	basicAuthUsername, _ := cmd.Flags().GetString("basic-auth-username")
	basicAuthSecretID, _ := cmd.Flags().GetString("basic-auth-secret-id")

	bucket, err := storage.LoadBucket(ctx, bucketURI)
	if err != nil {
		return err
	}

	handler := storagehttp.BucketHandler(bucket)

	if basicAuthSecretID != "" {
		auth := authn.NewBasicAuthenticator[basicAuthCredentials](
			secret.StoreFunc(func(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, secret.Info, error) {
				if name != basicAuthUsername {
					return secret.Value{}, secret.Info{}, secret.ErrNotFound
				}
				return secret.Get(ctx, basicAuthSecretID, options...)
			}),
		)
		handler = authn.NewHandler(auth, handler)
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
