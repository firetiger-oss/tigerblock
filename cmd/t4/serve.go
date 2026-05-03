package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/secret/authn"
	"github.com/firetiger-oss/tigerblock/storage"
	storagehttp "github.com/firetiger-oss/tigerblock/storage/http"

	_ "github.com/firetiger-oss/tigerblock/secret/aws"
	_ "github.com/firetiger-oss/tigerblock/secret/env"
	_ "github.com/firetiger-oss/tigerblock/secret/gcp"
)

const certCacheTTL = 1 * time.Minute

type pemLoader func(ctx context.Context) ([]byte, error)

func newPEMLoader(value string) pemLoader {
	if _, _, _, err := secret.DefaultRegistry().ParseSecret(value); err == nil {
		return func(ctx context.Context) ([]byte, error) {
			v, _, err := secret.Get(ctx, value)
			return []byte(v), err
		}
	}
	return func(ctx context.Context) ([]byte, error) {
		return os.ReadFile(value)
	}
}

type certCache struct {
	mu        sync.Mutex
	cert      *tls.Certificate
	expiresAt time.Time
}

func (c *certCache) get(ctx context.Context, loadCert, loadKey pemLoader) (*tls.Certificate, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cert != nil && time.Now().Before(c.expiresAt) {
		return c.cert, nil
	}
	certPEM, err := loadCert(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading TLS certificate: %w", err)
	}
	keyPEM, err := loadKey(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading TLS key: %w", err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("parsing TLS certificate: %w", err)
	}
	c.cert = &cert
	c.expiresAt = time.Now().Add(certCacheTTL)
	return c.cert, nil
}

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
	serveCmd.Flags().String("tls-cert", "", "TLS certificate: file path or secret URI (e.g. cert.pem, env:CERT_PEM, arn:aws:...)")
	serveCmd.Flags().String("tls-key", "", "TLS private key: file path or secret URI")
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
	tlsCert, _ := cmd.Flags().GetString("tls-cert")
	tlsKey, _ := cmd.Flags().GetString("tls-key")

	if (tlsCert == "") != (tlsKey == "") {
		return fmt.Errorf("--tls-cert and --tls-key must be set together")
	}
	tlsEnabled := tlsCert != ""

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

	if tlsEnabled {
		loadCert := newPEMLoader(tlsCert)
		loadKey := newPEMLoader(tlsKey)
		cache := &certCache{}
		if _, err := cache.get(ctx, loadCert, loadKey); err != nil {
			return err
		}
		server.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return cache.get(hello.Context(), loadCert, loadKey)
			},
		}
	}

	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	var serveErr error
	if tlsEnabled {
		serveErr = server.ListenAndServeTLS("", "")
	} else {
		serveErr = server.ListenAndServe()
	}
	if serveErr != nil && serveErr != http.ErrServerClosed {
		return serveErr
	}

	return nil
}
