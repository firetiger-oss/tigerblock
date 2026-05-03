package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/secret/authn"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/cache"
	storagehttp "github.com/firetiger-oss/tigerblock/storage/http"

	_ "github.com/firetiger-oss/tigerblock/secret/aws"
	_ "github.com/firetiger-oss/tigerblock/secret/env"
	_ "github.com/firetiger-oss/tigerblock/secret/gcp"
)

const certCacheTTL = 1 * time.Minute

type pemLoader func(ctx context.Context) ([]byte, error)

func fileLoader(path string) pemLoader {
	return func(ctx context.Context) ([]byte, error) {
		return os.ReadFile(path)
	}
}

func secretLoader(id string) pemLoader {
	return func(ctx context.Context) ([]byte, error) {
		v, _, err := secret.Get(ctx, id)
		return []byte(v), err
	}
}

type certProvider struct {
	loadCert pemLoader
	loadKey  pemLoader
	ttl      time.Duration
	cache    cache.TTL[struct{}, *tls.Certificate]
	lastGood atomic.Pointer[tls.Certificate]
}

func (p *certProvider) get(ctx context.Context) (*tls.Certificate, error) {
	now := time.Now()
	cert, _, err := p.cache.Load(struct{}{}, now, func() (int64, *tls.Certificate, time.Time, error) {
		certPEM, err := p.loadCert(ctx)
		if err != nil {
			return 0, nil, time.Time{}, fmt.Errorf("loading TLS certificate: %w", err)
		}
		keyPEM, err := p.loadKey(ctx)
		if err != nil {
			return 0, nil, time.Time{}, fmt.Errorf("loading TLS key: %w", err)
		}
		c, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return 0, nil, time.Time{}, fmt.Errorf("parsing TLS certificate: %w", err)
		}
		return 0, &c, now.Add(p.ttl), nil
	})
	if err != nil {
		if good := p.lastGood.Load(); good != nil {
			log.Printf("warning: TLS cert refresh failed, serving last known good cert: %v", err)
			return good, nil
		}
		return nil, err
	}
	p.lastGood.Store(cert)
	return cert, nil
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
	serveCmd.Flags().String("tls-cert-file", "", "Path to PEM-encoded TLS certificate file")
	serveCmd.Flags().String("tls-cert-secret", "", "Secret ID resolving to PEM-encoded TLS certificate")
	serveCmd.Flags().String("tls-key-file", "", "Path to PEM-encoded TLS private key file")
	serveCmd.Flags().String("tls-key-secret", "", "Secret ID resolving to PEM-encoded TLS private key")
}

func tlsLoader(file, secretID, label string) (pemLoader, error) {
	switch {
	case file != "" && secretID != "":
		return nil, fmt.Errorf("--tls-%s-file and --tls-%s-secret are mutually exclusive", label, label)
	case file != "":
		return fileLoader(file), nil
	case secretID != "":
		return secretLoader(secretID), nil
	default:
		return nil, nil
	}
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
	tlsCertFile, _ := cmd.Flags().GetString("tls-cert-file")
	tlsCertSecret, _ := cmd.Flags().GetString("tls-cert-secret")
	tlsKeyFile, _ := cmd.Flags().GetString("tls-key-file")
	tlsKeySecret, _ := cmd.Flags().GetString("tls-key-secret")

	loadCert, err := tlsLoader(tlsCertFile, tlsCertSecret, "cert")
	if err != nil {
		return err
	}
	loadKey, err := tlsLoader(tlsKeyFile, tlsKeySecret, "key")
	if err != nil {
		return err
	}
	if (loadCert == nil) != (loadKey == nil) {
		return fmt.Errorf("TLS certificate and key must be set together")
	}
	tlsEnabled := loadCert != nil

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
		provider := &certProvider{
			loadCert: loadCert,
			loadKey:  loadKey,
			ttl:      certCacheTTL,
			cache:    cache.TTL[struct{}, *tls.Certificate]{Limit: 1},
		}
		if _, err := provider.get(ctx); err != nil {
			return err
		}
		server.TLSConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
				return provider.get(hello.Context())
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
