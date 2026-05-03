package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/tigerblock/cache"
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
	Use:   "serve <bucket-uri> | <name>=<uri> [<name>=<uri>...]",
	Short: "Serve one or more storage buckets over HTTP",
	Long: `Start an HTTP server that serves objects from one or more storage buckets.

Two argument forms are supported:

  - A single unnamed bucket URI is mounted at /:
      t4 serve file:///tmp/data

  - One or more <name>=<uri> pairs are mounted via path-style addressing
    at /<name>/...; bucket names must match [A-Za-z0-9._-]+:
      t4 serve a=file:///tmp/a b=file:///tmp/b

The two forms cannot be mixed.`,
	Args: cobra.MinimumNArgs(1),
	RunE: runServe,
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

// bucketSpec identifies one bucket to serve. An empty name means the
// bucket is mounted at the server root (single-bucket back-compat mode).
type bucketSpec struct {
	name string
	uri  string
}

var bucketNameRegexp = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func parseBucketArgs(args []string) ([]bucketSpec, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("at least one bucket argument is required")
	}

	specs := make([]bucketSpec, 0, len(args))
	namedCount := 0
	seen := make(map[string]bool, len(args))
	for _, arg := range args {
		name, uri, hasEq := strings.Cut(arg, "=")
		if hasEq && bucketNameRegexp.MatchString(name) {
			if uri == "" {
				return nil, fmt.Errorf("invalid bucket argument %q: uri must be non-empty", arg)
			}
			if seen[name] {
				return nil, fmt.Errorf("duplicate bucket name %q", name)
			}
			seen[name] = true
			specs = append(specs, bucketSpec{name: name, uri: uri})
			namedCount++
			continue
		}
		// `=` may legitimately appear in a positional URI (e.g.
		// `file:///tmp/a=b`); fall through to positional treatment
		// when the LHS doesn't look like a bucket name.
		specs = append(specs, bucketSpec{uri: arg})
	}

	switch {
	case namedCount == len(specs):
		return specs, nil
	case namedCount == 0 && len(specs) == 1:
		return specs, nil
	default:
		return nil, fmt.Errorf("all bucket arguments must be of the form name=uri (or pass a single bucket URI without a name)")
	}
}

func buildBucketMux(ctx context.Context, specs []bucketSpec, handlerOpts ...storagehttp.HandlerOption) (http.Handler, error) {
	if len(specs) == 1 && specs[0].name == "" {
		bucket, err := storage.LoadBucket(ctx, specs[0].uri)
		if err != nil {
			return nil, err
		}
		return storagehttp.BucketHandler(bucket, handlerOpts...), nil
	}

	mux := http.NewServeMux()
	names := make([]string, 0, len(specs))
	for _, b := range specs {
		bucket, err := storage.LoadBucket(ctx, b.uri)
		if err != nil {
			return nil, fmt.Errorf("loading bucket %q: %w", b.name, err)
		}
		h := storagehttp.StripBucketNamePrefix(b.name, storagehttp.BucketHandler(bucket, handlerOpts...))
		mux.Handle("/"+b.name, h)
		mux.Handle("/"+b.name+"/", h)
		names = append(names, b.name)
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string][]string{"buckets": names})
			return
		}
		first, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
		storagehttp.Error(w, "NoSuchBucket", "The specified bucket does not exist", first, http.StatusNotFound)
	})

	return mux, nil
}

func runServe(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cmd.SilenceUsage = true

	specs, err := parseBucketArgs(args)
	if err != nil {
		return err
	}

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

	handler, err := buildBucketMux(ctx, specs)
	if err != nil {
		return err
	}

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
