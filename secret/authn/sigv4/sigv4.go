package sigv4

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
)

// Option configures AWS SigV4 signing behavior.
type Option func(*Config)

// Config holds configuration for AWS SigV4 signing.
type Config struct {
	// Service is the AWS service name (e.g., "lambda", "execute-api", "s3").
	Service string

	// Region is the AWS region (e.g., "us-east-1").
	Region string

	// Credentials provides AWS credentials for signing.
	// If nil, requests pass through unsigned.
	Credentials aws.CredentialsProvider

	// SignPayload controls whether the request body is included in the signature.
	// When false, uses "UNSIGNED-PAYLOAD" (faster for large bodies).
	// When true, computes SHA-256 hash of the body.
	// Default: false
	SignPayload bool

	// DisableSessionToken controls whether the X-Amz-Security-Token header is set.
	// Default: false (session token is included if available)
	DisableSessionToken bool
}

// WithService sets the AWS service name for signing.
func WithService(service string) Option {
	return func(c *Config) {
		c.Service = service
	}
}

// WithRegion sets the AWS region for signing.
func WithRegion(region string) Option {
	return func(c *Config) {
		c.Region = region
	}
}

// WithCredentials sets the credentials provider for signing.
func WithCredentials(creds aws.CredentialsProvider) Option {
	return func(c *Config) {
		c.Credentials = creds
	}
}

// WithSignPayload controls whether the request body is included in the signature.
func WithSignPayload(sign bool) Option {
	return func(c *Config) {
		c.SignPayload = sign
	}
}

// WithDisableSessionToken controls whether the X-Amz-Security-Token header is set.
func WithDisableSessionToken(disable bool) Option {
	return func(c *Config) {
		c.DisableSessionToken = disable
	}
}

// NewTransport creates an http.RoundTripper that signs requests using AWS SigV4.
//
// The options specify the AWS service name, region, and credentials provider.
// Requests are signed before being passed to the underlying transport.
//
// Example usage with Lambda function URLs:
//
//	cfg, _ := config.LoadDefaultConfig(ctx)
//	transport := sigv4.NewTransport(http.DefaultTransport,
//	    sigv4.WithService("lambda"),
//	    sigv4.WithRegion("us-east-1"),
//	    sigv4.WithCredentials(cfg.Credentials),
//	)
//	client := &http.Client{Transport: transport}
func NewTransport(base http.RoundTripper, options ...Option) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}

	var config Config
	for _, opt := range options {
		opt(&config)
	}

	var signerOpts []func(*v4.SignerOptions)
	if config.DisableSessionToken {
		signerOpts = append(signerOpts, func(o *v4.SignerOptions) {
			o.DisableSessionToken = true
		})
	}

	return &transport{
		base:   base,
		signer: v4.NewSigner(signerOpts...),
		config: config,
	}
}

type transport struct {
	base   http.RoundTripper
	signer *v4.Signer
	config Config

	mu             sync.RWMutex
	cachedProvider aws.CredentialsProvider
	cachedCreds    aws.Credentials
	credsExpiry    time.Time
}

// RoundTrip implements http.RoundTripper.
// It signs the request using AWS SigV4 before passing it to the underlying transport.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())

	creds, err := t.retrieveCredentials(req.Context())
	if err != nil {
		return nil, fmt.Errorf("sigv4: failed to retrieve credentials: %w", err)
	}

	service, region := t.config.Service, t.config.Region
	if service == "" || region == "" {
		parsedService, parsedRegion := parseEndpoint(req.Host)
		if service == "" {
			service = parsedService
		}
		if region == "" {
			region = parsedRegion
		}
	}

	payloadHash, err := t.computePayloadHash(req)
	if err != nil {
		return nil, fmt.Errorf("sigv4: failed to compute payload hash: %w", err)
	}

	err = t.signer.SignHTTP(
		req.Context(),
		creds,
		req,
		payloadHash,
		service,
		region,
		time.Now(),
	)
	if err != nil {
		return nil, fmt.Errorf("sigv4: failed to sign request: %w", err)
	}

	return t.base.RoundTrip(req)
}

func (t *transport) retrieveCredentials(ctx context.Context) (aws.Credentials, error) {
	t.mu.RLock()
	if t.cachedCreds.HasKeys() && time.Now().Before(t.credsExpiry) {
		creds := t.cachedCreds
		t.mu.RUnlock()
		return creds, nil
	}
	t.mu.RUnlock()

	t.mu.Lock()
	defer t.mu.Unlock()

	// Double-check after acquiring write lock
	if t.cachedCreds.HasKeys() && time.Now().Before(t.credsExpiry) {
		return t.cachedCreds, nil
	}

	provider := t.config.Credentials
	if provider == nil {
		// Load default credentials lazily
		if t.cachedProvider == nil {
			cfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				return aws.Credentials{}, err
			}
			t.cachedProvider = cfg.Credentials
		}
		provider = t.cachedProvider
	}

	creds, err := provider.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, err
	}

	t.cachedCreds = creds
	// Cache until 5 minutes before expiration, or for 5 minutes if no expiration
	if creds.CanExpire && !creds.Expires.IsZero() {
		t.credsExpiry = creds.Expires.Add(-5 * time.Minute)
	} else {
		t.credsExpiry = time.Now().Add(5 * time.Minute)
	}

	return creds, nil
}

func (t *transport) computePayloadHash(req *http.Request) (string, error) {
	if !t.config.SignPayload {
		return unsignedPayload, nil
	}

	if req.Body == nil || req.ContentLength == 0 {
		return emptyPayloadHash, nil
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		return "", err
	}
	req.Body.Close()

	req.Body = io.NopCloser(bytes.NewReader(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}

	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}

const (
	unsignedPayload  = "UNSIGNED-PAYLOAD"
	emptyPayloadHash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// parseEndpoint extracts service and region from an AWS endpoint hostname.
// Returns empty strings if the hostname doesn't match known AWS patterns.
func parseEndpoint(host string) (service, region string) {
	// Strip port if present
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	// Lambda function URLs: {id}.lambda-url.{region}.on.aws
	if suffix, ok := strings.CutSuffix(host, ".on.aws"); ok {
		if _, rest, ok := strings.Cut(suffix, ".lambda-url."); ok {
			if region, _, _ = strings.Cut(rest, "."); region != "" {
				return "lambda", region
			}
		}
	}

	// Standard AWS endpoints: {service}.{region}.amazonaws.com
	if suffix, ok := strings.CutSuffix(host, ".amazonaws.com"); ok {
		if rest, region, ok := cutLast(suffix, '.'); ok {
			if _, service, ok := cutLast(rest, '.'); ok {
				return service, region
			}
			// Only two segments: service.region
			return rest, region
		}
	}

	return "", ""
}

// cutLast is like strings.Cut but finds the last separator.
func cutLast(s string, sep byte) (before, after string, ok bool) {
	if i := strings.LastIndexByte(s, sep); i >= 0 {
		return s[:i], s[i+1:], true
	}
	return "", "", false
}
