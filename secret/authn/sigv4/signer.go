package sigv4

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/firetiger-oss/storage/secret"
)

// Signer implements secret.Signer using AWS SigV4 presigned URLs.
type Signer struct {
	config Config
	signer *v4.Signer

	mu             sync.RWMutex
	cachedProvider aws.CredentialsProvider
	cachedCreds    aws.Credentials
	credsExpiry    time.Time
}

// NewSigner creates a secret.Signer that generates AWS SigV4 presigned URLs.
//
// Example usage:
//
//	cfg, _ := config.LoadDefaultConfig(ctx)
//	signer := sigv4.NewSigner(
//	    sigv4.WithService("s3"),
//	    sigv4.WithRegion("us-east-1"),
//	    sigv4.WithCredentials(cfg.Credentials),
//	)
func NewSigner(options ...Option) secret.Signer {
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

	return &Signer{
		config: config,
		signer: v4.NewSigner(signerOpts...),
	}
}

// Sign generates a presigned URL for the given HTTP method and URL.
// The expiration time specifies when the presigned URL expires.
func (s *Signer) Sign(ctx context.Context, method string, u *url.URL, expiration time.Time) (string, error) {
	creds, err := s.retrieveCredentials(ctx)
	if err != nil {
		return "", fmt.Errorf("sigv4: failed to retrieve credentials: %w", err)
	}

	service, region := s.config.Service, s.config.Region
	if service == "" || region == "" {
		parsedService, parsedRegion := parseEndpoint(u.Host)
		if service == "" {
			service = parsedService
		}
		if region == "" {
			region = parsedRegion
		}
	}

	// Calculate duration until expiration in seconds
	duration := time.Until(expiration)
	if duration < time.Second {
		duration = time.Second
	}
	expiresSeconds := int64(duration.Seconds())

	// Create a request for presigning
	req, err := http.NewRequestWithContext(ctx, method, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("sigv4: failed to create request: %w", err)
	}

	// Add X-Amz-Expires to the query parameters before signing
	q := req.URL.Query()
	q.Set("X-Amz-Expires", strconv.FormatInt(expiresSeconds, 10))
	req.URL.RawQuery = q.Encode()

	// Use PresignHTTP to generate the presigned URL
	signedURL, _, err := s.signer.PresignHTTP(
		ctx,
		creds,
		req,
		emptyPayloadHash,
		service,
		region,
		time.Now(),
	)
	if err != nil {
		return "", fmt.Errorf("sigv4: failed to presign request: %w", err)
	}

	return signedURL, nil
}

func (s *Signer) retrieveCredentials(ctx context.Context) (aws.Credentials, error) {
	s.mu.RLock()
	if s.cachedCreds.HasKeys() && time.Now().Before(s.credsExpiry) {
		creds := s.cachedCreds
		s.mu.RUnlock()
		return creds, nil
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if s.cachedCreds.HasKeys() && time.Now().Before(s.credsExpiry) {
		return s.cachedCreds, nil
	}

	provider := s.config.Credentials
	if provider == nil {
		// Load default credentials lazily
		if s.cachedProvider == nil {
			cfg, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				return aws.Credentials{}, err
			}
			s.cachedProvider = cfg.Credentials
		}
		provider = s.cachedProvider
	}

	creds, err := provider.Retrieve(ctx)
	if err != nil {
		return aws.Credentials{}, err
	}

	s.cachedCreds = creds
	// Cache until 5 minutes before expiration, or for 5 minutes if no expiration
	if creds.CanExpire && !creds.Expires.IsZero() {
		s.credsExpiry = creds.Expires.Add(-5 * time.Minute)
	} else {
		s.credsExpiry = time.Now().Add(5 * time.Minute)
	}

	return creds, nil
}
