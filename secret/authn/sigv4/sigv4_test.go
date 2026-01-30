package sigv4

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type mockCredentialsProvider struct {
	creds aws.Credentials
	err   error
}

func (m mockCredentialsProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	return m.creds, m.err
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testCredentials() aws.Credentials {
	return aws.Credentials{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		SessionToken:    "session-token",
	}
}

func TestNewTransport(t *testing.T) {
	t.Run("signs request with authorization header", func(t *testing.T) {
		var capturedReq *http.Request
		base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})

		transport := NewTransport(base,
			WithService("lambda"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		req, _ := http.NewRequest("GET", "https://example.lambda-url.us-east-1.on.aws/", nil)

		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
			t.Errorf("expected AWS4 signature, got %q", auth)
		}

		amzDate := capturedReq.Header.Get("X-Amz-Date")
		if amzDate == "" {
			t.Error("expected X-Amz-Date header")
		}

		securityToken := capturedReq.Header.Get("X-Amz-Security-Token")
		if securityToken != "session-token" {
			t.Errorf("expected X-Amz-Security-Token header, got %q", securityToken)
		}
	})

	t.Run("auto-detects service and region from lambda URL", func(t *testing.T) {
		var capturedReq *http.Request
		base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})

		transport := NewTransport(base,
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		req, _ := http.NewRequest("GET", "https://abc123.lambda-url.us-east-1.on.aws/", nil)

		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
			t.Errorf("expected AWS4 signature, got %q", auth)
		}
	})

	t.Run("auto-detects service and region from API Gateway URL", func(t *testing.T) {
		var capturedReq *http.Request
		base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})

		transport := NewTransport(base,
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		req, _ := http.NewRequest("GET", "https://xyz789.execute-api.us-west-2.amazonaws.com/prod/items", nil)

		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
			t.Errorf("expected AWS4 signature, got %q", auth)
		}
	})

	t.Run("explicit options override auto-detection", func(t *testing.T) {
		var capturedReq *http.Request
		base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})

		transport := NewTransport(base,
			WithService("custom-service"),
			WithRegion("us-gov-west-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		req, _ := http.NewRequest("GET", "https://abc123.lambda-url.us-east-1.on.aws/", nil)

		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
			t.Errorf("expected AWS4 signature, got %q", auth)
		}
	})

	t.Run("does not modify original request", func(t *testing.T) {
		base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})

		transport := NewTransport(base,
			WithService("lambda"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		req, _ := http.NewRequest("GET", "https://example.com/", nil)

		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if req.Header.Get("Authorization") != "" {
			t.Error("original request should not be modified")
		}
	})

	t.Run("signs POST request with body using unsigned payload by default", func(t *testing.T) {
		var capturedReq *http.Request
		base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})

		transport := NewTransport(base,
			WithService("execute-api"),
			WithRegion("us-west-2"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		body := `{"key": "value"}`
		req, _ := http.NewRequest("POST", "https://api.example.com/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		auth := capturedReq.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
			t.Errorf("expected AWS4 signature, got %q", auth)
		}
	})

	t.Run("signs POST request with body hash when SignPayload is true", func(t *testing.T) {
		var capturedReq *http.Request
		base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})

		transport := NewTransport(base,
			WithService("s3"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
			WithSignPayload(true),
		)

		body := `{"key": "value"}`
		req, _ := http.NewRequest("POST", "https://s3.example.com/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Verify body was preserved
		capturedBody, _ := io.ReadAll(capturedReq.Body)
		if string(capturedBody) != body {
			t.Errorf("body not preserved: got %q, want %q", capturedBody, body)
		}

		auth := capturedReq.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "AWS4-HMAC-SHA256") {
			t.Errorf("expected AWS4 signature, got %q", auth)
		}
	})

	t.Run("uses default transport when nil", func(t *testing.T) {
		rt := NewTransport(nil,
			WithService("lambda"),
			WithRegion("us-east-1"),
		)

		tr := rt.(*transport)
		if tr.base == nil {
			t.Error("expected base transport to be set to default")
		}
	})

	t.Run("disables session token when configured", func(t *testing.T) {
		var capturedReq *http.Request
		base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			capturedReq = req
			return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
		})

		transport := NewTransport(base,
			WithService("lambda"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
			WithDisableSessionToken(true),
		)

		req, _ := http.NewRequest("GET", "https://example.lambda-url.us-east-1.on.aws/", nil)

		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if capturedReq.Header.Get("X-Amz-Security-Token") != "" {
			t.Error("expected no X-Amz-Security-Token header when disabled")
		}
	})
}

func TestCredentialCaching(t *testing.T) {
	callCount := 0
	provider := mockCredentialsProvider{
		creds: testCredentials(),
	}

	// Wrap provider to count calls
	countingProvider := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		callCount++
		return provider.Retrieve(ctx)
	})

	base := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
	})

	transport := NewTransport(base,
		WithService("lambda"),
		WithRegion("us-east-1"),
		WithCredentials(countingProvider),
	)

	// Make multiple requests
	for i := 0; i < 5; i++ {
		req, _ := http.NewRequest("GET", "https://example.com/", nil)
		_, err := transport.RoundTrip(req)
		if err != nil {
			t.Fatalf("request %d: unexpected error: %v", i, err)
		}
	}

	// Credentials should only be retrieved once due to caching
	if callCount != 1 {
		t.Errorf("expected credentials to be retrieved once, got %d", callCount)
	}
}

func TestNewSigner(t *testing.T) {
	t.Run("generates presigned URL with required parameters", func(t *testing.T) {
		signer := NewSigner(
			WithService("s3"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		u, _ := url.Parse("https://my-bucket.s3.us-east-1.amazonaws.com/test-key")
		expiration := time.Now().Add(15 * time.Minute)

		signedURL, err := signer.Sign(context.Background(), "GET", u, expiration)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parsedURL, err := url.Parse(signedURL)
		if err != nil {
			t.Fatalf("failed to parse signed URL: %v", err)
		}

		q := parsedURL.Query()

		// Check required SigV4 query parameters
		if q.Get("X-Amz-Algorithm") != "AWS4-HMAC-SHA256" {
			t.Errorf("expected X-Amz-Algorithm=AWS4-HMAC-SHA256, got %q", q.Get("X-Amz-Algorithm"))
		}

		if q.Get("X-Amz-Credential") == "" {
			t.Error("expected X-Amz-Credential to be set")
		}

		if q.Get("X-Amz-Date") == "" {
			t.Error("expected X-Amz-Date to be set")
		}

		if q.Get("X-Amz-Expires") == "" {
			t.Error("expected X-Amz-Expires to be set")
		}

		if q.Get("X-Amz-Signature") == "" {
			t.Error("expected X-Amz-Signature to be set")
		}

		if q.Get("X-Amz-SignedHeaders") == "" {
			t.Error("expected X-Amz-SignedHeaders to be set")
		}
	})

	t.Run("includes security token when credentials have session token", func(t *testing.T) {
		signer := NewSigner(
			WithService("s3"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		u, _ := url.Parse("https://my-bucket.s3.us-east-1.amazonaws.com/test-key")
		expiration := time.Now().Add(15 * time.Minute)

		signedURL, err := signer.Sign(context.Background(), "GET", u, expiration)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parsedURL, _ := url.Parse(signedURL)
		q := parsedURL.Query()

		if q.Get("X-Amz-Security-Token") != "session-token" {
			t.Errorf("expected X-Amz-Security-Token=session-token, got %q", q.Get("X-Amz-Security-Token"))
		}
	})

	t.Run("omits security token when DisableSessionToken is true", func(t *testing.T) {
		signer := NewSigner(
			WithService("s3"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
			WithDisableSessionToken(true),
		)

		u, _ := url.Parse("https://my-bucket.s3.us-east-1.amazonaws.com/test-key")
		expiration := time.Now().Add(15 * time.Minute)

		signedURL, err := signer.Sign(context.Background(), "GET", u, expiration)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parsedURL, _ := url.Parse(signedURL)
		q := parsedURL.Query()

		if q.Get("X-Amz-Security-Token") != "" {
			t.Error("expected no X-Amz-Security-Token when disabled")
		}
	})

	t.Run("sets correct expiration duration", func(t *testing.T) {
		signer := NewSigner(
			WithService("s3"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		u, _ := url.Parse("https://my-bucket.s3.us-east-1.amazonaws.com/test-key")
		expiration := time.Now().Add(1 * time.Hour)

		signedURL, err := signer.Sign(context.Background(), "GET", u, expiration)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parsedURL, _ := url.Parse(signedURL)
		q := parsedURL.Query()

		expires := q.Get("X-Amz-Expires")
		// Should be approximately 3600 seconds (within 5 second tolerance)
		var expiresInt int
		fmt.Sscanf(expires, "%d", &expiresInt)
		if expiresInt < 3595 || expiresInt > 3605 {
			t.Errorf("expected X-Amz-Expires ~3600, got %d", expiresInt)
		}
	})

	t.Run("works with PUT method", func(t *testing.T) {
		signer := NewSigner(
			WithService("s3"),
			WithRegion("us-east-1"),
			WithCredentials(mockCredentialsProvider{creds: testCredentials()}),
		)

		u, _ := url.Parse("https://my-bucket.s3.us-east-1.amazonaws.com/test-key")
		expiration := time.Now().Add(15 * time.Minute)

		signedURL, err := signer.Sign(context.Background(), "PUT", u, expiration)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		parsedURL, _ := url.Parse(signedURL)
		q := parsedURL.Query()

		if q.Get("X-Amz-Signature") == "" {
			t.Error("expected X-Amz-Signature to be set for PUT request")
		}
	})

	t.Run("caches credentials across multiple Sign calls", func(t *testing.T) {
		callCount := 0
		provider := mockCredentialsProvider{
			creds: testCredentials(),
		}

		countingProvider := aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			callCount++
			return provider.Retrieve(ctx)
		})

		signer := NewSigner(
			WithService("s3"),
			WithRegion("us-east-1"),
			WithCredentials(countingProvider),
		)

		u, _ := url.Parse("https://my-bucket.s3.us-east-1.amazonaws.com/test-key")
		expiration := time.Now().Add(15 * time.Minute)

		// Make multiple Sign calls
		for i := 0; i < 5; i++ {
			_, err := signer.Sign(context.Background(), "GET", u, expiration)
			if err != nil {
				t.Fatalf("request %d: unexpected error: %v", i, err)
			}
		}

		// Credentials should only be retrieved once due to caching
		if callCount != 1 {
			t.Errorf("expected credentials to be retrieved once, got %d", callCount)
		}
	})
}

func TestParseEndpoint(t *testing.T) {
	tests := []struct {
		host    string
		service string
		region  string
	}{
		// Lambda function URLs
		{"abc123.lambda-url.us-east-1.on.aws", "lambda", "us-east-1"},
		{"xyz789.lambda-url.eu-west-1.on.aws", "lambda", "eu-west-1"},

		// API Gateway
		{"api123.execute-api.us-west-2.amazonaws.com", "execute-api", "us-west-2"},
		{"xyz.execute-api.ap-southeast-1.amazonaws.com", "execute-api", "ap-southeast-1"},

		// S3
		{"s3.us-east-1.amazonaws.com", "s3", "us-east-1"},
		{"mybucket.s3.eu-central-1.amazonaws.com", "s3", "eu-central-1"},

		// Other AWS services
		{"lambda.us-east-1.amazonaws.com", "lambda", "us-east-1"},
		{"dynamodb.ap-northeast-1.amazonaws.com", "dynamodb", "ap-northeast-1"},

		// With port
		{"abc123.lambda-url.us-east-1.on.aws:443", "lambda", "us-east-1"},
		{"api.execute-api.us-west-2.amazonaws.com:8080", "execute-api", "us-west-2"},

		// Unknown hosts return empty
		{"example.com", "", ""},
		{"localhost", "", ""},
		{"api.custom-domain.com", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			service, region := parseEndpoint(tt.host)
			if service != tt.service {
				t.Errorf("parseEndpoint(%q) service = %q, want %q", tt.host, service, tt.service)
			}
			if region != tt.region {
				t.Errorf("parseEndpoint(%q) region = %q, want %q", tt.host, region, tt.region)
			}
		})
	}
}
