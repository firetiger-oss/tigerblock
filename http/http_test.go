package http_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/firetiger-oss/storage"
	storagehttp "github.com/firetiger-oss/storage/http"
	"github.com/firetiger-oss/storage/memory"
	s3storage "github.com/firetiger-oss/storage/s3"
	storagetest "github.com/firetiger-oss/storage/test"
)

func TestHTTPStorage(t *testing.T) {
	tests := []struct {
		scenario string
		options  []storagehttp.BucketOption
	}{
		{
			scenario: "default",
			options:  []storagehttp.BucketOption{},
		},
		{
			scenario: "list-type=1",
			options: []storagehttp.BucketOption{
				storagehttp.WithListType("1"),
			},
		},
		{
			scenario: "list-type=2",
			options: []storagehttp.BucketOption{
				storagehttp.WithListType("2"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			storagetest.TestStorage(t, func(t *testing.T) (storage.Bucket, error) {
				l, err := net.Listen("tcp", ":0")
				if err != nil {
					return nil, err
				}

				location := "http://" + l.Addr().String()

				s := &http.Server{
					Handler: storagehttp.BucketHandler(new(memory.Bucket),
						storagehttp.WithLocation(location),
						storagehttp.WithMaxKeys(1),
					),
				}

				go s.Serve(l)

				t.Cleanup(func() {
					s.Close()
					l.Close()
				})

				return storagehttp.NewRegistry("http", test.options...).LoadBucket(t.Context(), location)
			})
		})
	}
}

// TestHTTPStorageWithS3Client tests the HTTP storage implementation
// using the S3 client as a client, to ensure S3 compatibility.
func TestHTTPStorageWithS3Client(t *testing.T) {
	storagetest.TestStorage(t, func(t *testing.T) (storage.Bucket, error) {
		// We have to strip the "/testbucket" prefix from the URL because the
		// S3 client uses path-style due to setting the endpoint resolver with
		// an immutable hostname.
		server := httptest.NewServer(
			storagehttp.StripBucketNamePrefix("testbucket",
				storagehttp.BucketHandler(new(memory.Bucket)),
			),
		)

		t.Cleanup(func() {
			server.Close()
		})

		s3Config, err := config.LoadDefaultConfig(t.Context())
		if err != nil {
			return nil, err
		}

		s3Client := s3.NewFromConfig(s3Config, func(o *s3.Options) {
			o.Region = "us-east-1"
			o.Credentials = aws.AnonymousCredentials{}
			o.BaseEndpoint = aws.String(server.URL)
			o.UsePathStyle = true
			o.HTTPClient = &http.Client{
				Transport: &debugTransport{
					transport: http.DefaultTransport,
					t:         t,
				},
			}
		})
		return s3storage.NewBucket(s3Client, "testbucket"), nil
	})
}

type debugTransport struct {
	transport http.RoundTripper
	t         testing.TB
}

func (debug *debugTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	b, _ := httputil.DumpRequestOut(req, false)
	debug.t.Log(string(b))

	res, err := debug.transport.RoundTrip(req)
	if err != nil {
		debug.t.Logf("error make http request: %v\n", err)
		return nil, err
	}

	b, _ = httputil.DumpResponse(res, false)
	debug.t.Log(string(b))

	res.Body = &debugReadCloser{body: res.Body, t: debug.t}
	return res, nil

}

type debugReadCloser struct {
	body io.ReadCloser
	read int64
	t    testing.TB
}

func (debug *debugReadCloser) Read(p []byte) (int, error) {
	n, err := debug.body.Read(p)
	debug.read += int64(n)
	if err != nil && err != io.EOF {
		debug.t.Logf("error reading the response body after reading %d bytes: %v\n", debug.read, err)
	}
	return n, err
}

func (debug *debugReadCloser) Close() error {
	err := debug.body.Close()
	if err != nil {
		debug.t.Logf("error closing the response body after reading %d bytes: %v\n", debug.read, err)
	}
	return err
}

// rateLimitingTransport is a custom http.RoundTripper that simulates rate limiting
// by returning HTTP 429 (Too Many Requests) responses for all requests.
type rateLimitingTransport struct {
	transport http.RoundTripper
}

func (rt *rateLimitingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Always return 429 Too Many Requests
	return &http.Response{
		Status:     "429 Too Many Requests",
		StatusCode: http.StatusTooManyRequests,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewBufferString("rate limit exceeded")),
		Request:    req,
	}, nil
}

// TestHTTPStorageRateLimiting verifies that the HTTP backend properly handles
// 429 Too Many Requests responses and returns storage.ErrTooManyRequests.
func TestHTTPStorageRateLimiting(t *testing.T) {
	// Create a test server with a memory bucket backend
	server := httptest.NewServer(
		storagehttp.BucketHandler(new(memory.Bucket)),
	)
	t.Cleanup(func() {
		server.Close()
	})

	// Create HTTP client with rate-limiting transport
	client := &http.Client{
		Transport: &rateLimitingTransport{
			transport: http.DefaultTransport,
		},
	}

	// Create bucket with the rate-limiting client
	bucket := storagehttp.NewBucket(server.URL, storagehttp.WithClient(client))

	ctx := t.Context()

	// Test GetObject returns ErrTooManyRequests
	t.Run("GetObject", func(t *testing.T) {
		_, _, err := bucket.GetObject(ctx, "test-key")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, storage.ErrTooManyRequests) {
			t.Errorf("expected ErrTooManyRequests, got: %v", err)
		}
		// Verify error message includes context
		if !strings.Contains(err.Error(), "429") {
			t.Errorf("expected error message to contain status code 429, got: %v", err)
		}
	})

	// Test PutObject returns ErrTooManyRequests
	t.Run("PutObject", func(t *testing.T) {
		_, err := bucket.PutObject(ctx, "test-key", strings.NewReader("test-value"))
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, storage.ErrTooManyRequests) {
			t.Errorf("expected ErrTooManyRequests, got: %v", err)
		}
		if !strings.Contains(err.Error(), "429") {
			t.Errorf("expected error message to contain status code 429, got: %v", err)
		}
	})

	// Test HeadObject returns ErrTooManyRequests
	t.Run("HeadObject", func(t *testing.T) {
		_, err := bucket.HeadObject(ctx, "test-key")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, storage.ErrTooManyRequests) {
			t.Errorf("expected ErrTooManyRequests, got: %v", err)
		}
		if !strings.Contains(err.Error(), "429") {
			t.Errorf("expected error message to contain status code 429, got: %v", err)
		}
	})

	// Test DeleteObject returns ErrTooManyRequests
	t.Run("DeleteObject", func(t *testing.T) {
		err := bucket.DeleteObject(ctx, "test-key")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, storage.ErrTooManyRequests) {
			t.Errorf("expected ErrTooManyRequests, got: %v", err)
		}
		if !strings.Contains(err.Error(), "429") {
			t.Errorf("expected error message to contain status code 429, got: %v", err)
		}
	})

	// Test ListObjects returns ErrTooManyRequests
	t.Run("ListObjects", func(t *testing.T) {
		for _, err := range bucket.ListObjects(ctx) {
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !errors.Is(err, storage.ErrTooManyRequests) {
				t.Errorf("expected ErrTooManyRequests, got: %v", err)
			}
			if !strings.Contains(err.Error(), "429") {
				t.Errorf("expected error message to contain status code 429, got: %v", err)
			}
			break // Only check the first error
		}
	})

	// Test Access/Create returns ErrTooManyRequests
	t.Run("Access", func(t *testing.T) {
		err := bucket.Access(ctx)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !errors.Is(err, storage.ErrTooManyRequests) {
			t.Errorf("expected ErrTooManyRequests, got: %v", err)
		}
		if !strings.Contains(err.Error(), "429") {
			t.Errorf("expected error message to contain status code 429, got: %v", err)
		}
	})
}
