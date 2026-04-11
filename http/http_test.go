package http_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"strings"
	"testing"
	"time"

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

// TestHTTPStorageRangeHeaderFormat verifies that the HTTP client correctly formats
// Range headers for closed ranges (bytes=0-100).
func TestHTTPStorageRangeHeaderFormat(t *testing.T) {
	tests := []struct {
		name          string
		start         int64
		end           int64
		expectedRange string
	}{
		{
			name:          "closed range from start",
			start:         0,
			end:           100,
			expectedRange: "bytes=0-100",
		},
		{
			name:          "closed range with offset",
			start:         512,
			end:           1023,
			expectedRange: "bytes=512-1023",
		},
		{
			name:          "single byte range",
			start:         42,
			end:           42,
			expectedRange: "bytes=42-42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedRange string

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedRange = r.Header.Get("Range")
				w.WriteHeader(http.StatusPartialContent)
				w.Write([]byte("test content"))
			}))
			t.Cleanup(server.Close)

			bucket := storagehttp.NewBucket(server.URL)
			_, _, err := bucket.GetObject(t.Context(), "test-key", storage.BytesRange(tc.start, tc.end))
			if err != nil {
				t.Fatalf("GetObject failed: %v", err)
			}

			if capturedRange != tc.expectedRange {
				t.Errorf("expected Range header %q, got %q", tc.expectedRange, capturedRange)
			}
		})
	}
}

// TestHTTPServerOpenEndedRangeFormat verifies that when the HTTP server receives
// an open-ended Range request (bytes=N-) and forwards it via PresignGetObject,
// the Range header is correctly formatted as "bytes=N-" instead of the invalid "bytes=N--1".
//
// This tests the server-side handling where parseBytesRange sets end=-1 for open-ended
// ranges, and the bucket's Range formatting must handle this correctly.
func TestHTTPServerOpenEndedRangeFormat(t *testing.T) {
	tests := []struct {
		name           string
		requestRange   string
		expectedFormat string
	}{
		{
			name:           "open-ended from start",
			requestRange:   "bytes=0-",
			expectedFormat: "bytes=0-",
		},
		{
			name:           "open-ended with offset",
			requestRange:   "bytes=1024-",
			expectedFormat: "bytes=1024-",
		},
		{
			name:           "closed range",
			requestRange:   "bytes=0-100",
			expectedFormat: "bytes=0-100",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var capturedRange string

			// Create a mock backend that captures presign requests
			backend := &mockPresignBucket{
				onPresignGetObject: func(rangeHeader string) {
					capturedRange = rangeHeader
				},
			}

			server := httptest.NewServer(storagehttp.BucketHandler(backend))
			t.Cleanup(server.Close)

			req, err := http.NewRequest("GET", server.URL+"/test-key", nil)
			if err != nil {
				t.Fatalf("failed to create request: %v", err)
			}
			req.Header.Set("Range", tc.requestRange)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			resp.Body.Close()

			if capturedRange != tc.expectedFormat {
				t.Errorf("expected Range %q, got %q", tc.expectedFormat, capturedRange)
			}
		})
	}
}

// mockPresignBucket is a bucket that returns ErrPresignRedirect from GetObject,
// triggering the presign path in the server handler.
type mockPresignBucket struct {
	storage.Bucket
	onPresignGetObject func(rangeHeader string)
}

func (m *mockPresignBucket) Location() string {
	return "mock://bucket"
}

func (m *mockPresignBucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	return nil, storage.ObjectInfo{}, storage.ErrPresignRedirect
}

func (m *mockPresignBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	opts := storage.NewGetOptions(options...)
	if start, end, ok := opts.BytesRange(); ok {
		var rangeHeader string
		if end >= 0 {
			rangeHeader = fmt.Sprintf("bytes=%d-%d", start, end)
		} else {
			rangeHeader = fmt.Sprintf("bytes=%d-", start)
		}
		if m.onPresignGetObject != nil {
			m.onPresignGetObject(rangeHeader)
		}
	}
	return "https://example.com/presigned", nil
}

// TestHTTPStorageWithPathInURL verifies that loading an HTTP bucket with a path
// in the URL (e.g., http://host/v1/path/to/endpoint) correctly routes all
// operations through the full URL path. This was a bug where loadBucket stripped
// the path and used WithPrefix instead, which broke ListObjects by sending
// ?prefix=v1%2Fpath%2F to the root instead of listing at the full URL path.
func TestHTTPStorageWithPathInURL(t *testing.T) {
	backend := new(memory.Bucket)
	ctx := t.Context()

	// Put some objects in the backend
	for _, key := range []string{"file1.txt", "file2.txt", "dir/file3.txt"} {
		if _, err := backend.PutObject(ctx, key, strings.NewReader("content-"+key)); err != nil {
			t.Fatal(err)
		}
	}

	// Start an HTTP server that serves the bucket under a sub-path
	const pathPrefix = "/v1/agents/test/artifacts"
	mux := http.NewServeMux()
	mux.Handle(pathPrefix+"/", http.StripPrefix(pathPrefix,
		storagehttp.BucketHandler(backend, storagehttp.WithMaxKeys(10)),
	))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	// Load the bucket using the full URL with path
	bucket, err := storage.LoadBucket(ctx, server.URL+pathPrefix)
	if err != nil {
		t.Fatal(err)
	}

	// HeadObject should work (sends HEAD to full URL path + key)
	t.Run("HeadObject", func(t *testing.T) {
		info, err := bucket.HeadObject(ctx, "file1.txt")
		if err != nil {
			t.Fatalf("HeadObject failed: %v", err)
		}
		if info.Size != int64(len("content-file1.txt")) {
			t.Errorf("expected size %d, got %d", len("content-file1.txt"), info.Size)
		}
	})

	// GetObject should work (sends GET to full URL path + key)
	t.Run("GetObject", func(t *testing.T) {
		r, _, err := bucket.GetObject(ctx, "file1.txt")
		if err != nil {
			t.Fatalf("GetObject failed: %v", err)
		}
		defer r.Close()
		data, _ := io.ReadAll(r)
		if string(data) != "content-file1.txt" {
			t.Errorf("expected 'content-file1.txt', got %q", data)
		}
	})

	// ListObjects should work (sends GET to full URL path with query params,
	// NOT to root with ?prefix=v1%2Fagents%2F...)
	t.Run("ListObjects", func(t *testing.T) {
		var keys []string
		for obj, err := range bucket.ListObjects(ctx) {
			if err != nil {
				t.Fatalf("ListObjects failed: %v", err)
			}
			keys = append(keys, obj.Key)
		}
		if len(keys) != 3 {
			t.Errorf("expected 3 objects, got %d: %v", len(keys), keys)
		}
	})

	// PutObject should work (sends PUT to full URL path + key)
	t.Run("PutObject", func(t *testing.T) {
		_, err := bucket.PutObject(ctx, "new-file.txt", strings.NewReader("new content"))
		if err != nil {
			t.Fatalf("PutObject failed: %v", err)
		}
		// Verify via backend
		r, _, err := backend.GetObject(ctx, "new-file.txt")
		if err != nil {
			t.Fatalf("backend GetObject failed: %v", err)
		}
		defer r.Close()
		data, _ := io.ReadAll(r)
		if string(data) != "new content" {
			t.Errorf("expected 'new content', got %q", data)
		}
	})

	// DeleteObject should work
	t.Run("DeleteObject", func(t *testing.T) {
		err := bucket.DeleteObject(ctx, "file2.txt")
		if err != nil {
			t.Fatalf("DeleteObject failed: %v", err)
		}
		_, err = backend.HeadObject(ctx, "file2.txt")
		if !errors.Is(err, storage.ErrObjectNotFound) {
			t.Errorf("expected ErrObjectNotFound after delete, got: %v", err)
		}
	})
}

// TestHTTPServerEscapedPaths verifies that the HTTP server correctly preserves
// percent-encoded characters in URL paths, preventing automatic unescaping.
// This validates the fix where makeKey() uses r.URL.EscapedPath() instead of r.URL.Path.
//
// The fix ensures that escaped paths are not mistakenly unescaped, which could lead to:
// 1. Security issues (e.g., path traversal with ..%2F)
// 2. Incorrect object key lookups when keys contain percent-encoded sequences
func TestHTTPServerEscapedPaths(t *testing.T) {
	// Create a memory bucket backend
	backend := new(memory.Bucket)
	ctx := t.Context()

	// Test cases where percent-encoding should be preserved in the object key
	testCases := []struct {
		name        string
		objectKey   string // The actual object key to store (with percent encoding preserved)
		urlPath     string // The URL path to use in HTTP requests
		content     string
		description string
	}{
		{
			name:        "escaped slash preserved",
			objectKey:   "foo%2Fbar",  // Key contains literal %2F
			urlPath:     "/foo%2Fbar", // URL with %2F
			content:     "slash content",
			description: "Escaped slash %2F should be preserved in the key, not decoded to /",
		},
		{
			name:        "escaped space preserved",
			objectKey:   "my%20file",  // Key contains literal %20
			urlPath:     "/my%20file", // URL with %20
			content:     "space content",
			description: "Escaped space %20 should be preserved in the key, not decoded to space",
		},
		{
			name:        "escaped percent preserved",
			objectKey:   "foo%25bar",  // Key contains literal %25
			urlPath:     "/foo%25bar", // URL with %25
			content:     "percent content",
			description: "Escaped percent %25 should be preserved in the key, not decoded to %",
		},
		{
			name:        "double dot slash prevented",
			objectKey:   "safe%2F..%2Fetc",  // Key contains escaped path traversal attempt
			urlPath:     "/safe%2F..%2Fetc", // URL with escaped ../
			content:     "security content",
			description: "Escaped path traversal sequences should stay escaped",
		},
		{
			name:        "normal unescaped path",
			objectKey:   "foo/bar/baz",  // Normal hierarchical key
			urlPath:     "/foo/bar/baz", // Normal path
			content:     "normal content",
			description: "Normal unescaped paths should work as before",
		},
	}

	// Create test server
	server := httptest.NewServer(storagehttp.BucketHandler(backend))
	t.Cleanup(func() {
		server.Close()
	})

	client := &http.Client{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// First, put the object directly into the backend
			_, err := backend.PutObject(ctx, tc.objectKey, strings.NewReader(tc.content))
			if err != nil {
				t.Fatalf("failed to put object in backend: %v", err)
			}

			// Test HEAD request
			t.Run("HEAD", func(t *testing.T) {
				req, err := http.NewRequest("HEAD", server.URL+tc.urlPath, nil)
				if err != nil {
					t.Fatalf("failed to create HEAD request: %v", err)
				}

				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("HEAD request failed: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("HEAD request failed: expected status 200, got %d", resp.StatusCode)
				}

				// Check Content-Length header
				if resp.ContentLength != int64(len(tc.content)) {
					t.Errorf("HEAD request: expected Content-Length %d, got %d", len(tc.content), resp.ContentLength)
				}
			})

			// Test GET request
			t.Run("GET", func(t *testing.T) {
				req, err := http.NewRequest("GET", server.URL+tc.urlPath, nil)
				if err != nil {
					t.Fatalf("failed to create GET request: %v", err)
				}

				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("GET request failed: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					t.Errorf("GET request failed: expected status 200, got %d", resp.StatusCode)
				}

				body, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatalf("failed to read response body: %v", err)
				}

				if string(body) != tc.content {
					t.Errorf("GET request: expected body %q, got %q", tc.content, string(body))
				}
			})

			// Test PUT request (update the object)
			t.Run("PUT", func(t *testing.T) {
				newContent := tc.content + " updated"
				req, err := http.NewRequest("PUT", server.URL+tc.urlPath, strings.NewReader(newContent))
				if err != nil {
					t.Fatalf("failed to create PUT request: %v", err)
				}

				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("PUT request failed: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
					t.Errorf("PUT request failed: expected status 200 or 201, got %d", resp.StatusCode)
				}

				// Verify the update by reading from backend
				reader, _, err := backend.GetObject(ctx, tc.objectKey)
				if err != nil {
					t.Fatalf("failed to get updated object from backend: %v", err)
				}
				defer reader.Close()

				body, err := io.ReadAll(reader)
				if err != nil {
					t.Fatalf("failed to read updated object: %v", err)
				}

				if string(body) != newContent {
					t.Errorf("PUT request: expected updated content %q, got %q", newContent, string(body))
				}
			})

			// Test DELETE request
			t.Run("DELETE", func(t *testing.T) {
				req, err := http.NewRequest("DELETE", server.URL+tc.urlPath, nil)
				if err != nil {
					t.Fatalf("failed to create DELETE request: %v", err)
				}

				resp, err := client.Do(req)
				if err != nil {
					t.Fatalf("DELETE request failed: %v", err)
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
					t.Errorf("DELETE request failed: expected status 200 or 204, got %d", resp.StatusCode)
				}

				// Verify deletion by checking if object exists in backend
				_, err = backend.HeadObject(ctx, tc.objectKey)
				if !errors.Is(err, storage.ErrObjectNotFound) {
					t.Errorf("DELETE request: object should not exist, got error: %v", err)
				}
			})

			// Clean up: delete the object if it still exists
			t.Cleanup(func() {
				backend.DeleteObject(ctx, tc.objectKey)
			})
		})
	}
}
