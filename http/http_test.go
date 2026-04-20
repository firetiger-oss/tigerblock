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
// Range headers for both closed ranges (bytes=N-M) and open-ended ranges
// (bytes=N-) used for tail reads.
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
		{
			name:          "open-ended range from start",
			start:         0,
			end:           -1,
			expectedRange: "bytes=0-",
		},
		{
			name:          "open-ended range from offset",
			start:         5,
			end:           -1,
			expectedRange: "bytes=5-",
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

// TestHTTPStorageTranslates416 verifies that a 416 Range Not Satisfiable
// response (returned when the requested start is past the end of the
// object) is translated into an empty reader with the object size parsed
// from the Content-Range response header. This removes the need for a
// preliminary HEAD request by downstream callers doing tail reads.
func TestHTTPStorageTranslates416(t *testing.T) {
	const size = int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	t.Cleanup(server.Close)

	bucket := storagehttp.NewBucket(server.URL)
	r, info, err := bucket.GetObject(t.Context(), "test-key", storage.BytesRange(size, -1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer r.Close()
	if info.Size != size {
		t.Errorf("ObjectInfo.Size = %d, want %d", info.Size, size)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if len(b) != 0 {
		t.Errorf("expected empty body, got %q", b)
	}
}

// TestHTTPServerReturns416ForStartPastEnd verifies that when a client
// requests a range whose start is past the end of the object, the HTTP
// server responds with 416 and a "bytes */size" Content-Range header
// (matching real S3) so the client can translate it into an empty reader.
func TestHTTPServerReturns416ForStartPastEnd(t *testing.T) {
	mem := new(memory.Bucket)
	if _, err := mem.PutObject(t.Context(), "obj", strings.NewReader("abcdefghij")); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(storagehttp.BucketHandler(mem))
	t.Cleanup(server.Close)

	req, err := http.NewRequest(http.MethodGet, server.URL+"/obj", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=100-")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("status = %d, want 416", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes */10" {
		t.Errorf("Content-Range = %q, want %q", got, "bytes */10")
	}
}

// TestHTTPServerTranscodedRangeNotRejected guards against using the
// stored (compressed) ObjectInfo.Size for 416 detection when the
// backend serves transcoded content. A GCS object with
// Content-Encoding: gzip reports its compressed size in ObjectInfo,
// but the reader the bucket returns is the decompressed body. A
// client request whose start is past the compressed size can still
// be a valid read — the server must not pre-emptively emit 416 based
// on the compressed size.
func TestHTTPServerTranscodedRangeNotRejected(t *testing.T) {
	body := strings.Repeat("decompressed body ", 100) // 1800 bytes
	backend := &transcodedBucket{
		info: storage.ObjectInfo{
			Size:            60, // pretend stored (compressed) size
			ContentEncoding: "gzip",
			ContentType:     "text/plain",
		},
		body: body,
	}

	server := httptest.NewServer(storagehttp.BucketHandler(backend))
	t.Cleanup(server.Close)

	req, err := http.NewRequest(http.MethodGet, server.URL+"/transcoded", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Offset past the compressed size but within the decompressed body.
	req.Header.Set("Range", "bytes=100-")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("server returned 416 for a range whose backend reader is non-empty; should stream the body instead")
	}
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) == 0 {
		t.Fatalf("expected non-empty body from transcoded backend, got %d bytes", len(got))
	}
}

// transcodedBucket simulates a gs-style transcoded GET: ObjectInfo
// reports the compressed size, the body is the decompressed content.
type transcodedBucket struct {
	storage.Bucket
	info storage.ObjectInfo
	body string
}

func (b *transcodedBucket) Location() string { return "mock://transcoded" }

func (b *transcodedBucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	return b.info, nil
}

func (b *transcodedBucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	getOptions := storage.NewGetOptions(options...)
	body := b.body
	if start, end, ok := getOptions.BytesRange(); ok {
		_ = end
		// Mimic gs behaviour: the reader carries a slice of the
		// decompressed body starting at `start`, regardless of the
		// stored compressed size.
		if start >= int64(len(body)) {
			body = ""
		} else {
			body = body[start:]
		}
	}
	return io.NopCloser(strings.NewReader(body)), b.info, nil
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

// TestHTTPStorageSpecialCharacterKeys verifies that object keys with special
// characters round-trip correctly through the full HTTP client → server → memory
// backend stack. The keys stored in the backing memory bucket must match the
// original keys exactly, with no percent-encoding artifacts.
func TestHTTPStorageSpecialCharacterKeys(t *testing.T) {
	backend := new(memory.Bucket)
	ctx := t.Context()

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	location := "http://" + l.Addr().String()

	s := &http.Server{
		Handler: storagehttp.BucketHandler(backend,
			storagehttp.WithLocation(location),
			storagehttp.WithMaxKeys(100),
		),
	}
	go s.Serve(l)
	t.Cleanup(func() {
		s.Close()
		l.Close()
	})

	bucket := storagehttp.NewBucket(location)

	testKeys := []string{
		"hello world",
		"path/with spaces/file",
		"100%done",
		"file+name",
		"file(1).txt",
		"caf\u00e9/menu",
		"foo%2Fbar", // literal %2F in key name (not a slash)
		"my%20file", // literal %20 in key name (not a space)
		"foo%25bar", // literal %25 in key name (not a percent)
	}

	for _, key := range testKeys {
		t.Run(key, func(t *testing.T) {
			content := "content for " + key

			// PutObject through the HTTP client
			_, err := bucket.PutObject(ctx, key, strings.NewReader(content))
			if err != nil {
				t.Fatalf("PutObject(%q) failed: %v", key, err)
			}

			// Verify the key in the backing memory bucket is not mangled
			reader, _, err := backend.GetObject(ctx, key)
			if err != nil {
				t.Fatalf("backend.GetObject(%q) failed (key may be stored with wrong encoding): %v", key, err)
			}
			body, _ := io.ReadAll(reader)
			reader.Close()
			if string(body) != content {
				t.Fatalf("backend content mismatch for key %q: got %q, want %q", key, body, content)
			}

			// HeadObject
			info, err := bucket.HeadObject(ctx, key)
			if err != nil {
				t.Fatalf("HeadObject(%q) failed: %v", key, err)
			}
			if info.Size != int64(len(content)) {
				t.Errorf("HeadObject(%q) size = %d, want %d", key, info.Size, len(content))
			}

			// GetObject
			reader, _, err = bucket.GetObject(ctx, key)
			if err != nil {
				t.Fatalf("GetObject(%q) failed: %v", key, err)
			}
			body, _ = io.ReadAll(reader)
			reader.Close()
			if string(body) != content {
				t.Errorf("GetObject(%q) body = %q, want %q", key, body, content)
			}

			// CopyObject
			copyKey := key + "-copy"
			if err := bucket.CopyObject(ctx, key, copyKey); err != nil {
				t.Fatalf("CopyObject(%q, %q) failed: %v", key, copyKey, err)
			}
			reader, _, err = backend.GetObject(ctx, copyKey)
			if err != nil {
				t.Fatalf("backend.GetObject(%q) after copy failed: %v", copyKey, err)
			}
			body, _ = io.ReadAll(reader)
			reader.Close()
			if string(body) != content {
				t.Errorf("CopyObject content mismatch for %q: got %q, want %q", copyKey, body, content)
			}

			// DeleteObject
			if err := bucket.DeleteObject(ctx, key); err != nil {
				t.Fatalf("DeleteObject(%q) failed: %v", key, err)
			}
			_, err = backend.HeadObject(ctx, key)
			if !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("after DeleteObject(%q), backend still has object: %v", key, err)
			}

			// Clean up copy
			backend.DeleteObject(ctx, copyKey)
		})
	}

	// Test that ListObjects returns correctly decoded keys
	t.Run("ListObjects", func(t *testing.T) {
		for _, key := range testKeys {
			if _, err := bucket.PutObject(ctx, key, strings.NewReader("x")); err != nil {
				t.Fatalf("PutObject(%q) failed: %v", key, err)
			}
		}

		var listedKeys []string
		for obj, err := range bucket.ListObjects(ctx) {
			if err != nil {
				t.Fatalf("ListObjects failed: %v", err)
			}
			listedKeys = append(listedKeys, obj.Key)
		}

		if len(listedKeys) != len(testKeys) {
			t.Fatalf("ListObjects returned %d keys, want %d: %v", len(listedKeys), len(testKeys), listedKeys)
		}

		for _, key := range testKeys {
			found := false
			for _, listed := range listedKeys {
				if listed == key {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("ListObjects missing key %q; got %v", key, listedKeys)
			}
		}

		// Clean up
		for _, key := range testKeys {
			backend.DeleteObject(ctx, key)
		}
	})
}

// TestHTTPServerKeyDecodingRaw verifies server-side key extraction with raw HTTP
// requests, independent of the Go client. This catches issues where client and
// server changes mask each other.
func TestHTTPServerKeyDecodingRaw(t *testing.T) {
	backend := new(memory.Bucket)
	ctx := t.Context()

	server := httptest.NewServer(storagehttp.BucketHandler(backend))
	t.Cleanup(server.Close)

	client := &http.Client{}

	// Each case maps a raw URL path to the expected key in the backend.
	// The URL path is what an HTTP client would send on the wire.
	testCases := []struct {
		name        string
		urlPath     string // raw URL path (percent-encoded as needed)
		expectedKey string // key that should appear in backend
	}{
		{
			name:        "space encoded as %20",
			urlPath:     "/hello%20world",
			expectedKey: "hello world",
		},
		{
			name:        "nested path with encoded spaces",
			urlPath:     "/path/with%20spaces/file",
			expectedKey: "path/with spaces/file",
		},
		{
			name:        "literal percent via double encoding %25",
			urlPath:     "/100%25done",
			expectedKey: "100%done",
		},
		{
			name:        "literal %2F via double encoding %252F",
			urlPath:     "/foo%252Fbar",
			expectedKey: "foo%2Fbar",
		},
		{
			name:        "literal %20 via double encoding %2520",
			urlPath:     "/my%2520file",
			expectedKey: "my%20file",
		},
		{
			name:        "normal hierarchical path",
			urlPath:     "/foo/bar/baz",
			expectedKey: "foo/bar/baz",
		},
		{
			name:        "plus sign preserved",
			urlPath:     "/file+name",
			expectedKey: "file+name",
		},
		{
			name:        "unicode encoded",
			urlPath:     "/caf%C3%A9/menu",
			expectedKey: "caf\u00e9/menu",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// PUT via raw request
			content := "content for " + tc.expectedKey
			req, err := http.NewRequest("PUT", server.URL+tc.urlPath, strings.NewReader(content))
			if err != nil {
				t.Fatal(err)
			}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("PUT %s: status %d", tc.urlPath, resp.StatusCode)
			}

			// Verify the key in the backend
			reader, _, err := backend.GetObject(ctx, tc.expectedKey)
			if err != nil {
				// List all keys to help diagnose
				var keys []string
				for obj, _ := range backend.ListObjects(ctx) {
					keys = append(keys, obj.Key)
				}
				t.Fatalf("backend.GetObject(%q) failed: %v (backend has keys: %v)", tc.expectedKey, err, keys)
			}
			body, _ := io.ReadAll(reader)
			reader.Close()
			if string(body) != content {
				t.Errorf("backend content = %q, want %q", body, content)
			}

			// GET via raw request
			req, _ = http.NewRequest("GET", server.URL+tc.urlPath, nil)
			resp, err = client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("GET %s: status %d", tc.urlPath, resp.StatusCode)
			}
			if string(body) != content {
				t.Errorf("GET body = %q, want %q", body, content)
			}

			// DELETE via raw request
			req, _ = http.NewRequest("DELETE", server.URL+tc.urlPath, nil)
			resp, err = client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("DELETE %s: status %d", tc.urlPath, resp.StatusCode)
			}

			_, err = backend.HeadObject(ctx, tc.expectedKey)
			if !errors.Is(err, storage.ErrObjectNotFound) {
				t.Errorf("after DELETE, backend still has key %q: %v", tc.expectedKey, err)
			}
		})
	}

	// Test CopyObject with raw request and URL-encoded copy source
	t.Run("CopyObjectRaw", func(t *testing.T) {
		_, err := backend.PutObject(ctx, "hello world", strings.NewReader("copy me"))
		if err != nil {
			t.Fatal(err)
		}

		// Copy via raw PUT with X-Amz-Copy-Source header (URL-encoded source key)
		req, _ := http.NewRequest("PUT", server.URL+"/hello%20world-copy", nil)
		req.Header.Set("X-Amz-Copy-Source", "//hello%20world") // bucket is empty for memory
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("COPY status %d: %s", resp.StatusCode, body)
		}

		// Verify copy landed with correct key
		reader, _, err := backend.GetObject(ctx, "hello world-copy")
		if err != nil {
			t.Fatalf("backend.GetObject(\"hello world-copy\") failed: %v", err)
		}
		body, _ := io.ReadAll(reader)
		reader.Close()
		if string(body) != "copy me" {
			t.Errorf("copy content = %q, want %q", body, "copy me")
		}
	})
}
