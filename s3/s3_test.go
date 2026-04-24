package s3_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/firetiger-oss/storage"
	storages3 "github.com/firetiger-oss/storage/s3"
	"github.com/firetiger-oss/storage/s3/fakes3"
	storagetest "github.com/firetiger-oss/storage/test"
)

// TestS3GetObjectTranslates416 verifies that a 416 Range Not Satisfiable
// response (returned by S3 when the requested start is past the end of the
// object) is translated into an empty reader with the object size parsed
// from the Content-Range response header. This removes the need for a
// preliminary HEAD request by downstream callers doing tail reads.
func TestS3GetObjectTranslates416(t *testing.T) {
	const size = int64(100)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	}))
	defer server.Close()

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		config.WithHTTPClient(awshttp.NewBuildableClient()),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(u.String())
		o.UsePathStyle = true
	})
	bucket := storages3.NewBucket(client, "test-bucket")

	r, info, err := bucket.GetObject(t.Context(), "test-key", storage.BytesRange(size, -1))
	if err != nil {
		var respErr *smithyhttp.ResponseError
		if errors.As(err, &respErr) {
			t.Fatalf("expected empty reader + nil error, got HTTP %d: %v", respErr.HTTPStatusCode(), err)
		}
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

func TestS3(t *testing.T) {
	storagetest.TestStorage(t, func(*testing.T) (storage.Bucket, error) {
		bucket := "test"
		client := fakes3.NewClient(bucket)
		return storages3.NewBucket(client, bucket), nil
	})
}

// mockRangeClient captures GetObject inputs for testing Range header formatting.
type mockRangeClient struct {
	storages3.Client
	capturedRange string
}

func (m *mockRangeClient) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if params.Range != nil {
		m.capturedRange = aws.ToString(params.Range)
	}
	return &s3.GetObjectOutput{
		Body: io.NopCloser(strings.NewReader("test")),
	}, nil
}

// TestS3RangeHeaderFormat verifies that the S3 client correctly formats
// Range headers for both closed ranges (bytes=N-M) and open-ended ranges
// (bytes=N-) used for tail reads.
func TestS3RangeHeaderFormat(t *testing.T) {
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
			mock := &mockRangeClient{}
			bucket := storages3.NewBucket(mock, "test-bucket")

			_, _, err := bucket.GetObject(t.Context(), "test-key", storage.BytesRange(tc.start, tc.end))
			if err != nil {
				t.Fatalf("GetObject failed: %v", err)
			}

			if mock.capturedRange != tc.expectedRange {
				t.Errorf("expected Range %q, got %q", tc.expectedRange, mock.capturedRange)
			}
		})
	}
}

func TestS3PutObjectKnownLengthNonSeekableBodyStaysSinglePart(t *testing.T) {
	var (
		putCount       int
		multipartCalls []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if _, ok := q["uploads"]; ok {
			multipartCalls = append(multipartCalls, "CreateMultipartUpload")
			http.Error(w, "multipart not expected", http.StatusMethodNotAllowed)
			return
		}
		if q.Get("uploadId") != "" {
			multipartCalls = append(multipartCalls, r.Method+" uploadId="+q.Get("uploadId"))
			http.Error(w, "multipart not expected", http.StatusMethodNotAllowed)
			return
		}
		if r.Method != http.MethodPut {
			http.Error(w, "unexpected method "+r.Method, http.StatusMethodNotAllowed)
			return
		}
		putCount++
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("ETag", `"deadbeef"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("AKID", "SECRET", "")),
		config.WithHTTPClient(awshttp.NewBuildableClient()),
	)
	if err != nil {
		t.Fatal(err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(u.String())
		o.UsePathStyle = true
	})
	bucket := storages3.NewBucket(client, "test-bucket")

	payload := bytes.Repeat([]byte("abcdef"), 1024*1024)
	sum := sha256.Sum256(payload)
	body := io.NopCloser(bytes.NewReader(payload))

	if _, seekable := any(body).(io.Seeker); seekable {
		t.Fatalf("io.NopCloser(bytes.NewReader(...)) unexpectedly implements io.Seeker")
	}

	if _, err := bucket.PutObject(
		t.Context(),
		"test-key",
		body,
		storage.ContentLength(int64(len(payload))),
		storage.ChecksumSHA256(sum),
	); err != nil {
		t.Fatalf("PutObject with non-seekable body failed: %v", err)
	}

	if len(multipartCalls) != 0 {
		t.Fatalf("PutObject unexpectedly went multipart: %v", multipartCalls)
	}
	if putCount != 1 {
		t.Fatalf("expected exactly 1 PutObject request, got %d", putCount)
	}
}
