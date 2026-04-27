package serve_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/firetiger-oss/tigerblock/notification"
	"github.com/firetiger-oss/tigerblock/notification/serve"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
)

func TestServeHTTPMode(t *testing.T) {
	bucket := memory.NewBucket()
	_, err := bucket.PutObject(t.Context(), "test.txt",
		strings.NewReader("hello world"),
		storage.ContentType("text/plain"),
	)
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	var receivedBody atomic.Value
	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		receivedBody.Store(string(body))
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))

	// Find an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Start server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- serve.Serve(objectHandler,
			serve.WithPort(strconv.Itoa(port)),
			serve.WithHealthPath("/health"),
		)
	}()

	// Wait for server to start
	baseURL := "http://127.0.0.1:" + strconv.Itoa(port)
	waitForServer(t, baseURL+"/health", 2*time.Second)

	t.Run("health endpoint", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/health")
		if err != nil {
			t.Fatalf("health check failed: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
	})

	t.Run("AWS EventBridge endpoint", func(t *testing.T) {
		payload := `{
			"version": "0",
			"id": "test-id",
			"detail-type": "Object Created",
			"source": "aws.s3",
			"account": "123456789012",
			"time": "2025-01-15T10:30:00Z",
			"region": "us-west-2",
			"detail": {
				"bucket": {"name": "my-bucket"},
				"object": {"key": "test.txt", "size": 11}
			}
		}`

		resp, err := http.Post(baseURL+"/aws", "application/json", bytes.NewReader([]byte(payload)))
		if err != nil {
			t.Fatalf("AWS endpoint failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("expected status 200, got %d: %s", resp.StatusCode, body)
		}

		if got := receivedBody.Load(); got != "hello world" {
			t.Errorf("expected body 'hello world', got '%v'", got)
		}
	})

	t.Run("GCP Pub/Sub endpoint", func(t *testing.T) {
		// GCP Pub/Sub uses base64-encoded data
		objectData := base64.StdEncoding.EncodeToString([]byte(`{"name":"test.txt","bucket":"my-bucket","size":"11"}`))
		payload := `{
			"message": {
				"data": "` + objectData + `",
				"attributes": {
					"bucketId": "my-bucket",
					"objectId": "test.txt",
					"eventType": "OBJECT_FINALIZE"
				},
				"messageId": "123",
				"publishTime": "2025-01-15T10:30:00Z"
			},
			"subscription": "projects/test/subscriptions/test-sub"
		}`

		resp, err := http.Post(baseURL+"/gcp", "application/json", bytes.NewReader([]byte(payload)))
		if err != nil {
			t.Fatalf("GCP endpoint failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("expected status 200, got %d: %s", resp.StatusCode, body)
		}
	})

	t.Run("Cloudflare Queues endpoint", func(t *testing.T) {
		payload := `{
			"account": "account-id",
			"action": "PutObject",
			"bucket": "my-bucket",
			"object": {"key": "test.txt", "size": 11, "eTag": "abc123"},
			"eventTime": "2025-01-15T10:30:00Z"
		}`

		resp, err := http.Post(baseURL+"/cloudflare", "application/json", bytes.NewReader([]byte(payload)))
		if err != nil {
			t.Fatalf("Cloudflare endpoint failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("expected status 200, got %d: %s", resp.StatusCode, body)
		}
	})

	t.Run("method not allowed", func(t *testing.T) {
		resp, err := http.Get(baseURL + "/aws")
		if err != nil {
			t.Fatalf("GET request failed: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("expected status 405, got %d", resp.StatusCode)
		}
	})
}

func TestServeOptionsDefaults(t *testing.T) {
	// Test that custom paths work
	bucket := memory.NewBucket()
	_, err := bucket.PutObject(t.Context(), "test.txt",
		strings.NewReader("test"),
		storage.ContentType("text/plain"),
	)
	if err != nil {
		t.Fatalf("failed to put object: %v", err)
	}

	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	go func() {
		serve.Serve(objectHandler,
			serve.WithPort(strconv.Itoa(port)),
			serve.WithAWSPath("/custom-aws"),
			serve.WithGCPPath("/custom-gcp"),
			serve.WithCloudflarePath("/custom-cf"),
			serve.WithHealthPath("/custom-health"),
		)
	}()

	baseURL := "http://127.0.0.1:" + strconv.Itoa(port)
	waitForServer(t, baseURL+"/custom-health", 2*time.Second)

	// Verify custom health path works
	resp, err := http.Get(baseURL + "/custom-health")
	if err != nil {
		t.Fatalf("custom health check failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	// Verify custom AWS path works
	payload := `{
		"version": "0",
		"detail-type": "Object Created",
		"source": "aws.s3",
		"detail": {
			"bucket": {"name": "my-bucket"},
			"object": {"key": "test.txt"}
		}
	}`
	resp, err = http.Post(baseURL+"/custom-aws", "application/json", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatalf("custom AWS endpoint failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Errorf("expected status 200, got %d: %s", resp.StatusCode, body)
	}
}

func TestServeDisabledHealth(t *testing.T) {
	bucket := memory.NewBucket()
	registry := storage.RegistryFunc(func(ctx context.Context, uri string) (storage.Bucket, error) {
		return bucket, nil
	})

	httpHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	objectHandler := notification.NewObjectHandler(httpHandler, notification.WithRegistry(registry))

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	go func() {
		serve.Serve(objectHandler,
			serve.WithPort(strconv.Itoa(port)),
			serve.WithHealthPath(""), // Disable health endpoint
		)
	}()

	baseURL := "http://127.0.0.1:" + strconv.Itoa(port)

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	// Health endpoint should not exist
	resp, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	// Should get 404 Not Found since health is disabled
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", resp.StatusCode)
	}
}

func waitForServer(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server did not start within %v", timeout)
}
