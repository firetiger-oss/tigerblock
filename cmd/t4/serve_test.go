package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseBucketArgs(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		wantSpecs []bucketSpec
		wantErr   string
	}{
		{
			name:      "single unnamed",
			args:      []string{"file:///tmp/a"},
			wantSpecs: []bucketSpec{{uri: "file:///tmp/a"}},
		},
		{
			name:      "single named",
			args:      []string{"a=file:///tmp/a"},
			wantSpecs: []bucketSpec{{name: "a", uri: "file:///tmp/a"}},
		},
		{
			name: "multiple named",
			args: []string{"a=file:///tmp/a", "b=file:///tmp/b"},
			wantSpecs: []bucketSpec{
				{name: "a", uri: "file:///tmp/a"},
				{name: "b", uri: "file:///tmp/b"},
			},
		},
		{
			name:    "no args",
			args:    nil,
			wantErr: "at least one bucket argument",
		},
		{
			name:    "mixed named and unnamed",
			args:    []string{"a=file:///tmp/a", "file:///tmp/b"},
			wantErr: "all bucket arguments must be of the form name=uri",
		},
		{
			name:    "multiple unnamed",
			args:    []string{"file:///tmp/a", "file:///tmp/b"},
			wantErr: "all bucket arguments must be of the form name=uri",
		},
		{
			name:    "empty rhs",
			args:    []string{"a="},
			wantErr: "invalid bucket argument",
		},
		{
			// `=file:///x` has no LHS that matches the bucket-name
			// regex, so it falls through to positional treatment
			// rather than erroring. Storage layer rejects it later.
			name:      "leading equals",
			args:      []string{"=file:///tmp/a"},
			wantSpecs: []bucketSpec{{uri: "=file:///tmp/a"}},
		},
		{
			// LHS contains `/` so it cannot be a bucket name —
			// treated as a positional URI.
			name:      "slash before equals",
			args:      []string{"a/b=file:///tmp/a"},
			wantSpecs: []bucketSpec{{uri: "a/b=file:///tmp/a"}},
		},
		{
			// Codex P2 regression: a single bucket URI containing
			// literal `=` (e.g. a filesystem path) must be accepted
			// as a positional unnamed URI.
			name:      "uri with equals",
			args:      []string{"file:///tmp/a=b"},
			wantSpecs: []bucketSpec{{uri: "file:///tmp/a=b"}},
		},
		{
			// Codex P3 pass 2: `.` and `..` are unreachable behind
			// http.ServeMux because Chrome/Go canonicalize `/.` and
			// `/..` to `/` before routing. Reject explicitly.
			name:    "dot bucket name",
			args:    []string{".=file:///tmp/a"},
			wantErr: "invalid bucket argument",
		},
		{
			name:    "dotdot bucket name",
			args:    []string{"..=file:///tmp/a"},
			wantErr: "invalid bucket argument",
		},
		{
			name:    "duplicate name",
			args:    []string{"a=file:///tmp/a", "a=file:///tmp/b"},
			wantErr: "duplicate bucket name",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			specs, err := parseBucketArgs(tc.args)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(specs) != len(tc.wantSpecs) {
				t.Fatalf("got %d specs, want %d", len(specs), len(tc.wantSpecs))
			}
			for i, got := range specs {
				if got != tc.wantSpecs[i] {
					t.Errorf("spec[%d] = %+v, want %+v", i, got, tc.wantSpecs[i])
				}
			}
		})
	}
}

func TestBuildBucketMux(t *testing.T) {
	t.Run("named multi", func(t *testing.T) {
		ctx := t.Context()
		dirA := t.TempDir()
		dirB := t.TempDir()

		handler, err := buildBucketMux(ctx, []bucketSpec{
			{name: "a", uri: "file://" + dirA},
			{name: "b", uri: "file://" + dirB},
		})
		if err != nil {
			t.Fatalf("buildBucketMux: %v", err)
		}

		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)

		mustDo := func(t *testing.T, req *http.Request) (*http.Response, []byte) {
			t.Helper()
			res, err := server.Client().Do(req)
			if err != nil {
				t.Fatalf("request %s %s: %v", req.Method, req.URL, err)
			}
			body, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil {
				t.Fatalf("read body: %v", err)
			}
			return res, body
		}

		newReq := func(t *testing.T, method, path string, body io.Reader) *http.Request {
			t.Helper()
			req, err := http.NewRequestWithContext(ctx, method, server.URL+path, body)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			return req
		}

		// PUT /a/k1
		res, _ := mustDo(t, newReq(t, http.MethodPut, "/a/k1", strings.NewReader("foo")))
		if res.StatusCode != http.StatusOK {
			t.Fatalf("PUT /a/k1: status %d", res.StatusCode)
		}

		// GET /a/k1
		res, body := mustDo(t, newReq(t, http.MethodGet, "/a/k1", nil))
		if res.StatusCode != http.StatusOK {
			t.Fatalf("GET /a/k1: status %d", res.StatusCode)
		}
		if string(body) != "foo" {
			t.Errorf("GET /a/k1 body = %q, want %q", body, "foo")
		}

		// HEAD /a/k1
		res, _ = mustDo(t, newReq(t, http.MethodHead, "/a/k1", nil))
		if res.StatusCode != http.StatusOK {
			t.Errorf("HEAD /a/k1: status %d", res.StatusCode)
		}

		// LIST /a/?list-type=2
		res, body = mustDo(t, newReq(t, http.MethodGet, "/a/?list-type=2", nil))
		if res.StatusCode != http.StatusOK {
			t.Fatalf("LIST /a/: status %d", res.StatusCode)
		}
		if !strings.Contains(string(body), "<Key>k1</Key>") {
			t.Errorf("LIST /a/ missing k1 in body: %s", body)
		}

		// Isolation: /b/k1 must not exist
		res, body = mustDo(t, newReq(t, http.MethodGet, "/b/k1", nil))
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("GET /b/k1: status %d, want 404; body=%s", res.StatusCode, body)
		}

		// LIST /b/ empty
		res, body = mustDo(t, newReq(t, http.MethodGet, "/b/?list-type=2", nil))
		if res.StatusCode != http.StatusOK {
			t.Fatalf("LIST /b/: status %d", res.StatusCode)
		}
		if strings.Contains(string(body), "<Key>k1</Key>") {
			t.Errorf("LIST /b/ unexpectedly contains k1: %s", body)
		}

		// PUT /b/k2 then batch delete via POST /b/?delete
		res, _ = mustDo(t, newReq(t, http.MethodPut, "/b/k2", strings.NewReader("bar")))
		if res.StatusCode != http.StatusOK {
			t.Fatalf("PUT /b/k2: status %d", res.StatusCode)
		}
		deleteBody := `<Delete><Object><Key>k2</Key></Object></Delete>`
		req := newReq(t, http.MethodPost, "/b/?delete", strings.NewReader(deleteBody))
		req.Header.Set("Content-Type", "application/xml")
		res, body = mustDo(t, req)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("POST /b/?delete: status %d, body=%s", res.StatusCode, body)
		}
		if !strings.Contains(string(body), "<Key>k2</Key>") {
			t.Errorf("batch delete response missing k2: %s", body)
		}

		// DELETE /a/k1
		res, _ = mustDo(t, newReq(t, http.MethodDelete, "/a/k1", nil))
		if res.StatusCode != http.StatusNoContent && res.StatusCode != http.StatusOK {
			t.Errorf("DELETE /a/k1: status %d", res.StatusCode)
		}

		// GET / returns JSON index
		res, body = mustDo(t, newReq(t, http.MethodGet, "/", nil))
		if res.StatusCode != http.StatusOK {
			t.Fatalf("GET /: status %d", res.StatusCode)
		}
		var index struct {
			Buckets []string `json:"buckets"`
		}
		if err := json.Unmarshal(body, &index); err != nil {
			t.Fatalf("decode index JSON: %v; body=%s", err, body)
		}
		if len(index.Buckets) != 2 || index.Buckets[0] != "a" || index.Buckets[1] != "b" {
			t.Errorf("index buckets = %v, want [a b]", index.Buckets)
		}

		// GET /missing/key returns 404 NoSuchBucket
		res, body = mustDo(t, newReq(t, http.MethodGet, "/missing/key", nil))
		if res.StatusCode != http.StatusNotFound {
			t.Errorf("GET /missing/key: status %d, want 404", res.StatusCode)
		}
		if !strings.Contains(string(body), "<Code>NoSuchBucket</Code>") {
			t.Errorf("GET /missing/key body missing NoSuchBucket: %s", body)
		}
	})

	t.Run("unnamed single", func(t *testing.T) {
		ctx := t.Context()
		dir := t.TempDir()

		handler, err := buildBucketMux(ctx, []bucketSpec{{uri: "file://" + dir}})
		if err != nil {
			t.Fatalf("buildBucketMux: %v", err)
		}

		server := httptest.NewServer(handler)
		t.Cleanup(server.Close)

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/k1", strings.NewReader("hello"))
		if err != nil {
			t.Fatalf("new PUT: %v", err)
		}
		res, err := server.Client().Do(req)
		if err != nil {
			t.Fatalf("PUT: %v", err)
		}
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("PUT /k1: status %d", res.StatusCode)
		}

		req, err = http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/k1", nil)
		if err != nil {
			t.Fatalf("new GET: %v", err)
		}
		res, err = server.Client().Do(req)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Fatalf("GET /k1: status %d", res.StatusCode)
		}
		if string(body) != "hello" {
			t.Errorf("GET /k1 body = %q, want %q", body, "hello")
		}
	})
}
