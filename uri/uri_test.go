package uri_test

import (
	"strings"
	"testing"

	"github.com/firetiger-oss/tigerblock/uri"
)

func TestSplit(t *testing.T) {
	tests := []struct {
		uri      string
		scheme   string
		location string
		path     string
	}{
		{
			uri:      "",
			scheme:   "",
			location: "",
			path:     "",
		},

		{
			uri:      "file:///path/to/object",
			scheme:   "file",
			location: "",
			path:     "path/to/object",
		},

		{
			uri:      "file:///file.json",
			scheme:   "file",
			location: "",
			path:     "file.json",
		},

		{
			uri:      "file:///",
			scheme:   "file",
			location: "",
			path:     "",
		},

		{
			uri:      "path/to/object",
			scheme:   "",
			location: "",
			path:     "path/to/object",
		},

		{
			uri:      "path/to/object//",
			scheme:   "",
			location: "",
			path:     "path/to/object/",
		},

		{
			uri:      "s3://bucket.com/path/to/object//",
			scheme:   "s3",
			location: "bucket.com",
			path:     "path/to/object/",
		},

		{
			uri:      "s3://bucket.com///path//to/object//",
			scheme:   "s3",
			location: "bucket.com",
			path:     "path/to/object/",
		},

		{
			uri:      ":memory:",
			scheme:   "",
			location: ":memory:",
			path:     "",
		},

		{
			uri:      ":memory:path/to/object",
			scheme:   "",
			location: ":memory:",
			path:     "path/to/object",
		},

		{
			uri:      ":memory:/path/to/object",
			scheme:   "",
			location: ":memory:",
			path:     "path/to/object",
		},

		{
			uri:      ":memory:///path/to/object",
			scheme:   "",
			location: ":memory:",
			path:     "path/to/object",
		},

		// Local file paths with . and .. directory indicators
		{
			uri:      "/tmp/.",
			scheme:   "file",
			location: "",
			path:     "tmp/",
		},

		{
			uri:      "/tmp/..",
			scheme:   "file",
			location: "",
			path:     "",
		},

		{
			uri:      "/tmp/foo/..",
			scheme:   "file",
			location: "",
			path:     "tmp/",
		},

		// Additional file:// test cases to verify the special behavior
		{
			uri:      "file:///",
			scheme:   "file",
			location: "",
			path:     "",
		},

		{
			uri:      "file:///usr/local/bin",
			scheme:   "file",
			location: "",
			path:     "usr/local/bin",
		},

		{
			uri:      "file:////some/path/with/leading/slash",
			scheme:   "file",
			location: "",
			path:     "some/path/with/leading/slash",
		},
	}

	for _, test := range tests {
		t.Run(test.uri, func(t *testing.T) {
			scheme, location, path := uri.Split(test.uri)
			if scheme != test.scheme {
				t.Fatalf("unexpected bucket type: %q != %q", scheme, test.scheme)
			}
			if location != test.location {
				t.Fatalf("unexpected bucket name: %q != %q", location, test.location)
			}
			if path != test.path {
				t.Fatalf("unexpected object key: %q != %q", path, test.path)
			}
		})
	}
}

func TestJoin(t *testing.T) {
	tests := []struct {
		scheme   string
		location string
		path     string
		uri      string
	}{
		{
			scheme:   "file",
			location: "",
			path:     "path/to/object",
			uri:      "file:///path/to/object",
		},

		{
			scheme:   "file",
			location: "",
			path:     "",
			uri:      "file:///",
		},

		{
			scheme:   "",
			location: "",
			path:     "path/to/object",
			uri:      "path/to/object",
		},

		{
			scheme:   "",
			location: "",
			path:     "path/to/object",
			uri:      "path/to/object",
		},

		{
			scheme:   "s3",
			location: "bucket.com",
			path:     "path/to/object",
			uri:      "s3://bucket.com/path/to/object",
		},

		{
			scheme:   "s3",
			location: "bucket.com",
			path:     "",
			uri:      "s3://bucket.com",
		},

		{
			scheme:   "s3",
			location: "bucket.com",
			path:     "///",
			uri:      "s3://bucket.com",
		},

		{
			scheme:   "",
			location: ":memory:",
			path:     "",
			uri:      ":memory:",
		},

		{
			scheme:   "",
			location: ":memory:",
			path:     "path/to/object",
			uri:      ":memory:path/to/object",
		},
	}

	for _, test := range tests {
		t.Run(test.uri, func(t *testing.T) {
			uri := uri.Join(test.scheme, test.location, test.path)
			if uri != test.uri {
				t.Fatalf("unexpected object URI: %q != %q", uri, test.uri)
			}
		})
	}
}

func TestJoinPreservesTrailingSlash(t *testing.T) {
	tests := []struct {
		name     string
		scheme   string
		location string
		path     string
		expected string
	}{
		{
			name:     "file scheme with trailing slash",
			scheme:   "file",
			location: "",
			path:     "path/to/dir/",
			expected: "file:///path/to/dir/",
		},
		{
			name:     "s3 scheme with trailing slash",
			scheme:   "s3",
			location: "bucket",
			path:     "path/to/dir/",
			expected: "s3://bucket/path/to/dir/",
		},
		{
			name:     "no scheme with trailing slash",
			scheme:   "",
			location: "",
			path:     "path/to/dir/",
			expected: "path/to/dir/",
		},
		{
			name:     "memory scheme with trailing slash",
			scheme:   "",
			location: ":memory:",
			path:     "path/to/dir/",
			expected: ":memory:path/to/dir/",
		},
		{
			name:     "multiple path segments with trailing slash on last",
			scheme:   "s3",
			location: "bucket",
			path:     "segment1/segment2/",
			expected: "s3://bucket/segment1/segment2/",
		},
		{
			name:     "trailing slash with empty path segments",
			scheme:   "s3",
			location: "bucket",
			path:     "path//to///dir/",
			expected: "s3://bucket/path/to/dir/",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := uri.Join(test.scheme, test.location, test.path)
			if result != test.expected {
				t.Fatalf("expected %q, got %q", test.expected, result)
			}
		})
	}
}

func TestSplitPathStyle(t *testing.T) {
	cases := []struct {
		name             string
		in               string
		scheme           string
		host             string
		bucket           string
		key              string
		wantErrSubstring string
	}{
		{name: "scheme host bucket key", in: "http://host/bucket/key", scheme: "http", host: "host", bucket: "bucket", key: "key"},
		{name: "scheme host bucket only", in: "http://host/bucket", scheme: "http", host: "host", bucket: "bucket"},
		{name: "scheme host bucket trailing", in: "http://host/bucket/", scheme: "http", host: "host", bucket: "bucket"},
		{name: "scheme host only", in: "http://host", scheme: "http", host: "host"},
		{name: "scheme host trailing", in: "http://host/", scheme: "http", host: "host"},
		{name: "key with slashes", in: "http://host/bucket/sub/key", scheme: "http", host: "host", bucket: "bucket", key: "sub/key"},
		{name: "key with trailing slash", in: "http://host/bucket/sub/", scheme: "http", host: "host", bucket: "bucket", key: "sub/"},
		{name: "host with port", in: "http://host:8080/b/k", scheme: "http", host: "host:8080", bucket: "b", key: "k"},
		{name: "https scheme", in: "https://h/b/k", scheme: "https", host: "h", bucket: "b", key: "k"},

		{name: "empty input", in: "", wantErrSubstring: "missing scheme"},
		{name: "no scheme", in: "host/bucket/key", wantErrSubstring: "missing scheme"},
		{name: "empty scheme", in: "://host/b/k", wantErrSubstring: "empty scheme"},
		{name: "no host", in: "http://", wantErrSubstring: "missing host"},
		{name: "no host with path", in: "http:///b/k", wantErrSubstring: "missing host"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scheme, host, bucket, key, err := uri.SplitPathStyle(tc.in)
			if tc.wantErrSubstring != "" {
				if err == nil {
					t.Fatalf("SplitPathStyle(%q) = nil err, want error containing %q", tc.in, tc.wantErrSubstring)
				}
				if !strings.Contains(err.Error(), tc.wantErrSubstring) {
					t.Fatalf("SplitPathStyle(%q) err = %v, want substring %q", tc.in, err, tc.wantErrSubstring)
				}
				return
			}
			if err != nil {
				t.Fatalf("SplitPathStyle(%q) unexpected err: %v", tc.in, err)
			}
			if scheme != tc.scheme || host != tc.host || bucket != tc.bucket || key != tc.key {
				t.Fatalf("SplitPathStyle(%q) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
					tc.in, scheme, host, bucket, key, tc.scheme, tc.host, tc.bucket, tc.key)
			}
		})
	}
}

func TestJoinPathStyle(t *testing.T) {
	cases := []struct {
		name   string
		scheme string
		host   string
		bucket string
		key    string
		want   string
	}{
		{name: "all parts", scheme: "http", host: "host", bucket: "b", key: "k", want: "http://host/b/k"},
		{name: "bucket only", scheme: "http", host: "host", bucket: "b", want: "http://host/b"},
		{name: "host only", scheme: "http", host: "host", want: "http://host"},
		{name: "key only", scheme: "http", host: "host", key: "k", want: "http://host/k"},
		{name: "key with slashes", scheme: "http", host: "host", bucket: "b", key: "sub/k", want: "http://host/b/sub/k"},
		{name: "host with port", scheme: "https", host: "h:8080", bucket: "b", key: "k", want: "https://h:8080/b/k"},
		// Schemeless form: omits the `scheme://` prefix entirely.
		// Useful for building S3-style `/bucket/key` resource paths
		// like the X-Amz-Copy-Source header.
		{name: "schemeless bucket key", bucket: "b", key: "k", want: "/b/k"},
		{name: "schemeless bucket only", bucket: "b", want: "/b"},
		{name: "schemeless key with slashes", bucket: "b", key: "sub/k", want: "/b/sub/k"},
		{name: "all empty", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := uri.JoinPathStyle(tc.scheme, tc.host, tc.bucket, tc.key)
			if got != tc.want {
				t.Fatalf("JoinPathStyle(%q,%q,%q,%q) = %q, want %q",
					tc.scheme, tc.host, tc.bucket, tc.key, got, tc.want)
			}
		})
	}
}

func TestSplitJoinPathStyleRoundTrip(t *testing.T) {
	// Cases where Join(Split(s)) returns s exactly. (Trailing
	// slashes on the bucket portion don't round-trip when the key
	// is empty — see TestSplitPathStyle.)
	uris := []string{
		"http://host/b/k",
		"http://host/b/sub/k",
		"http://host/b/sub/",
		"http://host/b",
		"http://host",
		"https://h:8080/bucket/key",
	}
	for _, in := range uris {
		t.Run(in, func(t *testing.T) {
			scheme, host, bucket, key, err := uri.SplitPathStyle(in)
			if err != nil {
				t.Fatalf("SplitPathStyle: %v", err)
			}
			got := uri.JoinPathStyle(scheme, host, bucket, key)
			if got != in {
				t.Fatalf("round-trip: %q -> Split -> Join -> %q", in, got)
			}
		})
	}
}
