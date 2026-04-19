package storage_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/firetiger-oss/storage"
)

func TestGetOptions(t *testing.T) {
	tests := []struct {
		scenario   string
		options    []storage.GetOption
		start, end int64
		hasRange   bool
	}{
		{
			scenario: "empty",
			options:  []storage.GetOption{},
		},

		{
			scenario: "byte range",
			options: []storage.GetOption{
				storage.BytesRange(1, 99),
			},
			start:    1,
			end:      99,
			hasRange: true,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			options := storage.NewGetOptions(test.options...)

			if start, end, ok := options.BytesRange(); !ok {
				if test.hasRange {
					t.Error("expected range to be set")
				}
			} else {
				if !test.hasRange {
					t.Error("expected range to be empty")
				}
				if start != test.start {
					t.Errorf("unexpected start: %d != %d", start, test.start)
				}
				if end != test.end {
					t.Errorf("unexpected end: %d != %d", end, test.end)
				}
			}
		})
	}
}

func TestPutOptions(t *testing.T) {
	tests := []struct {
		scenario        string
		options         []storage.PutOption
		contentType     string
		contentEncoding string
		cacheControl    string
		ifMatch         string
		ifNoneMatch     string
		metadata        map[string]string
	}{
		{
			scenario:    "empty",
			options:     []storage.PutOption{},
			contentType: "application/octet-stream",
		},

		{
			scenario: "content type",
			options: []storage.PutOption{
				storage.ContentType("text/plain"),
			},
			contentType: "text/plain",
		},

		{
			scenario: "content encoding",
			options: []storage.PutOption{
				storage.ContentEncoding("gzip"),
			},
			contentType:     "application/octet-stream",
			contentEncoding: "gzip",
		},

		{
			scenario: "cache control",
			options: []storage.PutOption{
				storage.CacheControl("max-age=3600"),
			},
			contentType:  "application/octet-stream",
			cacheControl: "max-age=3600",
		},

		{
			scenario: "if-match",
			options: []storage.PutOption{
				storage.IfMatch("etag-1"),
			},
			contentType: "application/octet-stream",
			ifMatch:     "etag-1",
		},

		{
			scenario: "if-none-match",
			options: []storage.PutOption{
				storage.IfNoneMatch("etag-2"),
			},
			contentType: "application/octet-stream",
			ifNoneMatch: "etag-2",
		},

		{
			scenario: "metadata",
			options: []storage.PutOption{
				storage.Metadata("hello", "world"),
				storage.Metadata("answer", "42"),
			},
			contentType: "application/octet-stream",
			metadata: map[string]string{
				"hello":  "world",
				"answer": "42",
			},
		},

		{
			scenario: "multiple options",
			options: []storage.PutOption{
				storage.ContentType("text/plain"),
				storage.CacheControl("public, max-age=86400"),
				storage.ContentEncoding("gzip"),
			},
			contentType:     "text/plain",
			cacheControl:    "public, max-age=86400",
			contentEncoding: "gzip",
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			options := storage.NewPutOptions(test.options...)

			if contentType := options.ContentType(); contentType != test.contentType {
				t.Errorf("unexpected content type: %q != %q", contentType, test.contentType)
			}

			if contentEncoding := options.ContentEncoding(); contentEncoding != test.contentEncoding {
				t.Errorf("unexpected content encoding: %q != %q", contentEncoding, test.contentEncoding)
			}

			if cacheControl := options.CacheControl(); cacheControl != test.cacheControl {
				t.Errorf("unexpected cache control: %q != %q", cacheControl, test.cacheControl)
			}

			if ifMatch := options.IfMatch(); ifMatch != test.ifMatch {
				t.Errorf("unexpected if-match: %q != %q", ifMatch, test.ifMatch)
			}

			if ifNoneMatch := options.IfNoneMatch(); ifNoneMatch != test.ifNoneMatch {
				t.Errorf("unexpected if-not-match: %q != %q", ifNoneMatch, test.ifNoneMatch)
			}
		})
	}
}

func TestPutOptionsContentLength(t *testing.T) {
	tests := []struct {
		scenario string
		reader   func() io.Reader
		options  []storage.PutOption
		want     int64
	}{
		{
			scenario: "explicit option takes precedence",
			reader: func() io.Reader {
				return strings.NewReader("hello")
			},
			options: []storage.PutOption{
				storage.ContentLength(99),
			},
			want: 99,
		},

		{
			scenario: "ContentLength() int64 interface",
			reader: func() io.Reader {
				return &contentLengthReader{size: 1<<32 + 1}
			},
			want: 1<<32 + 1,
		},

		{
			scenario: "Len() int interface (bytes.Buffer)",
			reader: func() io.Reader {
				return bytes.NewBufferString("hello world")
			},
			want: 11,
		},

		{
			scenario: "Len() int interface (strings.Reader)",
			reader: func() io.Reader {
				return strings.NewReader("hello")
			},
			want: 5,
		},

		{
			scenario: "ContentLength() int64 takes precedence over Len() int",
			reader: func() io.Reader {
				return &contentLengthAndLenReader{contentLength: 1<<32 + 1, len: 5}
			},
			want: 1<<32 + 1,
		},

		{
			scenario: "unknown reader returns -1",
			reader: func() io.Reader {
				return iotest.NewReadLogger("", strings.NewReader("hello"))
			},
			want: -1,
		},

		{
			// Regression: the io.Seeker fallback used to return the
			// absolute end offset instead of bytes remaining from
			// the current cursor.
			scenario: "io.Seeker advanced past header reports remaining bytes",
			reader: func() io.Reader {
				r := newSeeker([]byte("HEADERPAYLOAD"))
				r.advance(6)
				return r
			},
			want: 7,
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			options := storage.NewPutOptions(test.options...)
			got, err := options.ContentLength(test.reader())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != test.want {
				t.Errorf("unexpected content length: %d != %d", got, test.want)
			}
		})
	}
}

type contentLengthReader struct {
	size int64
}

func (r *contentLengthReader) Read(p []byte) (int, error) { return 0, io.EOF }
func (r *contentLengthReader) ContentLength() int64       { return r.size }

type contentLengthAndLenReader struct {
	contentLength int64
	len           int
}

func (r *contentLengthAndLenReader) Read(p []byte) (int, error) { return 0, io.EOF }
func (r *contentLengthAndLenReader) ContentLength() int64       { return r.contentLength }
func (r *contentLengthAndLenReader) Len() int                   { return r.len }

// seekerOnly wraps bytes.Reader to expose only Read+Seek (not Len),
// forcing PutOptions.ContentLength to fall through to the io.Seeker
// branch instead of using the Len() shortcut.
type seekerOnly struct{ r *bytes.Reader }

func newSeeker(b []byte) *seekerOnly                     { return &seekerOnly{r: bytes.NewReader(b)} }
func (s *seekerOnly) Read(p []byte) (int, error)         { return s.r.Read(p) }
func (s *seekerOnly) Seek(o int64, w int) (int64, error) { return s.r.Seek(o, w) }
func (s *seekerOnly) advance(n int64)                    { _, _ = s.r.Seek(n, io.SeekStart) }

// Regression: *os.File previously reported the absolute file size from
// Stat(), ignoring the cursor position. Callers that pass an
// already-advanced file (e.g. after sniffing a header) were getting a
// wrong content length, which now causes false-mismatch rejections in
// the memory and file backends.
func TestPutOptionsContentLengthFileAdvancedCursor(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "blob")
	if err := os.WriteFile(path, []byte("HEADERPAYLOAD"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer f.Close()

	if _, err := f.Seek(6, io.SeekStart); err != nil {
		t.Fatalf("Seek: %v", err)
	}

	options := storage.NewPutOptions()
	got, err := options.ContentLength(f)
	if err != nil {
		t.Fatalf("ContentLength: %v", err)
	}
	if got != 7 {
		t.Errorf("ContentLength = %d; want 7 (remaining bytes after advancing 6 of 13)", got)
	}
}

func TestListOptions(t *testing.T) {
	tests := []struct {
		scenario   string
		options    []storage.ListOption
		keyPrefix  string
		startAfter string
	}{
		{
			scenario: "empty",
			options:  []storage.ListOption{},
		},

		{
			scenario: "key prefix",
			options: []storage.ListOption{
				storage.KeyPrefix("prefix/"),
			},
			keyPrefix: "prefix/",
		},

		{
			scenario: "start after",
			options: []storage.ListOption{
				storage.StartAfter("marker-key"),
			},
			startAfter: "marker-key",
		},

		{
			scenario: "both options",
			options: []storage.ListOption{
				storage.KeyPrefix("prefix/"),
				storage.StartAfter("marker-key"),
			},
			keyPrefix:  "prefix/",
			startAfter: "marker-key",
		},
	}

	for _, test := range tests {
		t.Run(test.scenario, func(t *testing.T) {
			options := storage.NewListOptions(test.options...)

			if keyPrefix := options.KeyPrefix(); keyPrefix != test.keyPrefix {
				t.Errorf("unexpected key prefix: %q != %q", keyPrefix, test.keyPrefix)
			}

			if startAfter := options.StartAfter(); startAfter != test.startAfter {
				t.Errorf("unexpected start after: %q != %q", startAfter, test.startAfter)
			}
		})
	}
}
