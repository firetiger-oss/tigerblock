package gs_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"

	gcloud "cloud.google.com/go/storage"
	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/gs"
	"github.com/firetiger-oss/storage/gs/gsclient"
	storagetest "github.com/firetiger-oss/storage/test"
	"github.com/fsouza/fake-gcs-server/fakestorage"
)

func TestGoogleCloudStorageBucket(t *testing.T) {
	storagetest.TestStorage(t, func(*testing.T) (storage.Bucket, error) {
		server, googleClient, bucket := newServerAndClient(t)
		defer server.Stop()
		gsClient, err := gsclient.NewGoogleCloudStorageClient(t.Context(), bucket, gsclient.WithHTTPClient(server.HTTPClient()))
		if err != nil {
			return nil, err
		}
		return gs.NewBucket(googleClient, gsClient, bucket), nil
	})
}

// TestGCSTailRangePastDecompressedEndTranscoded covers the other edge
// of transcoded-range handling: an offset past the *decompressed* end
// of a gzip-transcoded object must still return an empty reader and
// nil error, matching the BytesRange(offset, -1) contract. Without
// special handling, the io.CopyN(Discard, reader, start) that the gs
// backend uses to seek into the transcoded stream would surface an
// io.EOF and be returned to the caller as an error.
func TestGCSTailRangePastDecompressedEndTranscoded(t *testing.T) {
	server, googleClient, bucketName := newServerAndClient(t)
	defer server.Stop()

	decompressed := strings.Repeat("hello world! ", 100) // 1300 bytes
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(decompressed)); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	compressed := buf.Bytes()

	server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName:      bucketName,
			Name:            "gzipped",
			ContentEncoding: "gzip",
			ContentType:     "text/plain",
		},
		Content: compressed,
	})

	gsClient, err := gsclient.NewGoogleCloudStorageClient(t.Context(), bucketName, gsclient.WithHTTPClient(server.HTTPClient()))
	if err != nil {
		t.Fatal(err)
	}
	bucket := gs.NewBucket(googleClient, gsClient, bucketName)

	cases := []struct {
		name  string
		start int64
	}{
		{name: "at end of decompressed", start: int64(len(decompressed))},
		{name: "past end of decompressed", start: int64(len(decompressed)) + 500},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, _, err := bucket.GetObject(t.Context(), "gzipped", storage.BytesRange(tc.start, -1))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				t.Fatalf("unexpected read error: %v", err)
			}
			if len(b) != 0 {
				t.Fatalf("expected empty body, got %d bytes", len(b))
			}
		})
	}
}

// TestGCSTailRangeAgainstTranscodedObject guards the case where a GCS
// object is stored gzip-compressed (Content-Encoding: gzip) and served
// decompressed on read. Object attributes report the compressed size,
// but a caller's BytesRange offsets are in decompressed-byte space.
// The bucket must not short-circuit or clamp range requests using the
// compressed attrs.Size, since an offset past that size can still be
// valid in the decompressed stream.
func TestGCSTailRangeAgainstTranscodedObject(t *testing.T) {
	server, googleClient, bucketName := newServerAndClient(t)
	defer server.Stop()

	decompressed := strings.Repeat("hello world! ", 100) // 1300 bytes
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write([]byte(decompressed)); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	compressed := buf.Bytes()
	if len(compressed) >= len(decompressed) {
		t.Fatalf("setup: compressed (%d) should be smaller than decompressed (%d)", len(compressed), len(decompressed))
	}

	server.CreateObject(fakestorage.Object{
		ObjectAttrs: fakestorage.ObjectAttrs{
			BucketName:      bucketName,
			Name:            "gzipped",
			ContentEncoding: "gzip",
			ContentType:     "text/plain",
		},
		Content: compressed,
	})

	gsClient, err := gsclient.NewGoogleCloudStorageClient(t.Context(), bucketName, gsclient.WithHTTPClient(server.HTTPClient()))
	if err != nil {
		t.Fatal(err)
	}
	bucket := gs.NewBucket(googleClient, gsClient, bucketName)

	// Offset between compressed size and decompressed size — the bucket
	// must not short-circuit this to an empty body using attrs.Size.
	const offset = int64(100)
	if offset <= int64(len(compressed)) {
		t.Fatalf("setup: need offset past compressed size (got %d vs %d)", offset, len(compressed))
	}
	r, _, err := bucket.GetObject(t.Context(), "gzipped", storage.BytesRange(offset, -1))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer r.Close()
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	want := decompressed[offset:]
	if string(got) != want {
		t.Fatalf("body mismatch: got %d bytes, want %d bytes (first bytes: got %q want %q)",
			len(got), len(want), head(string(got), 40), head(want, 40))
	}
}

func head(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

func newServerAndClient(t *testing.T) (*fakestorage.Server, *gcloud.Client, string) {
	server := fakestorage.NewServer(nil)
	client := server.Client()
	return server, client, "test"
}
