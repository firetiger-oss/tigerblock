package gs_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"strings"
	"testing"

	gcloud "cloud.google.com/go/storage"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/gs"
	"github.com/firetiger-oss/tigerblock/storage/gs/gsclient"
	storagetest "github.com/firetiger-oss/tigerblock/test"
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

// TestGCSGzipObjectServedAsStored verifies that objects uploaded with
// Content-Encoding: gzip are served as stored (compressed) rather than
// decompressed by GCS's automatic transcoding. The bucket opts out of
// transcoding via ReadCompressed(true) so ObjectInfo.Size matches the
// reader's byte length, Content-Encoding: gzip is surfaced to the
// caller, and range offsets operate on the stored bytes — the same
// contract every other backend in this package provides.
func TestGCSGzipObjectServedAsStored(t *testing.T) {
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

	t.Run("full object returns compressed bytes and ContentEncoding", func(t *testing.T) {
		r, info, err := bucket.GetObject(t.Context(), "gzipped")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, compressed) {
			t.Fatalf("body length = %d, want %d (compressed)", len(got), len(compressed))
		}
		if info.ContentEncoding != "gzip" {
			t.Errorf("ContentEncoding = %q, want %q", info.ContentEncoding, "gzip")
		}
		if info.Size != int64(len(compressed)) {
			t.Errorf("Size = %d, want %d", info.Size, len(compressed))
		}
	})

	t.Run("open-ended tail read in compressed space", func(t *testing.T) {
		const start = int64(10)
		r, _, err := bucket.GetObject(t.Context(), "gzipped", storage.BytesRange(start, -1))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if want := compressed[start:]; !bytes.Equal(got, want) {
			t.Fatalf("body length = %d, want %d", len(got), len(want))
		}
	})

	t.Run("start at end of compressed returns empty", func(t *testing.T) {
		r, _, err := bucket.GetObject(t.Context(), "gzipped", storage.BytesRange(int64(len(compressed)), -1))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("expected empty body, got %d bytes", len(got))
		}
	})

	t.Run("start past end of compressed returns empty", func(t *testing.T) {
		r, _, err := bucket.GetObject(t.Context(), "gzipped", storage.BytesRange(int64(len(compressed))+500, -1))
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 0 {
			t.Fatalf("expected empty body, got %d bytes", len(got))
		}
	})
}

func newServerAndClient(t *testing.T) (*fakestorage.Server, *gcloud.Client, string) {
	server := fakestorage.NewServer(nil)
	client := server.Client()
	return server, client, "test"
}
