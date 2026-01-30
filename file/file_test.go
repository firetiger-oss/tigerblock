package file_test

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/file"
	storagetest "github.com/firetiger-oss/storage/test"
)

func TestFileStorage(t *testing.T) {
	t.Parallel()
	storagetest.TestStorage(t, func(t *testing.T) (storage.Bucket, error) {
		root := filepath.Join(t.TempDir(), "bucket")
		return file.NewRegistry(root).LoadBucket(t.Context(), "")
	})
}

func TestLoadFileBucketLocation(t *testing.T) {
	t.Parallel()
	bucket, err := storage.LoadBucket(t.Context(), "file:///mnt/storage")
	if err != nil {
		t.Fatal(err)
	}
	bucketLocation := bucket.Location()
	if bucketLocation != "file:///mnt/storage/" {
		t.Errorf("expected bucket location to be 'file:///mnt/storage', got '%s'", bucketLocation)
	}
}

func TestFileStoragePutObjectContent(t *testing.T) {
	t.Parallel()
	root := filepath.Join(t.TempDir(), "bucket")
	bucket, err := file.NewRegistry(root).LoadBucket(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}

	if err := bucket.Create(t.Context()); err != nil {
		t.Fatal("error creating bucket:", err)
	}

	const key = "test-file.txt"
	const data = "This is test content that should be written to the file verbatim."

	putObject, err := bucket.PutObject(t.Context(), key, strings.NewReader(data))
	if err != nil {
		t.Fatal("error putting object:", err)
	}

	if putObject.Size != int64(len(data)) {
		t.Errorf("expected object size %d, got %d", len(data), putObject.Size)
	}

	filePath := filepath.Join(root, key)
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal("error reading file directly:", err)
	}

	if string(fileContent) != data {
		t.Errorf("file content doesn't match input data\nExpected: %q\nGot: %q", data, string(fileContent))
	}

	r, getObject, err := bucket.GetObject(t.Context(), key)
	if err != nil {
		t.Fatal("error getting object:", err)
	}
	defer r.Close()

	content, err := io.ReadAll(r)
	if err != nil {
		t.Fatal("error reading object content:", err)
	}

	if string(content) != data {
		t.Errorf("GetObject content doesn't match input data\nExpected: %q\nGot: %q", data, string(content))
	}

	if getObject.Size != putObject.Size {
		t.Errorf("size mismatch: PutObject size = %d, GetObject size = %d", putObject.Size, getObject.Size)
	}
}
