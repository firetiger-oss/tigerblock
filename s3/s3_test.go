package s3_test

import (
	"testing"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/s3/fakes3"
	storages3 "github.com/firetiger-oss/storage/s3"
	storagetest "github.com/firetiger-oss/storage/test"
)

func TestS3(t *testing.T) {
	storagetest.TestStorage(t, func(*testing.T) (storage.Bucket, error) {
		bucket := "test"
		client := fakes3.NewClient(bucket)
		return storages3.NewBucket(client, bucket), nil
	})
}
