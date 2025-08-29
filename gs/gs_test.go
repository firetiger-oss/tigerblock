package gs_test

import (
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

func newServerAndClient(t *testing.T) (*fakestorage.Server, *gcloud.Client, string) {
	server := fakestorage.NewServer(nil)
	client := server.Client()
	return server, client, "test"
}
