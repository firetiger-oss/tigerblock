package gs

import (
	"errors"
	"net/http"
	"testing"

	gcloud "cloud.google.com/go/storage"
	"github.com/firetiger-oss/tigerblock/storage"
	"google.golang.org/api/googleapi"
)

func TestMakeIcebergError(t *testing.T) {
	tests := []struct {
		name        string
		inputErr    error
		expectedErr error
		checkFunc   func(t *testing.T, err error)
	}{
		{
			name:        "object not found",
			inputErr:    gcloud.ErrObjectNotExist,
			expectedErr: storage.ErrObjectNotFound,
			checkFunc: func(t *testing.T, err error) {
				if !errors.Is(err, storage.ErrObjectNotFound) {
					t.Errorf("expected error to be storage.ErrObjectNotFound, but it wasn't")
				}
				if !errors.Is(err, gcloud.ErrObjectNotExist) {
					t.Errorf("expected error to be gcloud.ErrObjectNotExist, but it wasn't")
				}
			},
		},
		{
			name:        "bucket not found",
			inputErr:    gcloud.ErrBucketNotExist,
			expectedErr: storage.ErrBucketNotFound,
			checkFunc: func(t *testing.T, err error) {
				if !errors.Is(err, storage.ErrBucketNotFound) {
					t.Errorf("expected error to be storage.ErrBucketNotFound, but it wasn't")
				}
				if !errors.Is(err, gcloud.ErrBucketNotExist) {
					t.Errorf("expected error to be gcloud.ErrBucketNotExist, but it wasn't")
				}
			},
		},
		{
			name: "precondition failed",
			inputErr: &googleapi.Error{
				Code: http.StatusPreconditionFailed,
			},
			expectedErr: storage.ErrObjectNotMatch,
			checkFunc: func(t *testing.T, err error) {
				if !errors.Is(err, storage.ErrObjectNotMatch) {
					t.Errorf("expected error to be storage.ErrObjectNotMatch, but it wasn't")
				}
				var apiErr *googleapi.Error
				if !errors.As(err, &apiErr) {
					t.Errorf("expected error to be a googleapi.Error, but it wasn't")
				} else if apiErr.Code != http.StatusPreconditionFailed {
					t.Errorf("expected error code to be %d, got %d", http.StatusPreconditionFailed, apiErr.Code)
				}
			},
		},
		{
			name:        "other error",
			inputErr:    errors.New("some other error"),
			expectedErr: errors.New("some other error"),
			checkFunc: func(t *testing.T, err error) {
				if err.Error() != "some other error" {
					t.Errorf("expected error message to be 'some other error', got '%s'", err.Error())
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := makeIcebergError(tt.inputErr)
			tt.checkFunc(t, err)
		})
	}
}
