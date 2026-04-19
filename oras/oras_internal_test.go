package oras

import (
	"errors"
	"fmt"
	"testing"

	"github.com/firetiger-oss/storage"
	"oras.land/oras-go/v2/errdef"
)

func TestMakeError(t *testing.T) {
	// Every errdef sentinel makeError might attach. Used to verify
	// that passthrough cases don't accidentally pick up any of them.
	allSentinels := []error{
		errdef.ErrNotFound,
		errdef.ErrAlreadyExists,
		errdef.ErrInvalidDigest,
		errdef.ErrUnsupported,
	}

	tests := []struct {
		name      string
		in        error
		wantNil   bool
		wantIs    []error // every error here must satisfy errors.Is(out, e)
		wantNotIs []error // every error here must NOT satisfy errors.Is(out, e)
	}{
		{
			name:    "nil passes through",
			in:      nil,
			wantNil: true,
		},
		{
			name:   "ErrObjectNotFound → ErrNotFound",
			in:     storage.ErrObjectNotFound,
			wantIs: []error{errdef.ErrNotFound, storage.ErrObjectNotFound},
		},
		{
			name:   "wrapped ErrObjectNotFound → ErrNotFound",
			in:     fmt.Errorf("blobs/sha256/abc: %w", storage.ErrObjectNotFound),
			wantIs: []error{errdef.ErrNotFound, storage.ErrObjectNotFound},
		},
		{
			name:   "ErrBucketNotFound → ErrNotFound",
			in:     storage.ErrBucketNotFound,
			wantIs: []error{errdef.ErrNotFound, storage.ErrBucketNotFound},
		},
		{
			name:   "ErrObjectNotMatch → ErrAlreadyExists",
			in:     storage.ErrObjectNotMatch,
			wantIs: []error{errdef.ErrAlreadyExists, storage.ErrObjectNotMatch},
		},
		{
			name:   "wrapped ErrObjectNotMatch → ErrAlreadyExists",
			in:     fmt.Errorf("refs/v1: %w", storage.ErrObjectNotMatch),
			wantIs: []error{errdef.ErrAlreadyExists, storage.ErrObjectNotMatch},
		},
		{
			name:   "ErrChecksumMismatch → ErrInvalidDigest",
			in:     storage.ErrChecksumMismatch,
			wantIs: []error{errdef.ErrInvalidDigest, storage.ErrChecksumMismatch},
		},
		{
			name:   "wrapped ErrChecksumMismatch → ErrInvalidDigest",
			in:     fmt.Errorf("blobs/sha256/abc: %w", storage.ErrChecksumMismatch),
			wantIs: []error{errdef.ErrInvalidDigest, storage.ErrChecksumMismatch},
		},
		{
			name:   "ErrBucketReadOnly → ErrUnsupported",
			in:     storage.ErrBucketReadOnly,
			wantIs: []error{errdef.ErrUnsupported, storage.ErrBucketReadOnly},
		},
		{
			name:      "unrelated error passes through without an errdef sentinel",
			in:        errors.New("boom"),
			wantNotIs: allSentinels,
		},
		{
			name:      "ErrTooManyRequests has no errdef counterpart",
			in:        storage.ErrTooManyRequests,
			wantIs:    []error{storage.ErrTooManyRequests},
			wantNotIs: allSentinels,
		},
		{
			name:      "ErrInvalidObjectKey has no errdef counterpart",
			in:        storage.ErrInvalidObjectKey,
			wantIs:    []error{storage.ErrInvalidObjectKey},
			wantNotIs: allSentinels,
		},
		{
			name:      "ErrInvalidRange has no errdef counterpart",
			in:        storage.ErrInvalidRange,
			wantIs:    []error{storage.ErrInvalidRange},
			wantNotIs: allSentinels,
		},
		{
			name:      "ErrPresignNotSupported has no errdef counterpart",
			in:        storage.ErrPresignNotSupported,
			wantIs:    []error{storage.ErrPresignNotSupported},
			wantNotIs: allSentinels,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := makeError(tt.in)
			if tt.wantNil {
				if got != nil {
					t.Fatalf("makeError(nil) = %v; want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("makeError(%v) = nil; want non-nil", tt.in)
			}
			for _, target := range tt.wantIs {
				if !errors.Is(got, target) {
					t.Errorf("errors.Is(%v, %v) = false; want true", got, target)
				}
			}
			for _, target := range tt.wantNotIs {
				if errors.Is(got, target) {
					t.Errorf("errors.Is(%v, %v) = true; want false (passthrough should not attach an errdef sentinel)", got, target)
				}
			}
		})
	}
}
