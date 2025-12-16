package secret_test

import (
	"testing"

	"github.com/firetiger-oss/storage/memory"
	"github.com/firetiger-oss/storage/secret"
	"github.com/firetiger-oss/storage/test"
)

func TestManager(t *testing.T) {
	test.TestManager(t, func(*testing.T) (secret.Manager, error) {
		return secret.NewManager(memory.NewBucket()), nil
	})
}
