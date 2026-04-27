package secret_test

import (
	"testing"

	"github.com/firetiger-oss/tigerblock/secret"
	"github.com/firetiger-oss/tigerblock/storage/memory"
	"github.com/firetiger-oss/tigerblock/test"
)

func TestManager(t *testing.T) {
	test.TestManager(t, func(*testing.T) (secret.Manager, error) {
		return secret.NewManager(memory.NewBucket()), nil
	})
}
