package env

import (
	"context"
	"strings"

	"github.com/firetiger-oss/storage/secret"
)

type registry struct{}

func init() {
	secret.Register("env:", registry{})
}

func (registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	return NewManager(), nil
}

func (registry) ParseSecret(identifier string) (managerID, secretName, version string, err error) {
	// Format: env:ENV_VAR_NAME
	if strings.HasPrefix(identifier, "env:") {
		managerID = "env:"
		secretName = strings.TrimPrefix(identifier, "env:")
	}
	return
}
