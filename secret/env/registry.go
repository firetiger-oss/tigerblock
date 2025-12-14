package env

import (
	"context"
	"strings"

	"github.com/firetiger-oss/storage/secret"
)

type registry struct{}

func init() {
	// Register env backend with pattern that matches "env:" prefix
	// Format: env:ENV_VAR_NAME
	secret.Register(`^env:`, registry{})
}

func (registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	return NewManager(), nil
}

func (registry) ParseSecret(identifier string) (managerID, secretName string, err error) {
	// Format: env:ENV_VAR_NAME
	managerID, secretName, _ = strings.Cut(identifier, ":")
	return
}
