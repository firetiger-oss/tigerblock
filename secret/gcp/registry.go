package gcp

import (
	"context"
	"fmt"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"github.com/firetiger-oss/storage/secret"
)

type registry struct{}

func init() {
	// Register GCP backend with pattern that matches Secret Manager resource names
	// Format: projects/PROJECT_ID/secrets/SECRET_NAME[/versions/VERSION_ID]
	secret.Register(`^projects/[^/]+/(secrets|locations)`, &registry{})
}

func (r *registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	// Parse the resource name to extract project ID
	projectID, err := parseProjectID(identifier)
	if err != nil {
		return nil, err
	}

	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCP secret manager client: %w", err)
	}

	return NewManagerFromClient(&clientAdapter{client: client}, projectID), nil
}

func (r *registry) ParseSecret(identifier string) (managerID, secretName string, err error) {
	// Parse resource name: projects/PROJECT_ID/secrets/SECRET_NAME[/versions/VERSION_ID]
	prefix, rest, ok := strings.Cut(identifier, "/")
	if !ok || prefix != "projects" {
		return "", "", fmt.Errorf("invalid GCP Secret Manager resource name: %s", identifier)
	}

	projectID, rest, ok := strings.Cut(rest, "/")
	if !ok || projectID == "" {
		return "", "", fmt.Errorf("invalid GCP Secret Manager resource name format: %s", identifier)
	}

	secretsLiteral, rest, ok := strings.Cut(rest, "/")
	if !ok || secretsLiteral != "secrets" {
		return "", "", fmt.Errorf("invalid GCP resource name, expected 'secrets': %s", identifier)
	}

	secretName, _, _ = strings.Cut(rest, "/")
	if secretName == "" {
		return "", "", fmt.Errorf("invalid GCP Secret Manager resource name format: %s", identifier)
	}

	managerID = "projects/" + projectID
	return managerID, secretName, nil
}

// parseProjectID extracts the project ID from a GCP resource name or identifier
func parseProjectID(identifier string) (string, error) {
	// For resource names: projects/PROJECT_ID/...
	rest, ok := strings.CutPrefix(identifier, "projects/")
	if !ok {
		return "", fmt.Errorf("unsupported identifier format: %s", identifier)
	}
	projectID, _, _ := strings.Cut(rest, "/")
	if projectID == "" {
		return "", fmt.Errorf("invalid resource name format: %s", identifier)
	}
	return projectID, nil
}
