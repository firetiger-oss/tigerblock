package aws

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/firetiger-oss/storage/secret"
)

type registry struct{}

func init() {
	// Register AWS backend with pattern that matches Secrets Manager ARNs
	// Format: arn:aws:secretsmanager:REGION:ACCOUNT:secret:NAME[-SUFFIX]
	secret.Register(`^arn:aws:secretsmanager:`, &registry{})
}

func (r *registry) LoadManager(ctx context.Context, identifier string) (secret.Manager, error) {
	parsed, err := arn.Parse(identifier)
	if err != nil {
		return nil, fmt.Errorf("invalid ARN: %w", err)
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(parsed.Region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	return NewManagerFromConfig(cfg), nil
}

func (r *registry) ParseSecret(identifier string) (managerID, secretName string, err error) {
	parsed, err := arn.Parse(identifier)
	if err != nil {
		return "", "", fmt.Errorf("invalid AWS Secrets Manager ARN: %w", err)
	}

	if parsed.Service != "secretsmanager" {
		return "", "", fmt.Errorf("invalid AWS Secrets Manager ARN: expected service 'secretsmanager', got %q", parsed.Service)
	}

	// Resource format is "secret:NAME[-SUFFIX]"
	// Extract the secret name from after "secret:"
	resource := parsed.Resource
	if !strings.HasPrefix(resource, "secret:") {
		return "", "", fmt.Errorf("invalid AWS Secrets Manager ARN: expected resource type 'secret:', got %q", resource)
	}
	secretName = strings.TrimPrefix(resource, "secret:")

	// Manager ID is the ARN prefix without the resource
	managerID = "arn:" + parsed.Partition + ":" + parsed.Service + ":" + parsed.Region + ":" + parsed.AccountID

	return managerID, secretName, nil
}
