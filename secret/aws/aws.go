package aws

import (
	"context"
	"fmt"
	"iter"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/firetiger-oss/storage/secret"
)

// Client defines the subset of secretsmanager.Client methods used by Manager.
// This allows for mocking in tests.
type Client interface {
	CreateSecret(ctx context.Context, params *secretsmanager.CreateSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error)
	GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
	DescribeSecret(ctx context.Context, params *secretsmanager.DescribeSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error)
	PutSecretValue(ctx context.Context, params *secretsmanager.PutSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error)
	UpdateSecret(ctx context.Context, params *secretsmanager.UpdateSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.UpdateSecretOutput, error)
	DeleteSecret(ctx context.Context, params *secretsmanager.DeleteSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.DeleteSecretOutput, error)
	ListSecrets(ctx context.Context, params *secretsmanager.ListSecretsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error)
	ListSecretVersionIds(ctx context.Context, params *secretsmanager.ListSecretVersionIdsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretVersionIdsOutput, error)
}

// Verify *secretsmanager.Client implements Client
var _ Client = (*secretsmanager.Client)(nil)

// Manager implements secret.Manager for AWS Secrets Manager
type Manager struct {
	client Client
}

// NewManager creates a Manager using the default AWS config.
// Panics on configuration error.
func NewManager() *Manager {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		panic("aws: failed to load config: " + err.Error())
	}
	return NewManagerFromConfig(cfg)
}

// NewManagerFromConfig creates a Manager from an AWS config.
func NewManagerFromConfig(cfg aws.Config) *Manager {
	return NewManagerFromClient(secretsmanager.NewFromConfig(cfg))
}

// NewManagerFromClient creates a Manager from a Client implementation.
// This is useful for testing with mock clients.
func NewManagerFromClient(client Client) *Manager {
	return &Manager{client: client}
}

func (m *Manager) CreateSecret(ctx context.Context, name string, value secret.Value, options ...secret.CreateOption) (secret.Info, error) {
	opts := secret.NewCreateOptions(options...)

	var awsTags []types.Tag
	for key, val := range opts.Tags() {
		awsTags = append(awsTags, types.Tag{
			Key:   aws.String(key),
			Value: aws.String(val),
		})
	}

	input := &secretsmanager.CreateSecretInput{
		Name:         aws.String(name),
		SecretBinary: value,
		Tags:         awsTags,
	}

	if desc := opts.Description(); desc != "" {
		input.Description = aws.String(desc)
	}

	result, err := m.client.CreateSecret(ctx, input)
	if err != nil {
		return secret.Info{}, convertError(err)
	}

	return secret.Info{
		Name:      name,
		Version:   aws.ToString(result.VersionId),
		CreatedAt: time.Now(),
		Tags:      opts.Tags(),
	}, nil
}

func (m *Manager) GetSecret(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, secret.Info, error) {
	opts := secret.NewGetOptions(options...)

	input := &secretsmanager.GetSecretValueInput{
		SecretId: aws.String(name),
	}

	if version := opts.Version(); version != "" {
		input.VersionId = aws.String(version)
	}

	result, err := m.client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, secret.Info{}, convertError(err)
	}

	var value secret.Value
	if result.SecretBinary != nil {
		value = result.SecretBinary
	} else if result.SecretString != nil {
		value = secret.Value(*result.SecretString)
	}

	info := secret.Info{
		Name:      name,
		Version:   aws.ToString(result.VersionId),
		CreatedAt: aws.ToTime(result.CreatedDate),
	}

	// Fetch tags via DescribeSecret (GetSecretValue doesn't return tags)
	descResult, err := m.client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(name),
	})
	if err == nil && descResult.Tags != nil {
		info.Tags = make(map[string]string, len(descResult.Tags))
		for _, tag := range descResult.Tags {
			info.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
		}
	}

	return value, info, nil
}

func (m *Manager) UpdateSecret(ctx context.Context, name string, value secret.Value, options ...secret.UpdateOption) (secret.Info, error) {
	opts := secret.NewUpdateOptions(options...)

	input := &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(name),
		SecretBinary: value,
	}

	result, err := m.client.PutSecretValue(ctx, input)
	if err != nil {
		return secret.Info{}, convertError(err)
	}

	info := secret.Info{
		Name:    name,
		Version: aws.ToString(result.VersionId),
	}

	// If description is provided, update it separately
	if desc := opts.Description(); desc != "" {
		_, err := m.client.UpdateSecret(ctx, &secretsmanager.UpdateSecretInput{
			SecretId:    aws.String(name),
			Description: aws.String(desc),
		})
		if err != nil {
			return info, convertError(err)
		}
	}

	return info, nil
}

func (m *Manager) DeleteSecret(ctx context.Context, name string) error {
	_, err := m.client.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId:                   aws.String(name),
		ForceDeleteWithoutRecovery: aws.Bool(true),
	})
	return convertError(err)
}

func (m *Manager) ListSecrets(ctx context.Context, options ...secret.ListOption) iter.Seq2[secret.Secret, error] {
	opts := secret.NewListOptions(options...)

	return func(yield func(secret.Secret, error) bool) {
		var nextToken *string
		for {
			input := &secretsmanager.ListSecretsInput{
				NextToken: nextToken,
			}

			if maxResults := opts.MaxResults(); maxResults > 0 {
				input.MaxResults = aws.Int32(int32(maxResults))
			}

			// Build filters for tags
			if len(opts.Tags()) > 0 {
				for key, value := range opts.Tags() {
					input.Filters = append(input.Filters, types.Filter{
						Key:    types.FilterNameStringTypeTagKey,
						Values: []string{key},
					})
					input.Filters = append(input.Filters, types.Filter{
						Key:    types.FilterNameStringTypeTagValue,
						Values: []string{value},
					})
				}
			}

			// Add name prefix filter if specified
			if prefix := opts.NamePrefix(); prefix != "" {
				input.Filters = append(input.Filters, types.Filter{
					Key:    types.FilterNameStringTypeName,
					Values: []string{prefix},
				})
			}

			output, err := m.client.ListSecrets(ctx, input)
			if err != nil {
				yield(secret.Secret{}, convertError(err))
				return
			}

			for _, s := range output.SecretList {
				// Convert AWS tags to map
				tags := make(map[string]string)
				for _, tag := range s.Tags {
					tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
				}

				sec := secret.Secret{
					Name:      aws.ToString(s.Name),
					CreatedAt: aws.ToTime(s.CreatedDate),
					UpdatedAt: aws.ToTime(s.LastChangedDate),
					Tags:      tags,
				}

				if !yield(sec, nil) {
					return
				}
			}

			if output.NextToken == nil {
				break
			}
			nextToken = output.NextToken
		}
	}
}

func (m *Manager) ListSecretVersions(ctx context.Context, name string, options ...secret.ListVersionOption) iter.Seq2[secret.Version, error] {
	opts := secret.NewListVersionOptions(options...)

	return func(yield func(secret.Version, error) bool) {
		var nextToken *string
		for {
			input := &secretsmanager.ListSecretVersionIdsInput{
				SecretId:  aws.String(name),
				NextToken: nextToken,
			}

			if maxResults := opts.MaxResults(); maxResults > 0 {
				input.MaxResults = aws.Int32(int32(maxResults))
			}

			output, err := m.client.ListSecretVersionIds(ctx, input)
			if err != nil {
				yield(secret.Version{}, convertError(err))
				return
			}

			for _, versionEntry := range output.Versions {
				// Determine state based on version stages
				state := secret.VersionStateDisabled
				for _, stage := range versionEntry.VersionStages {
					if stage == "AWSCURRENT" {
						state = secret.VersionStateEnabled
						break
					}
				}

				// Filter by state if specified
				if len(opts.States()) > 0 {
					found := false
					for _, s := range opts.States() {
						if s == state {
							found = true
							break
						}
					}
					if !found {
						continue
					}
				}

				version := secret.Version{
					ID:        aws.ToString(versionEntry.VersionId),
					State:     state,
					CreatedAt: aws.ToTime(versionEntry.CreatedDate),
				}

				if !yield(version, nil) {
					return
				}
			}

			if output.NextToken == nil {
				break
			}
			nextToken = output.NextToken
		}
	}
}

func (m *Manager) GetSecretVersion(ctx context.Context, name string, version string) (secret.Value, secret.Info, error) {
	result, err := m.client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId:  aws.String(name),
		VersionId: aws.String(version),
	})
	if err != nil {
		return nil, secret.Info{}, convertError(err)
	}

	var value secret.Value
	if result.SecretBinary != nil {
		value = result.SecretBinary
	} else if result.SecretString != nil {
		value = secret.Value(*result.SecretString)
	}

	info := secret.Info{
		Name:      name,
		Version:   version,
		CreatedAt: aws.ToTime(result.CreatedDate),
	}

	return value, info, nil
}

func (m *Manager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	// AWS doesn't support destroying individual versions
	// Versions are automatically removed after the secret is deleted
	return fmt.Errorf("destroying individual versions is not supported "+
		"by AWS Secrets Manager: %w", secret.ErrVersionNotFound)
}
