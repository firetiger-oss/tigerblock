package aws

import (
	"context"
	"fmt"
	"iter"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/firetiger-oss/storage/secret"
)

func validateSecretName(name string) error {
	if strings.HasPrefix(name, "arn:") {
		return fmt.Errorf("%w: full ARN not allowed, use simple name", secret.ErrInvalidName)
	}
	return nil
}

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
	if err := validateSecretName(name); err != nil {
		return secret.Info{}, err
	}
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

func (m *Manager) GetSecretValue(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
	if err := validateSecretName(name); err != nil {
		return nil, "", err
	}
	opts := secret.NewGetOptions(options...)
	input := &secretsmanager.GetSecretValueInput{SecretId: aws.String(name)}
	if version := opts.Version(); version != "" {
		input.VersionId = aws.String(version)
	}

	result, err := m.client.GetSecretValue(ctx, input)
	if err != nil {
		return nil, "", convertError(err)
	}

	if result.SecretBinary != nil {
		return result.SecretBinary, aws.ToString(result.VersionId), nil
	}
	if result.SecretString != nil {
		return secret.Value(*result.SecretString), aws.ToString(result.VersionId), nil
	}
	return nil, aws.ToString(result.VersionId), nil
}

func (m *Manager) GetSecretInfo(ctx context.Context, name string) (secret.Info, error) {
	if err := validateSecretName(name); err != nil {
		return secret.Info{}, err
	}
	result, err := m.client.DescribeSecret(ctx, &secretsmanager.DescribeSecretInput{
		SecretId: aws.String(name),
	})
	if err != nil {
		return secret.Info{}, convertError(err)
	}
	return secret.Info{
		Name:        name,
		Version:     currentVersion(result.VersionIdsToStages),
		CreatedAt:   aws.ToTime(result.CreatedDate),
		UpdatedAt:   aws.ToTime(result.LastChangedDate),
		Description: aws.ToString(result.Description),
		Tags:        makeTags(result.Tags),
	}, nil
}

func (m *Manager) UpdateSecret(ctx context.Context, name string, value secret.Value, options ...secret.UpdateOption) (secret.Info, error) {
	if err := validateSecretName(name); err != nil {
		return secret.Info{}, err
	}
	opts := secret.NewUpdateOptions(options...)
	result, err := m.client.PutSecretValue(ctx, &secretsmanager.PutSecretValueInput{
		SecretId:     aws.String(name),
		SecretBinary: value,
	})
	if err != nil {
		return secret.Info{}, convertError(err)
	}

	info := secret.Info{
		Name:    name,
		Version: aws.ToString(result.VersionId),
	}

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
	if err := validateSecretName(name); err != nil {
		return err
	}
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
				sec := secret.Secret{
					Name:      aws.ToString(s.Name),
					CreatedAt: aws.ToTime(s.CreatedDate),
					UpdatedAt: aws.ToTime(s.LastChangedDate),
					Tags:      makeTags(s.Tags),
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
	validateErr := validateSecretName(name)

	return func(yield func(secret.Version, error) bool) {
		if validateErr != nil {
			yield(secret.Version{}, validateErr)
			return
		}
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
				state := versionState(versionEntry.VersionStages)

				if !matchState(state, opts.States()) {
					continue
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

func (m *Manager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	// AWS doesn't support destroying individual versions
	// Versions are automatically removed after the secret is deleted
	return fmt.Errorf("destroying individual versions is not supported by AWS Secrets Manager: %w", secret.ErrNotFound)
}

func currentVersion(versionIdsToStages map[string][]string) string {
	for vid, stages := range versionIdsToStages {
		for _, stage := range stages {
			if stage == "AWSCURRENT" {
				return vid
			}
		}
	}
	return ""
}

func makeTags(tags []types.Tag) map[string]string {
	if tags == nil {
		return nil
	}
	m := make(map[string]string, len(tags))
	for _, tag := range tags {
		m[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return m
}

func matchState(state secret.VersionState, filter []secret.VersionState) bool {
	if len(filter) == 0 {
		return true
	}
	for _, s := range filter {
		if s == state {
			return true
		}
	}
	return false
}

func versionState(stages []string) secret.VersionState {
	for _, stage := range stages {
		if stage == "AWSCURRENT" {
			return secret.VersionStateEnabled
		}
	}
	return secret.VersionStateDisabled
}
