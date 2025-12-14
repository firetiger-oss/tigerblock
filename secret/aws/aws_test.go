package aws

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/firetiger-oss/storage/secret"
	"github.com/firetiger-oss/storage/test"
)

// mockClient implements Client for testing
type mockClient struct {
	mu      sync.RWMutex
	secrets map[string]*mockSecret
}

type mockSecret struct {
	versions    []mockVersion
	tags        []types.Tag
	description string
	createdAt   time.Time
	updatedAt   time.Time
}

type mockVersion struct {
	id        string
	value     []byte
	createdAt time.Time
}

func newMockClient() *mockClient {
	return &mockClient{secrets: make(map[string]*mockSecret)}
}

func (m *mockClient) CreateSecret(ctx context.Context, params *secretsmanager.CreateSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.CreateSecretOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	name := aws.ToString(params.Name)
	if _, exists := m.secrets[name]; exists {
		return nil, &types.ResourceExistsException{Message: aws.String("secret already exists")}
	}

	now := time.Now()
	versionID := "v1-" + now.Format("20060102150405.000")
	m.secrets[name] = &mockSecret{
		versions: []mockVersion{
			{id: versionID, value: params.SecretBinary, createdAt: now},
		},
		tags:        params.Tags,
		description: aws.ToString(params.Description),
		createdAt:   now,
		updatedAt:   now,
	}

	return &secretsmanager.CreateSecretOutput{
		Name:      params.Name,
		VersionId: aws.String(versionID),
	}, nil
}

func (m *mockClient) GetSecretValue(ctx context.Context, params *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	name := aws.ToString(params.SecretId)
	sec, exists := m.secrets[name]
	if !exists {
		return nil, &types.ResourceNotFoundException{Message: aws.String("secret not found")}
	}

	if len(sec.versions) == 0 {
		return nil, &types.ResourceNotFoundException{Message: aws.String("no versions")}
	}

	// Find requested version or latest
	var ver *mockVersion
	requestedVersionID := aws.ToString(params.VersionId)
	if requestedVersionID != "" {
		for i := range sec.versions {
			if sec.versions[i].id == requestedVersionID {
				ver = &sec.versions[i]
				break
			}
		}
		if ver == nil {
			return nil, &types.ResourceNotFoundException{
				Message: aws.String("version not found"),
			}
		}
	} else {
		// Return latest version
		ver = &sec.versions[len(sec.versions)-1]
	}

	return &secretsmanager.GetSecretValueOutput{
		Name:         aws.String(name),
		SecretBinary: ver.value,
		VersionId:    aws.String(ver.id),
		CreatedDate:  aws.Time(ver.createdAt),
	}, nil
}

func (m *mockClient) DescribeSecret(ctx context.Context, params *secretsmanager.DescribeSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.DescribeSecretOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	name := aws.ToString(params.SecretId)
	sec, exists := m.secrets[name]
	if !exists {
		return nil, &types.ResourceNotFoundException{Message: aws.String("secret not found")}
	}

	return &secretsmanager.DescribeSecretOutput{
		Name:            aws.String(name),
		Description:     aws.String(sec.description),
		Tags:            sec.tags,
		CreatedDate:     aws.Time(sec.createdAt),
		LastChangedDate: aws.Time(sec.updatedAt),
	}, nil
}

func (m *mockClient) PutSecretValue(ctx context.Context, params *secretsmanager.PutSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.PutSecretValueOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	name := aws.ToString(params.SecretId)
	sec, exists := m.secrets[name]
	if !exists {
		return nil, &types.ResourceNotFoundException{Message: aws.String("secret not found")}
	}

	now := time.Now()
	versionNum := len(sec.versions) + 1
	versionID := "v" + string(rune('0'+versionNum)) + "-" + now.Format("20060102150405.000")
	sec.versions = append(sec.versions, mockVersion{
		id:        versionID,
		value:     params.SecretBinary,
		createdAt: now,
	})
	sec.updatedAt = now

	return &secretsmanager.PutSecretValueOutput{
		VersionId: aws.String(versionID),
	}, nil
}

func (m *mockClient) UpdateSecret(ctx context.Context, params *secretsmanager.UpdateSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.UpdateSecretOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	name := aws.ToString(params.SecretId)
	sec, exists := m.secrets[name]
	if !exists {
		return nil, &types.ResourceNotFoundException{Message: aws.String("secret not found")}
	}

	if params.Description != nil {
		sec.description = aws.ToString(params.Description)
	}
	sec.updatedAt = time.Now()

	return &secretsmanager.UpdateSecretOutput{
		Name: params.SecretId,
	}, nil
}

func (m *mockClient) DeleteSecret(ctx context.Context, params *secretsmanager.DeleteSecretInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.DeleteSecretOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	name := aws.ToString(params.SecretId)
	delete(m.secrets, name)

	return &secretsmanager.DeleteSecretOutput{}, nil
}

func (m *mockClient) ListSecrets(ctx context.Context, params *secretsmanager.ListSecretsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	var secretList []types.SecretListEntry
secretLoop:
	for name, sec := range m.secrets {
		// Apply filters
		if len(params.Filters) > 0 {
			// Collect tag key and value filters to apply together
			var requiredTagKeys, requiredTagValues []string

			for _, filter := range params.Filters {
				switch filter.Key {
				case types.FilterNameStringTypeName:
					// Check if name matches any of the filter values (prefix match)
					match := false
					for _, val := range filter.Values {
						if len(name) >= len(val) && name[:len(val)] == val {
							match = true
							break
						}
					}
					if !match {
						continue secretLoop
					}
				case types.FilterNameStringTypeTagKey:
					requiredTagKeys = append(requiredTagKeys, filter.Values...)
				case types.FilterNameStringTypeTagValue:
					requiredTagValues = append(requiredTagValues, filter.Values...)
				}
			}

			// Check tag key filters
			for _, reqKey := range requiredTagKeys {
				found := false
				for _, tag := range sec.tags {
					if aws.ToString(tag.Key) == reqKey {
						found = true
						break
					}
				}
				if !found {
					continue secretLoop
				}
			}

			// Check tag value filters
			for _, reqVal := range requiredTagValues {
				found := false
				for _, tag := range sec.tags {
					if aws.ToString(tag.Value) == reqVal {
						found = true
						break
					}
				}
				if !found {
					continue secretLoop
				}
			}
		}

		entry := types.SecretListEntry{
			Name:            aws.String(name),
			Tags:            sec.tags,
			CreatedDate:     aws.Time(sec.createdAt),
			LastChangedDate: aws.Time(sec.updatedAt),
		}
		secretList = append(secretList, entry)
	}

	// Apply max results
	if params.MaxResults != nil && int32(len(secretList)) > *params.MaxResults {
		secretList = secretList[:*params.MaxResults]
	}

	return &secretsmanager.ListSecretsOutput{
		SecretList: secretList,
	}, nil
}

func (m *mockClient) ListSecretVersionIds(ctx context.Context, params *secretsmanager.ListSecretVersionIdsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretVersionIdsOutput, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	name := aws.ToString(params.SecretId)
	sec, exists := m.secrets[name]
	if !exists {
		return nil, &types.ResourceNotFoundException{Message: aws.String("secret not found")}
	}

	var versions []types.SecretVersionsListEntry
	for i, v := range sec.versions {
		stages := []string{"AWSPREVIOUS"}
		if i == len(sec.versions)-1 {
			stages = []string{"AWSCURRENT"}
		}
		versions = append(versions, types.SecretVersionsListEntry{
			VersionId:     aws.String(v.id),
			VersionStages: stages,
			CreatedDate:   aws.Time(v.createdAt),
		})
	}

	return &secretsmanager.ListSecretVersionIdsOutput{
		Versions: versions,
	}, nil
}

func TestManager(t *testing.T) {
	test.TestManager(t, func(t *testing.T) (secret.Manager, error) {
		return NewManagerFromClient(newMockClient()), nil
	})
}

func TestRegistryParseSecret(t *testing.T) {
	reg := &registry{}

	tests := []struct {
		name           string
		identifier     string
		wantManagerID  string
		wantSecretName string
		wantErr        bool
	}{
		{
			name:           "valid ARN",
			identifier:     "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret-AbCdEf",
			wantManagerID:  "arn:aws:secretsmanager:us-east-1:123456789012",
			wantSecretName: "my-secret-AbCdEf",
			wantErr:        false,
		},
		{
			name:           "ARN without suffix",
			identifier:     "arn:aws:secretsmanager:us-east-1:123456789012:secret:my-secret",
			wantManagerID:  "arn:aws:secretsmanager:us-east-1:123456789012",
			wantSecretName: "my-secret",
			wantErr:        false,
		},
		{
			name:           "secret with slashes",
			identifier:     "arn:aws:secretsmanager:us-east-1:123456789012:secret:app/database/password",
			wantManagerID:  "arn:aws:secretsmanager:us-east-1:123456789012",
			wantSecretName: "app/database/password",
			wantErr:        false,
		},
		{
			name:           "invalid ARN",
			identifier:     "invalid",
			wantManagerID:  "",
			wantSecretName: "",
			wantErr:        true,
		},
		{
			name:           "incomplete ARN",
			identifier:     "arn:aws:secretsmanager:us-east-1",
			wantManagerID:  "",
			wantSecretName: "",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotManagerID, gotSecretName, err := reg.ParseSecret(tt.identifier)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseSecret() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotManagerID != tt.wantManagerID {
				t.Errorf("ParseSecret() managerID = %v, want %v", gotManagerID, tt.wantManagerID)
			}
			if gotSecretName != tt.wantSecretName {
				t.Errorf("ParseSecret() secretName = %v, want %v", gotSecretName, tt.wantSecretName)
			}
		})
	}
}
