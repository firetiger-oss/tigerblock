package gcp

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/firetiger-oss/storage/secret"
	"github.com/firetiger-oss/storage/test"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockClient implements Client for testing
type mockClient struct {
	mu        sync.RWMutex
	projectID string
	secrets   map[string]*mockSecret
}

type mockSecret struct {
	name        string
	labels      map[string]string
	versions    []*mockVersion
	createdAt   time.Time
	description string
}

type mockVersion struct {
	id        string
	data      []byte
	state     secretmanagerpb.SecretVersion_State
	createdAt time.Time
}

func newMockClient(projectID string) *mockClient {
	return &mockClient{
		projectID: projectID,
		secrets:   make(map[string]*mockSecret),
	}
}

func (m *mockClient) CreateSecret(ctx context.Context, req *secretmanagerpb.CreateSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	name := req.SecretId
	if _, exists := m.secrets[name]; exists {
		return nil, status.Error(codes.AlreadyExists, "secret already exists")
	}

	now := time.Now()
	m.secrets[name] = &mockSecret{
		name:      name,
		labels:    req.Secret.Labels,
		versions:  []*mockVersion{},
		createdAt: now,
	}

	fullName := "projects/" + m.projectID + "/secrets/" + name
	return &secretmanagerpb.Secret{
		Name:       fullName,
		Labels:     req.Secret.Labels,
		CreateTime: timestamppb.New(now),
	}, nil
}

func (m *mockClient) AddSecretVersion(ctx context.Context, req *secretmanagerpb.AddSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.SecretVersion, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Extract secret name from parent path
	secretName := extractSecretName(req.Parent)
	sec, exists := m.secrets[secretName]
	if !exists {
		return nil, status.Error(codes.NotFound, "secret not found")
	}

	now := time.Now()
	versionNum := len(sec.versions) + 1
	versionID := strconv.Itoa(versionNum)

	// Disable previous version if exists
	for _, v := range sec.versions {
		if v.state == secretmanagerpb.SecretVersion_ENABLED {
			v.state = secretmanagerpb.SecretVersion_DISABLED
		}
	}

	version := &mockVersion{
		id:        versionID,
		data:      req.Payload.Data,
		state:     secretmanagerpb.SecretVersion_ENABLED,
		createdAt: now,
	}
	sec.versions = append(sec.versions, version)

	fullName := "projects/" + m.projectID + "/secrets/" + secretName + "/versions/" + versionID
	return &secretmanagerpb.SecretVersion{
		Name:       fullName,
		State:      secretmanagerpb.SecretVersion_ENABLED,
		CreateTime: timestamppb.New(now),
	}, nil
}

func (m *mockClient) AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	// Parse the version path
	secretName := extractSecretName(req.Name)
	versionID := extractVersionID(req.Name)

	sec, exists := m.secrets[secretName]
	if !exists {
		return nil, status.Error(codes.NotFound, "secret not found")
	}

	if len(sec.versions) == 0 {
		return nil, status.Error(codes.NotFound, "no versions found")
	}

	var version *mockVersion
	if versionID == "latest" || versionID == "" {
		// Find the latest enabled version
		for i := len(sec.versions) - 1; i >= 0; i-- {
			if sec.versions[i].state == secretmanagerpb.SecretVersion_ENABLED {
				version = sec.versions[i]
				break
			}
		}
		if version == nil {
			return nil, status.Error(codes.NotFound, "no enabled version found")
		}
	} else {
		// Find specific version
		for _, v := range sec.versions {
			if v.id == versionID {
				version = v
				break
			}
		}
		if version == nil {
			return nil, status.Error(codes.NotFound, "version not found")
		}
		// Check if version is destroyed
		if version.state == secretmanagerpb.SecretVersion_DESTROYED {
			return nil, status.Error(codes.FailedPrecondition, "version is destroyed")
		}
	}

	fullName := "projects/" + m.projectID + "/secrets/" + secretName + "/versions/" + version.id
	return &secretmanagerpb.AccessSecretVersionResponse{
		Name: fullName,
		Payload: &secretmanagerpb.SecretPayload{
			Data: version.data,
		},
	}, nil
}

func (m *mockClient) GetSecret(ctx context.Context, req *secretmanagerpb.GetSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	secretName := extractSecretName(req.Name)
	sec, exists := m.secrets[secretName]
	if !exists {
		return nil, status.Error(codes.NotFound, "secret not found")
	}

	return &secretmanagerpb.Secret{
		Name:       req.Name,
		Labels:     sec.labels,
		CreateTime: timestamppb.New(sec.createdAt),
	}, nil
}

func (m *mockClient) UpdateSecret(ctx context.Context, req *secretmanagerpb.UpdateSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	secretName := extractSecretName(req.Secret.Name)
	sec, exists := m.secrets[secretName]
	if !exists {
		return nil, status.Error(codes.NotFound, "secret not found")
	}

	if req.Secret.Labels != nil {
		sec.labels = req.Secret.Labels
	}

	return &secretmanagerpb.Secret{
		Name:       req.Secret.Name,
		Labels:     sec.labels,
		CreateTime: timestamppb.New(sec.createdAt),
	}, nil
}

func (m *mockClient) DeleteSecret(ctx context.Context, req *secretmanagerpb.DeleteSecretRequest, opts ...gax.CallOption) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	secretName := extractSecretName(req.Name)
	delete(m.secrets, secretName)

	return nil
}

// mockSecretIterator implements SecretIterator for testing
type mockSecretIterator struct {
	secrets []*secretmanagerpb.Secret
	index   int
}

func (it *mockSecretIterator) Next() (*secretmanagerpb.Secret, error) {
	if it.index >= len(it.secrets) {
		return nil, iterator.Done
	}
	sec := it.secrets[it.index]
	it.index++
	return sec, nil
}

func (m *mockClient) ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest, opts ...gax.CallOption) SecretIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var secrets []*secretmanagerpb.Secret
	for name, sec := range m.secrets {
		// Apply filter if present
		if req.Filter != "" {
			// Parse filter: labels.KEY=VALUE
			if !matchesFilter(sec.labels, req.Filter) {
				continue
			}
		}

		fullName := "projects/" + m.projectID + "/secrets/" + name
		secrets = append(secrets, &secretmanagerpb.Secret{
			Name:       fullName,
			Labels:     sec.labels,
			CreateTime: timestamppb.New(sec.createdAt),
		})
	}

	// Apply page size limit
	if req.PageSize > 0 && int32(len(secrets)) > req.PageSize {
		secrets = secrets[:req.PageSize]
	}

	return &mockSecretIterator{secrets: secrets}
}

// mockSecretVersionIterator implements SecretVersionIterator for testing
type mockSecretVersionIterator struct {
	versions []*secretmanagerpb.SecretVersion
	index    int
}

func (it *mockSecretVersionIterator) Next() (*secretmanagerpb.SecretVersion, error) {
	if it.index >= len(it.versions) {
		return nil, iterator.Done
	}
	ver := it.versions[it.index]
	it.index++
	return ver, nil
}

func (m *mockClient) ListSecretVersions(ctx context.Context, req *secretmanagerpb.ListSecretVersionsRequest, opts ...gax.CallOption) SecretVersionIterator {
	m.mu.RLock()
	defer m.mu.RUnlock()

	secretName := extractSecretName(req.Parent)
	sec, exists := m.secrets[secretName]
	if !exists {
		return &mockSecretVersionIterator{versions: nil}
	}

	var versions []*secretmanagerpb.SecretVersion
	for _, v := range sec.versions {
		fullName := "projects/" + m.projectID + "/secrets/" + secretName + "/versions/" + v.id
		versions = append(versions, &secretmanagerpb.SecretVersion{
			Name:       fullName,
			State:      v.state,
			CreateTime: timestamppb.New(v.createdAt),
		})
	}

	// Apply page size limit
	if req.PageSize > 0 && int32(len(versions)) > req.PageSize {
		versions = versions[:req.PageSize]
	}

	return &mockSecretVersionIterator{versions: versions}
}

func (m *mockClient) DestroySecretVersion(ctx context.Context, req *secretmanagerpb.DestroySecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.SecretVersion, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	secretName := extractSecretName(req.Name)
	versionID := extractVersionID(req.Name)

	sec, exists := m.secrets[secretName]
	if !exists {
		return nil, status.Error(codes.NotFound, "secret not found")
	}

	for _, v := range sec.versions {
		if v.id == versionID {
			v.state = secretmanagerpb.SecretVersion_DESTROYED
			return &secretmanagerpb.SecretVersion{
				Name:       req.Name,
				State:      secretmanagerpb.SecretVersion_DESTROYED,
				CreateTime: timestamppb.New(v.createdAt),
			}, nil
		}
	}

	return nil, status.Error(codes.NotFound, "version not found")
}

// matchesFilter checks if labels match a filter string like "labels.KEY=VALUE"
func matchesFilter(labels map[string]string, filter string) bool {
	// Parse filter: "labels.KEY=VALUE"
	if len(filter) < 8 || filter[:7] != "labels." {
		return true // Invalid filter, don't filter
	}

	rest := filter[7:]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '=' {
			key := rest[:i]
			value := rest[i+1:]
			if labels == nil {
				return false
			}
			return labels[key] == value
		}
	}
	return true
}

func TestManager(t *testing.T) {
	test.TestManager(t, func(t *testing.T) (secret.Manager, error) {
		return NewManagerFromClient(newMockClient("test-project"), "test-project"), nil
	})
}

func TestParseProjectID(t *testing.T) {
	tests := []struct {
		name          string
		identifier    string
		wantProjectID string
		wantErr       bool
	}{
		{
			name:          "valid resource name",
			identifier:    "projects/my-project/secrets/my-secret",
			wantProjectID: "my-project",
			wantErr:       false,
		},
		{
			name:          "just project",
			identifier:    "projects/my-project",
			wantProjectID: "my-project",
			wantErr:       false,
		},
		{
			name:          "invalid format",
			identifier:    "invalid",
			wantProjectID: "",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseProjectID(tt.identifier)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseProjectID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.wantProjectID {
				t.Errorf("parseProjectID() = %v, want %v", got, tt.wantProjectID)
			}
		})
	}
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
			name:           "valid resource name",
			identifier:     "projects/my-project/secrets/my-secret",
			wantManagerID:  "projects/my-project",
			wantSecretName: "my-secret",
			wantErr:        false,
		},
		{
			name:           "resource name with version",
			identifier:     "projects/my-project/secrets/my-secret/versions/1",
			wantManagerID:  "projects/my-project",
			wantSecretName: "my-secret",
			wantErr:        false,
		},
		{
			name:           "invalid resource name",
			identifier:     "invalid",
			wantManagerID:  "",
			wantSecretName: "",
			wantErr:        true,
		},
		{
			name:           "incomplete resource name",
			identifier:     "projects/my-project",
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

func TestExtractSecretName(t *testing.T) {
	tests := []struct {
		name         string
		resourceName string
		want         string
	}{
		{
			name:         "full secret path",
			resourceName: "projects/my-project/secrets/my-secret",
			want:         "my-secret",
		},
		{
			name:         "version path",
			resourceName: "projects/my-project/secrets/my-secret/versions/1",
			want:         "my-secret",
		},
		{
			name:         "just name",
			resourceName: "my-secret",
			want:         "my-secret",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractSecretName(tt.resourceName); got != tt.want {
				t.Errorf("extractSecretName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractVersionID(t *testing.T) {
	tests := []struct {
		name         string
		resourceName string
		want         string
	}{
		{
			name:         "version path",
			resourceName: "projects/my-project/secrets/my-secret/versions/1",
			want:         "1",
		},
		{
			name:         "latest version",
			resourceName: "projects/my-project/secrets/my-secret/versions/latest",
			want:         "latest",
		},
		{
			name:         "no version",
			resourceName: "projects/my-project/secrets/my-secret",
			want:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractVersionID(tt.resourceName); got != tt.want {
				t.Errorf("extractVersionID() = %v, want %v", got, tt.want)
			}
		})
	}
}
