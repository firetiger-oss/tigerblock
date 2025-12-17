package gcp

import (
	"context"
	"iter"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/firetiger-oss/storage/secret"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
)

// SecretIterator defines the interface for iterating over secrets
type SecretIterator interface {
	Next() (*secretmanagerpb.Secret, error)
}

// SecretVersionIterator defines the interface for iterating over secret versions
type SecretVersionIterator interface {
	Next() (*secretmanagerpb.SecretVersion, error)
}

// Client defines the subset of secretmanager.Client methods used by Manager.
// This allows for mocking in tests.
type Client interface {
	CreateSecret(ctx context.Context, req *secretmanagerpb.CreateSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error)
	AddSecretVersion(ctx context.Context, req *secretmanagerpb.AddSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.SecretVersion, error)
	AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error)
	GetSecret(ctx context.Context, req *secretmanagerpb.GetSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error)
	UpdateSecret(ctx context.Context, req *secretmanagerpb.UpdateSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error)
	DeleteSecret(ctx context.Context, req *secretmanagerpb.DeleteSecretRequest, opts ...gax.CallOption) error
	ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest, opts ...gax.CallOption) SecretIterator
	ListSecretVersions(ctx context.Context, req *secretmanagerpb.ListSecretVersionsRequest, opts ...gax.CallOption) SecretVersionIterator
	DestroySecretVersion(ctx context.Context, req *secretmanagerpb.DestroySecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.SecretVersion, error)
}

// clientAdapter wraps *secretmanager.Client to implement Client interface
type clientAdapter struct {
	client *secretmanager.Client
}

func (a *clientAdapter) CreateSecret(ctx context.Context, req *secretmanagerpb.CreateSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error) {
	return a.client.CreateSecret(ctx, req, opts...)
}

func (a *clientAdapter) AddSecretVersion(ctx context.Context, req *secretmanagerpb.AddSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.SecretVersion, error) {
	return a.client.AddSecretVersion(ctx, req, opts...)
}

func (a *clientAdapter) AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	return a.client.AccessSecretVersion(ctx, req, opts...)
}

func (a *clientAdapter) GetSecret(ctx context.Context, req *secretmanagerpb.GetSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error) {
	return a.client.GetSecret(ctx, req, opts...)
}

func (a *clientAdapter) UpdateSecret(ctx context.Context, req *secretmanagerpb.UpdateSecretRequest, opts ...gax.CallOption) (*secretmanagerpb.Secret, error) {
	return a.client.UpdateSecret(ctx, req, opts...)
}

func (a *clientAdapter) DeleteSecret(ctx context.Context, req *secretmanagerpb.DeleteSecretRequest, opts ...gax.CallOption) error {
	return a.client.DeleteSecret(ctx, req, opts...)
}

func (a *clientAdapter) ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest, opts ...gax.CallOption) SecretIterator {
	return a.client.ListSecrets(ctx, req, opts...)
}

func (a *clientAdapter) ListSecretVersions(ctx context.Context, req *secretmanagerpb.ListSecretVersionsRequest, opts ...gax.CallOption) SecretVersionIterator {
	return a.client.ListSecretVersions(ctx, req, opts...)
}

func (a *clientAdapter) DestroySecretVersion(ctx context.Context, req *secretmanagerpb.DestroySecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.SecretVersion, error) {
	return a.client.DestroySecretVersion(ctx, req, opts...)
}

// Manager implements secret.Manager for GCP Secret Manager
type Manager struct {
	client      Client
	projectPath string
	projectID   string
}

// NewManager creates a new GCP Secret Manager manager using default credentials.
// Panics on configuration error.
func NewManager(projectID string) *Manager {
	if projectID == "" {
		panic("gcp: project ID is required")
	}

	client, err := secretmanager.NewClient(context.Background())
	if err != nil {
		panic("gcp: failed to create client: " + err.Error())
	}

	return &Manager{
		client:      &clientAdapter{client: client},
		projectID:   projectID,
		projectPath: "projects/" + projectID,
	}
}

// NewManagerFromClient creates a Manager from a Client implementation.
// This is useful for testing with mock clients.
func NewManagerFromClient(client Client, projectID string) *Manager {
	return &Manager{
		client:      client,
		projectID:   projectID,
		projectPath: "projects/" + projectID,
	}
}

func (m *Manager) CreateSecret(ctx context.Context, name string, value secret.Value, options ...secret.CreateOption) (secret.Info, error) {
	opts := secret.NewCreateOptions(options...)
	sec, err := m.client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{
		Parent:   m.projectPath,
		SecretId: name,
		Secret: &secretmanagerpb.Secret{
			Labels: opts.Tags(),
			Replication: &secretmanagerpb.Replication{
				Replication: &secretmanagerpb.Replication_Automatic_{
					Automatic: &secretmanagerpb.Replication_Automatic{},
				},
			},
		},
	})
	if err != nil {
		return secret.Info{}, convertError(err)
	}

	version, err := m.client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  sec.Name,
		Payload: &secretmanagerpb.SecretPayload{Data: value},
	})
	if err != nil {
		return secret.Info{}, convertError(err)
	}

	return secret.Info{
		Name:      name,
		Version:   extractVersionID(version.Name),
		CreatedAt: sec.CreateTime.AsTime(),
		Tags:      opts.Tags(),
	}, nil
}

func (m *Manager) GetSecretValue(ctx context.Context, name string, options ...secret.GetOption) (secret.Value, string, error) {
	opts := secret.NewGetOptions(options...)

	var versionPath string
	if version := opts.Version(); version != "" {
		versionPath = m.projectPath + "/secrets/" + name + "/versions/" + version
	} else {
		versionPath = m.projectPath + "/secrets/" + name + "/versions/latest"
	}

	result, err := m.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: versionPath,
	})
	if err != nil {
		return nil, "", convertError(err)
	}

	return result.Payload.Data, extractVersionID(result.Name), nil
}

func (m *Manager) GetSecretInfo(ctx context.Context, name string) (secret.Info, error) {
	result, err := m.client.GetSecret(ctx, &secretmanagerpb.GetSecretRequest{
		Name: m.projectPath + "/secrets/" + name,
	})
	if err != nil {
		return secret.Info{}, convertError(err)
	}

	// Note: GCP's GetSecret doesn't return version info directly.
	// The caller should use ListSecretVersions for version discovery.
	return secret.Info{
		Name:      name,
		CreatedAt: result.CreateTime.AsTime(),
		Tags:      result.Labels,
	}, nil
}

func (m *Manager) UpdateSecret(ctx context.Context, name string, value secret.Value, options ...secret.UpdateOption) (secret.Info, error) {
	opts := secret.NewUpdateOptions(options...)
	secretPath := m.projectPath + "/secrets/" + name

	version, err := m.client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{
		Parent:  secretPath,
		Payload: &secretmanagerpb.SecretPayload{Data: value},
	})
	if err != nil {
		return secret.Info{}, convertError(err)
	}

	info := secret.Info{
		Name:    name,
		Version: extractVersionID(version.Name),
	}

	if desc := opts.Description(); desc != "" {
		// Note: GCP doesn't have a description field, but we could use a label
		_, err := m.client.UpdateSecret(ctx, &secretmanagerpb.UpdateSecretRequest{
			Secret: &secretmanagerpb.Secret{
				Name:   secretPath,
				Labels: map[string]string{"description": desc},
			},
		})
		if err != nil {
			return info, convertError(err)
		}
	}

	return info, nil
}

func (m *Manager) DeleteSecret(ctx context.Context, name string) error {
	return convertError(m.client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{
		Name: m.projectPath + "/secrets/" + name,
	}))
}

func (m *Manager) ListSecrets(ctx context.Context, options ...secret.ListOption) iter.Seq2[secret.Secret, error] {
	opts := secret.NewListOptions(options...)

	return func(yield func(secret.Secret, error) bool) {
		req := &secretmanagerpb.ListSecretsRequest{
			Parent: m.projectPath,
		}

		if maxResults := opts.MaxResults(); maxResults > 0 {
			req.PageSize = int32(maxResults)
		}

		// GCP uses a filter string for tag filtering
		// Format: labels.KEY=VALUE
		if len(opts.Tags()) > 0 {
			var filters []string
			for key, value := range opts.Tags() {
				filters = append(filters, "labels."+key+"="+value)
			}
			req.Filter = filters[0]
			// Note: GCP filter syntax might need adjustment for
			// multiple tags
		}

		it := m.client.ListSecrets(ctx, req)

		for {
			sec, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				yield(secret.Secret{}, convertError(err))
				return
			}

			// Extract secret name from resource path
			secretName := extractSecretName(sec.Name)

			// Apply name prefix filter (client-side since GCP
			// doesn't support it natively)
			if prefix := opts.NamePrefix(); prefix != "" {
				if !hasPrefix(secretName, prefix) {
					continue
				}
			}

			s := secret.Secret{
				Name:      secretName,
				CreatedAt: sec.CreateTime.AsTime(),
				Tags:      sec.Labels,
			}

			if !yield(s, nil) {
				return
			}
		}
	}
}

func (m *Manager) ListSecretVersions(ctx context.Context, name string, options ...secret.ListVersionOption) iter.Seq2[secret.Version, error] {
	opts := secret.NewListVersionOptions(options...)

	return func(yield func(secret.Version, error) bool) {
		req := &secretmanagerpb.ListSecretVersionsRequest{
			Parent: m.projectPath + "/secrets/" + name,
		}

		if maxResults := opts.MaxResults(); maxResults > 0 {
			req.PageSize = int32(maxResults)
		}

		it := m.client.ListSecretVersions(ctx, req)

		for {
			ver, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				yield(secret.Version{}, convertError(err))
				return
			}

			// Map GCP state to secret.VersionState
			state := mapGCPState(ver.State)

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
				ID:        extractVersionID(ver.Name),
				State:     state,
				CreatedAt: ver.CreateTime.AsTime(),
			}

			if !yield(version, nil) {
				return
			}
		}
	}
}

func (m *Manager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	_, err := m.client.DestroySecretVersion(ctx, &secretmanagerpb.DestroySecretVersionRequest{
		Name: m.projectPath + "/secrets/" + name + "/versions/" + version,
	})
	return convertError(err)
}

// Helper functions

// extractSecretName extracts the secret name from a full resource path
// projects/PROJECT_ID/secrets/SECRET_NAME -> SECRET_NAME
func extractSecretName(resourceName string) string {
	parts := splitPath(resourceName)
	if len(parts) >= 4 && parts[2] == "secrets" {
		return parts[3]
	}
	return resourceName
}

// extractVersionID extracts the version ID from a full version resource path
// projects/PROJECT_ID/secrets/SECRET_NAME/versions/VERSION_ID -> VERSION_ID
func extractVersionID(resourceName string) string {
	parts := splitPath(resourceName)
	if len(parts) >= 6 && parts[4] == "versions" {
		return parts[5]
	}
	return ""
}

// splitPath splits a resource path by '/'
func splitPath(path string) []string {
	return splitString(path, "/")
}

// splitString splits a string by a delimiter
func splitString(s, delim string) []string {
	var result []string
	for {
		idx := indexOf(s, delim)
		if idx == -1 {
			result = append(result, s)
			break
		}
		result = append(result, s[:idx])
		s = s[idx+len(delim):]
	}
	return result
}

// indexOf returns the index of the first occurrence of substr in s, or -1 if
// not found
func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// hasPrefix checks if a string has a given prefix
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// mapGCPState maps GCP SecretVersion state to secret.VersionState
func mapGCPState(state secretmanagerpb.SecretVersion_State) secret.VersionState {
	switch state {
	case secretmanagerpb.SecretVersion_ENABLED:
		return secret.VersionStateEnabled
	case secretmanagerpb.SecretVersion_DISABLED:
		return secret.VersionStateDisabled
	case secretmanagerpb.SecretVersion_DESTROYED:
		return secret.VersionStateDestroyed
	default:
		return secret.VersionStateDisabled
	}
}
