package secret

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"iter"
	"strconv"
	"strings"
	"time"

	"github.com/firetiger-oss/storage"
)

// NewManager creates a new secret.Manager backed by a storage.Bucket.
// Secrets are stored as JSON objects in the bucket with the key "{name}.json".
//
// Example:
//
//	// Create an in-memory secret manager
//	mgr := secret.NewManager(memory.NewBucket())
//
//	// Create a file-backed secret manager
//	bucket, _ := storage.LoadBucket(ctx, "file:///var/secrets")
//	mgr := secret.NewManager(bucket)
func NewManager(bucket storage.Bucket) Manager {
	return &bucketManager{bucket: bucket}
}

type bucketManager struct {
	bucket storage.Bucket
}

type storedSecret struct {
	Versions    []storedVersion   `json:"versions"`
	Tags        map[string]string `json:"tags,omitempty"`
	Description string            `json:"description,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

type storedVersion struct {
	ID        string       `json:"id"`
	Value     []byte       `json:"value"`
	CreatedAt time.Time    `json:"created_at"`
	State     VersionState `json:"state"`
}

func (m *bucketManager) CreateSecret(ctx context.Context, name string, value Value, options ...CreateOption) (Info, error) {
	if err := context.Cause(ctx); err != nil {
		return Info{}, err
	}

	opts := NewCreateOptions(options...)
	key := secretKey(name)
	now := time.Now()

	secret := storedSecret{
		Versions: []storedVersion{
			{
				ID:        "1",
				Value:     value,
				CreatedAt: now,
				State:     VersionStateEnabled,
			},
		},
		Tags:        opts.Tags(),
		Description: opts.Description(),
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	// Atomic create: fail if object already exists
	if err := m.createSecret(ctx, key, &secret); err != nil {
		if errors.Is(err, storage.ErrObjectNotMatch) {
			return Info{}, ErrAlreadyExists
		}
		return Info{}, err
	}

	return Info{
		Name:         name,
		Version:      "1",
		CreatedAt:    now,
		UpdatedAt:    now,
		Tags:         secret.Tags,
		Description:  secret.Description,
		VersionCount: 1,
	}, nil
}

func (m *bucketManager) GetSecret(ctx context.Context, name string, options ...GetOption) (Value, Info, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, Info{}, err
	}

	opts := NewGetOptions(options...)
	key := secretKey(name)

	secret, _, err := m.readSecret(ctx, key)
	if err != nil {
		return nil, Info{}, err
	}

	targetVersion := opts.Version()
	var version *storedVersion

	if targetVersion != "" {
		// Find specific version
		for i := range secret.Versions {
			if secret.Versions[i].ID == targetVersion {
				version = &secret.Versions[i]
				break
			}
		}
		if version == nil {
			return nil, Info{}, ErrVersionNotFound
		}
		if version.State == VersionStateDestroyed {
			return nil, Info{}, ErrVersionNotFound
		}
	} else {
		// Find latest enabled version
		for i := len(secret.Versions) - 1; i >= 0; i-- {
			if secret.Versions[i].State == VersionStateEnabled {
				version = &secret.Versions[i]
				break
			}
		}
		if version == nil {
			return nil, Info{}, ErrNotFound
		}
	}

	return Value(version.Value), Info{
		Name:         name,
		Version:      version.ID,
		CreatedAt:    secret.CreatedAt,
		UpdatedAt:    secret.UpdatedAt,
		Tags:         secret.Tags,
		Description:  secret.Description,
		VersionCount: len(secret.Versions),
	}, nil
}

func (m *bucketManager) UpdateSecret(ctx context.Context, name string, value Value, options ...UpdateOption) (Info, error) {
	if err := context.Cause(ctx); err != nil {
		return Info{}, err
	}

	opts := NewUpdateOptions(options...)
	key := secretKey(name)

	secret, etag, err := m.readSecret(ctx, key)
	if err != nil {
		return Info{}, err
	}

	now := time.Now()
	nextVersion := strconv.Itoa(len(secret.Versions) + 1)

	secret.Versions = append(secret.Versions, storedVersion{
		ID:        nextVersion,
		Value:     value,
		CreatedAt: now,
		State:     VersionStateEnabled,
	})
	secret.UpdatedAt = now

	if desc := opts.Description(); desc != "" {
		secret.Description = desc
	}

	if err := m.writeSecret(ctx, key, secret, etag); err != nil {
		return Info{}, err
	}

	return Info{
		Name:         name,
		Version:      nextVersion,
		CreatedAt:    secret.CreatedAt,
		UpdatedAt:    now,
		Tags:         secret.Tags,
		Description:  secret.Description,
		VersionCount: len(secret.Versions),
	}, nil
}

func (m *bucketManager) DeleteSecret(ctx context.Context, name string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}

	key := secretKey(name)
	err := m.bucket.DeleteObject(ctx, key)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return nil // idempotent delete
	}
	return err
}

func (m *bucketManager) ListSecrets(ctx context.Context, options ...ListOption) iter.Seq2[Secret, error] {
	return func(yield func(Secret, error) bool) {
		if err := context.Cause(ctx); err != nil {
			yield(Secret{}, err)
			return
		}

		opts := NewListOptions(options...)
		namePrefix := opts.NamePrefix()
		tagFilters := opts.Tags()
		maxResults := opts.MaxResults()

		// Convert name prefix to key prefix
		keyPrefix := namePrefix
		if keyPrefix != "" {
			// Don't add .json suffix for prefix search
		}

		count := 0
		for obj, err := range m.bucket.ListObjects(ctx, storage.KeyPrefix(keyPrefix)) {
			if err != nil {
				yield(Secret{}, err)
				return
			}

			// Only process .json files
			if !strings.HasSuffix(obj.Key, ".json") {
				continue
			}

			name := strings.TrimSuffix(obj.Key, ".json")

			// Apply name prefix filter
			if namePrefix != "" && !strings.HasPrefix(name, namePrefix) {
				continue
			}

			// Read secret to get tags if we need to filter
			var secret *storedSecret
			if len(tagFilters) > 0 {
				secret, _, err = m.readSecret(ctx, obj.Key)
				if err != nil {
					yield(Secret{}, err)
					return
				}

				// Check tag filters
				if !matchesTags(secret.Tags, tagFilters) {
					continue
				}
			}

			// Apply max results
			if maxResults > 0 && count >= maxResults {
				return
			}

			s := Secret{
				Name:      name,
				UpdatedAt: obj.LastModified,
			}

			// Include tags if we already read them
			if secret != nil {
				s.Tags = secret.Tags
				s.CreatedAt = secret.CreatedAt
				s.UpdatedAt = secret.UpdatedAt
			}

			if !yield(s, nil) {
				return
			}
			count++
		}
	}
}

func (m *bucketManager) ListSecretVersions(ctx context.Context, name string, options ...ListVersionOption) iter.Seq2[Version, error] {
	return func(yield func(Version, error) bool) {
		if err := context.Cause(ctx); err != nil {
			yield(Version{}, err)
			return
		}

		key := secretKey(name)
		secret, _, err := m.readSecret(ctx, key)
		if err != nil {
			yield(Version{}, err)
			return
		}

		opts := NewListVersionOptions(options...)
		maxResults := opts.MaxResults()
		states := opts.States()

		count := 0
		// Return versions in reverse order (newest first)
		for i := len(secret.Versions) - 1; i >= 0; i-- {
			v := secret.Versions[i]

			// Filter by state if specified
			if len(states) > 0 && !containsState(states, v.State) {
				continue
			}

			if maxResults > 0 && count >= maxResults {
				return
			}

			if !yield(Version{
				ID:        v.ID,
				CreatedAt: v.CreatedAt,
				State:     v.State,
			}, nil) {
				return
			}
			count++
		}
	}
}

func (m *bucketManager) DestroySecretVersion(ctx context.Context, name string, version string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}

	key := secretKey(name)
	secret, etag, err := m.readSecret(ctx, key)
	if err != nil {
		return err
	}

	found := false
	for i := range secret.Versions {
		if secret.Versions[i].ID == version {
			if secret.Versions[i].State == VersionStateDestroyed {
				return ErrVersionNotFound
			}
			secret.Versions[i].State = VersionStateDestroyed
			secret.Versions[i].Value = nil // Clear the value
			found = true
			break
		}
	}

	if !found {
		return ErrVersionNotFound
	}

	return m.writeSecret(ctx, key, secret, etag)
}

func (m *bucketManager) createSecret(ctx context.Context, key string, secret *storedSecret) error {
	data, err := json.Marshal(secret)
	if err != nil {
		return err
	}
	// Atomic create: fail if object already exists
	_, err = m.bucket.PutObject(ctx, key, bytes.NewReader(data),
		storage.ContentType(storage.ContentTypeJSON),
		storage.IfNoneMatch("*"),
	)
	return err
}

func (m *bucketManager) readSecret(ctx context.Context, key string) (*storedSecret, string, error) {
	reader, info, err := m.bucket.GetObject(ctx, key)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	defer reader.Close()

	var secret storedSecret
	if err := json.NewDecoder(reader).Decode(&secret); err != nil {
		return nil, "", err
	}
	return &secret, info.ETag, nil
}

func (m *bucketManager) writeSecret(ctx context.Context, key string, secret *storedSecret, etag string) error {
	data, err := json.Marshal(secret)
	if err != nil {
		return err
	}
	// Conditional write: only succeed if ETag matches (optimistic locking)
	_, err = m.bucket.PutObject(ctx, key, bytes.NewReader(data),
		storage.ContentType(storage.ContentTypeJSON),
		storage.IfMatch(etag),
	)
	return err
}

func secretKey(name string) string {
	return name + ".json"
}

func matchesTags(secretTags, filterTags map[string]string) bool {
	for k, v := range filterTags {
		if secretTags[k] != v {
			return false
		}
	}
	return true
}

func containsState(states []VersionState, state VersionState) bool {
	for _, s := range states {
		if s == state {
			return true
		}
	}
	return false
}
