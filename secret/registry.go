package secret

import (
	"context"
	"fmt"
	"iter"
	"regexp"
	"sync"
)

// Registry is an interface for loading secret managers from identifiers.
// Each backend (AWS, GCP, env) implements a Registry to create Manager
// instances from native resource identifiers (ARNs, resource names, etc).
type Registry interface {
	// LoadManager loads a manager from a secret identifier.
	// The identifier can be a full secret path or just a manager identifier.
	LoadManager(ctx context.Context, identifier string) (Manager, error)

	// ParseSecret extracts the manager identifier and secret name from a full secret identifier.
	// Returns empty secretName if the identifier doesn't contain a secret name.
	ParseSecret(identifier string) (managerID, secretName string, err error)
}

// RegistryFunc is a function type that implements the Registry interface.
type RegistryFunc func(context.Context, string) (Manager, error)

// LoadManager implements the Registry interface for RegistryFunc.
func (reg RegistryFunc) LoadManager(ctx context.Context, identifier string) (Manager, error) {
	return reg(ctx, identifier)
}

// ParseSecret is a default implementation that returns an empty manager ID
// and uses the identifier as the secret name.
func (reg RegistryFunc) ParseSecret(identifier string) (string, string, error) {
	return "", identifier, nil
}

type registryEntry struct {
	pattern *regexp.Regexp
	reg     Registry
}

var (
	globalMutex     sync.RWMutex
	globalAdapters  []Adapter
	globalRegistries []registryEntry
)

// Register registers a registry with a regex pattern for matching identifiers.
// This function is typically called in init() by backend packages.
//
// Example:
//
//	func init() {
//		// Match AWS Secrets Manager ARNs
//		secret.Register(`^arn:aws:secretsmanager:`, NewAWSRegistry())
//
//		// Match GCP Secret Manager resource names
//		secret.Register(`^projects/[^/]+/secrets/`, NewGCPRegistry())
//
//		// Match env backend
//		secret.Register(`^env$`, NewEnvRegistry())
//	}
func Register(pattern string, reg Registry) {
	compiled := regexp.MustCompile(pattern)
	globalMutex.Lock()
	globalRegistries = append(globalRegistries, registryEntry{
		pattern: compiled,
		reg:     reg,
	})
	globalMutex.Unlock()
}

// Install installs global adapters that will be applied to all managers
// loaded through LoadManager or the global convenience functions.
//
// Example:
//
//	secret.Install(secret.WithInstrumentation())
func Install(adapters ...Adapter) {
	globalMutex.Lock()
	globalAdapters = append(globalAdapters, adapters...)
	globalMutex.Unlock()
}

// DefaultRegistry returns a registry that uses pattern-based matching.
func DefaultRegistry() Registry {
	return &defaultRegistry{}
}

type defaultRegistry struct{}

func (r *defaultRegistry) LoadManager(ctx context.Context, identifier string) (Manager, error) {
	if identifier == "" {
		return nil, fmt.Errorf("identifier is required")
	}

	globalMutex.RLock()
	registries := globalRegistries
	adapters := globalAdapters
	globalMutex.RUnlock()

	// Find matching registry by pattern
	for _, entry := range registries {
		if entry.pattern.MatchString(identifier) {
			manager, err := entry.reg.LoadManager(ctx, identifier)
			if err != nil {
				return nil, err
			}

			// Apply global adapters
			manager = AdaptManager(manager, adapters...)
			return manager, nil
		}
	}

	return nil, fmt.Errorf("no registry found for identifier: %s", identifier)
}

func (r *defaultRegistry) ParseSecret(identifier string) (string, string, error) {
	if identifier == "" {
		return "", "", fmt.Errorf("identifier is required")
	}

	globalMutex.RLock()
	registries := globalRegistries
	globalMutex.RUnlock()

	// Find matching registry by pattern
	for _, entry := range registries {
		if entry.pattern.MatchString(identifier) {
			return entry.reg.ParseSecret(identifier)
		}
	}

	return "", "", fmt.Errorf("no registry found for identifier: %s", identifier)
}

// LoadManager loads a secret manager from an identifier using the default registry.
//
// The identifier format depends on the backend:
//   - AWS: arn:aws:secretsmanager:REGION:ACCOUNT:secret:NAME
//   - GCP: projects/PROJECT_ID/secrets/NAME
//   - Env: env
//
// Examples:
//   - arn:aws:secretsmanager:us-east-1:123456789012:secret:db-password
//   - projects/my-project/secrets/db-password
//   - env
func LoadManager(ctx context.Context, identifier string) (Manager, error) {
	return DefaultRegistry().LoadManager(ctx, identifier)
}

// LoadManagerAt loads a secret manager from an identifier using the specified registry.
func LoadManagerAt(ctx context.Context, registry Registry, identifier string) (Manager, error) {
	return registry.LoadManager(ctx, identifier)
}

// Create is a convenience function that creates a secret using an identifier.
//
// Example:
//
//	secretARN := "arn:aws:secretsmanager:us-east-1:123456789012:secret:mydb"
//	info, err := secret.Create(ctx, secretARN,
//		secret.Value("secret-value"),
//		secret.Tag("env", "production"))
func Create(ctx context.Context, secretID string, value Value, options ...CreateOption) (Info, error) {
	return CreateAt(ctx, DefaultRegistry(), secretID, value, options...)
}

// CreateAt is like Create but uses the specified registry.
func CreateAt(ctx context.Context, registry Registry, secretID string, value Value, options ...CreateOption) (Info, error) {
	managerID, name, err := registry.ParseSecret(secretID)
	if err != nil {
		return Info{}, err
	}

	if name == "" {
		return Info{}, fmt.Errorf("secret name required in identifier: %s", secretID)
	}

	manager, err := registry.LoadManager(ctx, managerID)
	if err != nil {
		return Info{}, err
	}

	return manager.CreateSecret(ctx, name, value, options...)
}

// Get is a convenience function that retrieves a secret using an identifier.
//
// Example:
//
//	secretARN := "arn:aws:secretsmanager:us-east-1:123456789012:secret:mydb"
//	value, info, err := secret.Get(ctx, secretARN)
func Get(ctx context.Context, secretID string, options ...GetOption) (Value, Info, error) {
	return GetAt(ctx, DefaultRegistry(), secretID, options...)
}

// GetAt is like Get but uses the specified registry.
func GetAt(ctx context.Context, registry Registry, secretID string, options ...GetOption) (Value, Info, error) {
	managerID, name, err := registry.ParseSecret(secretID)
	if err != nil {
		return nil, Info{}, err
	}

	if name == "" {
		return nil, Info{}, fmt.Errorf("secret name required in identifier: %s", secretID)
	}

	manager, err := registry.LoadManager(ctx, managerID)
	if err != nil {
		return nil, Info{}, err
	}

	return manager.GetSecret(ctx, name, options...)
}

// Update is a convenience function that updates a secret using an identifier.
//
// Example:
//
//	secretARN := "arn:aws:secretsmanager:us-east-1:123456789012:secret:mydb"
//	info, err := secret.Update(ctx, secretARN, secret.Value("new-value"))
func Update(ctx context.Context, secretID string, value Value, options ...UpdateOption) (Info, error) {
	return UpdateAt(ctx, DefaultRegistry(), secretID, value, options...)
}

// UpdateAt is like Update but uses the specified registry.
func UpdateAt(ctx context.Context, registry Registry, secretID string, value Value, options ...UpdateOption) (Info, error) {
	managerID, name, err := registry.ParseSecret(secretID)
	if err != nil {
		return Info{}, err
	}

	if name == "" {
		return Info{}, fmt.Errorf("secret name required in identifier: %s", secretID)
	}

	manager, err := registry.LoadManager(ctx, managerID)
	if err != nil {
		return Info{}, err
	}

	return manager.UpdateSecret(ctx, name, value, options...)
}

// Delete is a convenience function that deletes a secret using an identifier.
//
// Example:
//
//	secretARN := "arn:aws:secretsmanager:us-east-1:123456789012:secret:mydb"
//	err := secret.Delete(ctx, secretARN)
func Delete(ctx context.Context, secretID string) error {
	return DeleteAt(ctx, DefaultRegistry(), secretID)
}

// DeleteAt is like Delete but uses the specified registry.
func DeleteAt(ctx context.Context, registry Registry, secretID string) error {
	managerID, name, err := registry.ParseSecret(secretID)
	if err != nil {
		return err
	}

	if name == "" {
		return fmt.Errorf("secret name required in identifier: %s", secretID)
	}

	manager, err := registry.LoadManager(ctx, managerID)
	if err != nil {
		return err
	}

	return manager.DeleteSecret(ctx, name)
}

// List is a convenience function that lists secrets using a manager identifier.
//
// Example:
//
//	managerARN := "arn:aws:secretsmanager:us-east-1:123456789012"
//	for s, err := range secret.List(ctx, managerARN, secret.NamePrefix("db-")) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(s.Name)
//	}
func List(ctx context.Context, managerID string, options ...ListOption) iter.Seq2[Secret, error] {
	return ListAt(ctx, DefaultRegistry(), managerID, options...)
}

// ListAt is like List but uses the specified registry.
func ListAt(ctx context.Context, registry Registry, managerID string, options ...ListOption) iter.Seq2[Secret, error] {
	return func(yield func(Secret, error) bool) {
		manager, err := registry.LoadManager(ctx, managerID)
		if err != nil {
			yield(Secret{}, err)
			return
		}

		for secret, err := range manager.ListSecrets(ctx, options...) {
			if !yield(secret, err) {
				return
			}
		}
	}
}

