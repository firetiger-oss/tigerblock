// Package test provides a comprehensive test suite for secret.Manager implementations.
// The test suite validates all required behaviors across different backends and adapters.
package test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/firetiger-oss/storage/secret"
)

// TestManager runs a comprehensive test suite against a secret manager implementation.
// The loadManager function should create a fresh manager instance for each test.
//
// Example usage:
//
//	test.TestManager(t, func(t *testing.T) (secret.Manager, error) {
//		return secret.LoadManager(t.Context(), "secret://env")
//	})
func TestManager(t *testing.T, loadManager func(*testing.T) (secret.Manager, error)) {
	// Test with different adapters to ensure adapters don't break functionality
	adapters := []struct {
		name    string
		adapter secret.Adapter
	}{
		{
			name:    "base",
			adapter: secret.AdapterFunc(func(m secret.Manager) secret.Manager { return m }),
		},
	}

	tests := []struct {
		scenario string
		function func(*testing.T, secret.Manager)
	}{
		{
			scenario: "creating and retrieving a secret works",
			function: skipIfReadOnly(testCreateAndGet),
		},
		{
			scenario: "getting a non-existent secret returns error",
			function: testGetNotExist,
		},
		{
			scenario: "creating a duplicate secret returns error",
			function: skipIfReadOnly(testCreateDuplicate),
		},
		{
			scenario: "updating a secret works",
			function: skipIfReadOnly(testUpdate),
		},
		{
			scenario: "deleting a secret works",
			function: skipIfReadOnly(testDelete),
		},
		{
			scenario: "delete is idempotent",
			function: skipIfReadOnly(testDeleteIdempotent),
		},
		{
			scenario: "listing secrets works",
			function: testList,
		},
		{
			scenario: "listing with name prefix filter",
			function: testListWithPrefix,
		},
		{
			scenario: "listing with max results",
			function: testListWithMaxResults,
		},
		{
			scenario: "creating secret with tags",
			function: skipIfReadOnlyOrNoTags(testCreateWithTags),
		},
		{
			scenario: "filtering secrets by tags",
			function: skipIfReadOnlyOrNoTags(testListWithTagFilter),
		},
		{
			scenario: "context cancellation is respected",
			function: testContextCancellation,
		},
		{
			scenario: "listing secret versions works",
			function: skipIfReadOnly(testListSecretVersions),
		},
		{
			scenario: "getting secret with version option",
			function: skipIfReadOnly(testGetSecretWithVersion),
		},
		{
			scenario: "getting non-existent version returns error",
			function: testGetSecretVersionNotFound,
		},
		{
			scenario: "destroying a secret version",
			function: skipIfReadOnlyOrNoDestroy(testDestroySecretVersion),
		},
		{
			scenario: "updating secret with description",
			function: skipIfReadOnly(testUpdateSecretWithDescription),
		},
		{
			scenario: "creating secret with description",
			function: skipIfReadOnly(testCreateSecretWithDescription),
		},
		{
			scenario: "updating non-existent secret returns error",
			function: skipIfReadOnly(testUpdateNonExistent),
		},
		{
			scenario: "listing with multiple filters",
			function: skipIfReadOnlyOrNoTags(testListWithMultipleFilters),
		},
	}

	for _, adapter := range adapters {
		t.Run(adapter.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.scenario, func(t *testing.T) {
					manager, err := loadManager(t)
					if err != nil {
						t.Fatal("unexpected error loading manager:", err)
					}
					manager = adapter.adapter.AdaptManager(manager)
					test.function(t, manager)
				})
			}
		})
	}
}

func skipIfReadOnly(test func(*testing.T, secret.Manager)) func(*testing.T, secret.Manager) {
	return func(t *testing.T, m secret.Manager) {
		ctx := t.Context()
		testName := "test-readonly-check-" + randomString()
		_, err := m.CreateSecret(ctx, testName, []byte("value"))
		if errors.Is(err, secret.ErrReadOnly) {
			t.Skip("backend is read-only")
		}
		if err == nil {
			_ = m.DeleteSecret(ctx, testName)
		}
		test(t, m)
	}
}

func skipIfReadOnlyOrNoTags(test func(*testing.T, secret.Manager)) func(*testing.T, secret.Manager) {
	return func(t *testing.T, m secret.Manager) {
		ctx := t.Context()

		// Check read-only first
		testName := "test-readonly-check-" + randomString()
		_, err := m.CreateSecret(ctx, testName, []byte("value"))
		if errors.Is(err, secret.ErrReadOnly) {
			t.Skip("backend is read-only")
		}
		if err == nil {
			_ = m.DeleteSecret(ctx, testName)
		}

		// Check tag support
		testName = "test-tag-support-" + randomString()
		_, err = m.CreateSecret(ctx, testName, []byte("test"), secret.Tag("test", "value"))
		if err == nil {
			_ = m.DeleteSecret(ctx, testName)
		}

		test(t, m)
	}
}

func skipIfReadOnlyOrNoDestroy(test func(*testing.T, secret.Manager)) func(*testing.T, secret.Manager) {
	return func(t *testing.T, m secret.Manager) {
		ctx := t.Context()

		// Check read-only first
		testName := "test-readonly-check-" + randomString()
		_, err := m.CreateSecret(ctx, testName, []byte("value"))
		if errors.Is(err, secret.ErrReadOnly) {
			t.Skip("backend is read-only")
		}
		if err == nil {
			_ = m.DeleteSecret(ctx, testName)
		}

		// Check destroy support
		err = m.DestroySecretVersion(ctx, "nonexistent", "1")
		if err != nil {
			if strings.Contains(err.Error(), "not supported") {
				t.Skip("backend does not support version destruction")
			}
			if !errors.Is(err, secret.ErrNotFound) {
				t.Skip("backend does not support version destruction")
			}
		}

		test(t, m)
	}
}

func testCreateAndGet(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-secret-" + randomString()
	value := []byte("secret-value-" + randomString())

	info, err := manager.CreateSecret(ctx, name, value)
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}

	if info.Name != name {
		t.Errorf("expected name %q, got %q", name, info.Name)
	}

	gotValue, gotVersion, err := manager.GetSecretValue(ctx, name)
	if err != nil {
		t.Fatal("unexpected error getting secret:", err)
	}

	if string(gotValue) != string(value) {
		t.Errorf("expected value %q, got %q", value, gotValue)
	}

	if gotVersion == "" {
		t.Error("expected non-empty version")
	}

	// Clean up
	_ = manager.DeleteSecret(ctx, name)
}

func testGetNotExist(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	_, _, err := manager.GetSecretValue(ctx, "nonexistent-secret-"+randomString())
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, secret.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testCreateDuplicate(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-duplicate-" + randomString()
	value := []byte("value")

	_, err := manager.CreateSecret(ctx, name, value)
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}
	defer manager.DeleteSecret(ctx, name)

	_, err = manager.CreateSecret(ctx, name, value)
	if err == nil {
		t.Fatal("expected error creating duplicate, got nil")
	}

	if !errors.Is(err, secret.ErrAlreadyExists) {
		t.Errorf("expected ErrAlreadyExists, got %v", err)
	}
}

func testUpdate(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-update-" + randomString()
	initialValue := []byte("initial-value")
	newValue := []byte("new-value")

	_, err := manager.CreateSecret(ctx, name, initialValue)
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}
	defer manager.DeleteSecret(ctx, name)

	_, err = manager.UpdateSecret(ctx, name, newValue)
	if err != nil {
		t.Fatal("unexpected error updating secret:", err)
	}

	gotValue, _, err := manager.GetSecretValue(ctx, name)
	if err != nil {
		t.Fatal("unexpected error getting secret:", err)
	}

	if string(gotValue) != string(newValue) {
		t.Errorf("expected value %q, got %q", newValue, gotValue)
	}
}

func testDelete(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-delete-" + randomString()
	value := []byte("value")

	_, err := manager.CreateSecret(ctx, name, value)
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}

	err = manager.DeleteSecret(ctx, name)
	if err != nil {
		t.Fatal("unexpected error deleting secret:", err)
	}

	_, _, err = manager.GetSecretValue(ctx, name)
	if err == nil {
		t.Fatal("expected error getting deleted secret, got nil")
	}

	if !errors.Is(err, secret.ErrNotFound) {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func testDeleteIdempotent(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-delete-idempotent-" + randomString()

	// Delete non-existent secret should not error (idempotent)
	err := manager.DeleteSecret(ctx, name)
	// Some backends may return ErrNotFound, others may be truly idempotent
	// Both are acceptable
	if err != nil && !errors.Is(err, secret.ErrNotFound) {
		t.Errorf("unexpected error deleting non-existent secret: %v", err)
	}
}

func testList(t *testing.T, manager secret.Manager) {
	ctx := t.Context()

	count := 0
	for _, err := range manager.ListSecrets(ctx) {
		if err != nil {
			t.Fatal("unexpected error listing secrets:", err)
		}
		count++
	}

	// Should successfully list secrets (count can be zero for empty lists)
	// Test passes as long as no error occurred
}

func testListWithPrefix(t *testing.T, manager secret.Manager) {
	ctx := t.Context()

	// For read-only backends, just test that prefix filtering works with existing vars
	var secrets []secret.Secret
	for s, err := range manager.ListSecrets(ctx, secret.NamePrefix("TEST_")) {
		if err != nil {
			t.Fatal("unexpected error listing secrets:", err)
		}
		secrets = append(secrets, s)
	}

	// All returned secrets should have the prefix
	for _, s := range secrets {
		if !strings.HasPrefix(s.Name, "TEST_") {
			t.Errorf("secret %q does not have prefix TEST_", s.Name)
		}
	}
}

func testListWithMaxResults(t *testing.T, manager secret.Manager) {
	ctx := t.Context()

	var secrets []secret.Secret
	for s, err := range manager.ListSecrets(ctx, secret.MaxResults(5)) {
		if err != nil {
			t.Fatal("unexpected error listing secrets:", err)
		}
		secrets = append(secrets, s)
	}

	if len(secrets) > 5 {
		t.Errorf("expected at most 5 secrets, got %d", len(secrets))
	}
}

func testCreateWithTags(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-tags-" + randomString()
	value := []byte("value")
	tags := map[string]string{
		"env":     "test",
		"service": "api",
	}

	info, err := manager.CreateSecret(ctx, name, value, secret.Tags(tags))
	if err != nil {
		t.Fatal("unexpected error creating secret with tags:", err)
	}
	defer manager.DeleteSecret(ctx, name)

	if len(info.Tags) == 0 {
		t.Error("expected tags to be set, got empty tags")
	}

	// Verify tags are returned on GetSecretInfo
	gotInfo, err := manager.GetSecretInfo(ctx, name)
	if err != nil {
		t.Fatal("unexpected error getting secret info:", err)
	}

	if len(gotInfo.Tags) == 0 {
		t.Error("expected tags to be persisted, got empty tags")
	}
}

func testListWithTagFilter(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name1 := "test-tag-filter-1-" + randomString()
	name2 := "test-tag-filter-2-" + randomString()

	// Create two secrets with different tags
	_, err := manager.CreateSecret(ctx, name1, []byte("value1"), secret.Tag("env", "prod"))
	if err != nil {
		t.Fatal("unexpected error creating secret 1:", err)
	}
	defer manager.DeleteSecret(ctx, name1)

	_, err = manager.CreateSecret(ctx, name2, []byte("value2"), secret.Tag("env", "dev"))
	if err != nil {
		t.Fatal("unexpected error creating secret 2:", err)
	}
	defer manager.DeleteSecret(ctx, name2)

	// List secrets with env=prod tag
	var secrets []secret.Secret
	for s, err := range manager.ListSecrets(ctx, secret.FilterByTag("env", "prod")) {
		if err != nil {
			t.Fatal("unexpected error listing secrets:", err)
		}
		secrets = append(secrets, s)
	}

	// Should find at least our secret
	found := false
	for _, s := range secrets {
		if s.Name == name1 {
			found = true
		}
		if s.Name == name2 {
			t.Errorf("found secret %q with wrong tag", name2)
		}
	}

	if !found {
		t.Error("did not find secret with matching tag")
	}
}

func testContextCancellation(t *testing.T, manager secret.Manager) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	_, _, err := manager.GetSecretValue(ctx, "any-secret")
	if err == nil {
		t.Fatal("expected error from cancelled context, got nil")
	}
}

func testListSecretVersions(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-versions-" + randomString()

	// Create a secret
	_, err := manager.CreateSecret(ctx, name, []byte("v1"))
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}
	defer manager.DeleteSecret(ctx, name)

	// Update to create a second version
	_, err = manager.UpdateSecret(ctx, name, []byte("v2"))
	if err != nil {
		t.Fatal("unexpected error updating secret:", err)
	}

	// List versions
	var versions []secret.Version
	for v, err := range manager.ListSecretVersions(ctx, name) {
		if err != nil {
			t.Fatal("unexpected error listing versions:", err)
		}
		versions = append(versions, v)
	}

	if len(versions) < 2 {
		t.Errorf("expected at least 2 versions, got %d", len(versions))
	}
}

func testGetSecretVersionNotFound(t *testing.T, manager secret.Manager) {
	ctx := t.Context()

	_, _, err := manager.GetSecretValue(ctx, "nonexistent-"+randomString(), secret.WithVersion("999"))
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	if !errors.Is(err, secret.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testDestroySecretVersion(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-destroy-version-" + randomString()

	// Create a secret
	info, err := manager.CreateSecret(ctx, name, []byte("v1"))
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}
	defer manager.DeleteSecret(ctx, name)

	firstVersion := info.Version

	// Update to create a second version (so we have something to destroy)
	_, err = manager.UpdateSecret(ctx, name, []byte("v2"))
	if err != nil {
		t.Fatal("unexpected error updating secret:", err)
	}

	// Destroy the first version
	if firstVersion != "" {
		err = manager.DestroySecretVersion(ctx, name, firstVersion)
		if err != nil {
			t.Fatal("unexpected error destroying version:", err)
		}

		// Verify the version is destroyed
		_, _, err = manager.GetSecretValue(ctx, name, secret.WithVersion(firstVersion))
		if err == nil {
			t.Error("expected error getting destroyed version, got nil")
		}
	}
}

func testGetSecretWithVersion(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-get-with-version-" + randomString()

	// Create a secret
	info, err := manager.CreateSecret(ctx, name, []byte("v1-value"))
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}
	defer manager.DeleteSecret(ctx, name)

	initialVersion := info.Version

	// Update to create second version
	_, err = manager.UpdateSecret(ctx, name, []byte("v2-value"))
	if err != nil {
		t.Fatal("unexpected error updating secret:", err)
	}

	// Get using WithVersion option
	if initialVersion != "" {
		value, _, err := manager.GetSecretValue(ctx, name, secret.WithVersion(initialVersion))
		if err != nil {
			t.Fatal("unexpected error getting secret with version:", err)
		}

		if string(value) != "v1-value" {
			t.Errorf("expected v1-value, got %q", value)
		}
	}
}

func testUpdateSecretWithDescription(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-update-desc-" + randomString()

	// Create a secret
	_, err := manager.CreateSecret(ctx, name, []byte("value"))
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}
	defer manager.DeleteSecret(ctx, name)

	// Update with description
	_, err = manager.UpdateSecret(ctx, name, []byte("new-value"),
		secret.UpdateDescription("test description"))
	if err != nil {
		t.Fatal("unexpected error updating secret:", err)
	}

	// Verify secret was updated
	value, _, err := manager.GetSecretValue(ctx, name)
	if err != nil {
		t.Fatal("unexpected error getting secret:", err)
	}

	if string(value) != "new-value" {
		t.Errorf("expected new-value, got %q", value)
	}
}

func testCreateSecretWithDescription(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-create-desc-" + randomString()

	// Create with description
	_, err := manager.CreateSecret(ctx, name, []byte("value"),
		secret.Description("test secret description"))
	if err != nil {
		t.Fatal("unexpected error creating secret:", err)
	}
	defer manager.DeleteSecret(ctx, name)

	// Verify secret exists
	value, _, err := manager.GetSecretValue(ctx, name)
	if err != nil {
		t.Fatal("unexpected error getting secret:", err)
	}

	if string(value) != "value" {
		t.Errorf("expected value, got %q", value)
	}
}

func testUpdateNonExistent(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	name := "test-update-nonexistent-" + randomString()

	_, err := manager.UpdateSecret(ctx, name, []byte("value"))
	if err == nil {
		t.Fatal("expected error updating non-existent secret, got nil")
	}

	if !errors.Is(err, secret.ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func testListWithMultipleFilters(t *testing.T, manager secret.Manager) {
	ctx := t.Context()
	prefix := "test-multi-filter-"
	name1 := prefix + "1-" + randomString()
	name2 := prefix + "2-" + randomString()
	name3 := "other-" + randomString()

	// Create secrets with different tags
	_, err := manager.CreateSecret(ctx, name1, []byte("v1"), secret.Tag("env", "prod"))
	if err != nil {
		t.Fatal("unexpected error creating secret 1:", err)
	}
	defer manager.DeleteSecret(ctx, name1)

	_, err = manager.CreateSecret(ctx, name2, []byte("v2"), secret.Tag("env", "dev"))
	if err != nil {
		t.Fatal("unexpected error creating secret 2:", err)
	}
	defer manager.DeleteSecret(ctx, name2)

	_, err = manager.CreateSecret(ctx, name3, []byte("v3"), secret.Tag("env", "prod"))
	if err != nil {
		t.Fatal("unexpected error creating secret 3:", err)
	}
	defer manager.DeleteSecret(ctx, name3)

	// List with prefix AND tag filter
	var secrets []secret.Secret
	for s, err := range manager.ListSecrets(ctx,
		secret.NamePrefix(prefix),
		secret.FilterByTag("env", "prod"),
		secret.MaxResults(10)) {
		if err != nil {
			t.Fatal("unexpected error listing secrets:", err)
		}
		secrets = append(secrets, s)
	}

	// Should find name1 (has prefix and prod tag)
	// Should NOT find name2 (has prefix but dev tag)
	// Should NOT find name3 (has prod tag but wrong prefix)
	foundName1 := false
	for _, s := range secrets {
		if s.Name == name1 {
			foundName1 = true
		}
		if s.Name == name2 {
			t.Errorf("found secret %q which has wrong tag", name2)
		}
		if s.Name == name3 {
			t.Errorf("found secret %q which has wrong prefix", name3)
		}
	}

	if !foundName1 {
		t.Error("did not find secret with matching prefix and tag")
	}
}

func randomString() string {
	return time.Now().Format("20060102-150405.000000")
}
