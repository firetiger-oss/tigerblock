package secret

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/firetiger-oss/storage/memory"
)

func TestRegister(t *testing.T) {
	// Save original registry
	globalMutex.Lock()
	originalRegistries := globalRegistries
	globalRegistries = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalRegistries = originalRegistries
		globalMutex.Unlock()
	}()

	mockReg := &mockRegistry{}
	Register(`^test://`, mockReg)

	globalMutex.RLock()
	count := len(globalRegistries)
	globalMutex.RUnlock()

	if count != 1 {
		t.Errorf("expected 1 registry, got %d", count)
	}
}

func TestLoadManager(t *testing.T) {
	// Register a test backend
	globalMutex.Lock()
	originalRegistries := globalRegistries
	globalRegistries = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalRegistries = originalRegistries
		globalMutex.Unlock()
	}()

	mockReg := &mockRegistry{
		manager: NewManager(memory.NewBucket()),
	}
	Register(`^test://`, mockReg)

	ctx := t.Context()
	manager, err := LoadManager(ctx, "test://location")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if manager == nil {
		t.Fatal("expected manager to be returned")
	}
}

func TestLoadManagerNoMatch(t *testing.T) {
	globalMutex.Lock()
	originalRegistries := globalRegistries
	globalRegistries = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalRegistries = originalRegistries
		globalMutex.Unlock()
	}()

	ctx := t.Context()
	_, err := LoadManager(ctx, "unknown://location")
	if err == nil {
		t.Fatal("expected error for unknown identifier")
	}

	if !strings.Contains(err.Error(), "no registry found") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestLoadManagerEmpty(t *testing.T) {
	ctx := t.Context()
	_, err := LoadManager(ctx, "")
	if err == nil {
		t.Fatal("expected error for empty identifier")
	}

	if !strings.Contains(err.Error(), "required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestCreateGlobalFunction(t *testing.T) {
	globalMutex.Lock()
	originalRegistries := globalRegistries
	globalRegistries = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalRegistries = originalRegistries
		globalMutex.Unlock()
	}()

	ctx := t.Context()
	mgr := NewManager(memory.NewBucket())
	mockReg := &mockRegistry{manager: mgr}
	Register(`^test://`, mockReg)

	info, err := Create(ctx, "test://location/my-secret", Value("value"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.Name != "my-secret" {
		t.Errorf("expected name 'my-secret', got %q", info.Name)
	}

	// Verify secret was created via the API
	_, _, err = mgr.GetSecret(ctx, "my-secret")
	if err != nil {
		t.Error("expected secret to be created")
	}
}

func TestGetGlobalFunction(t *testing.T) {
	globalMutex.Lock()
	originalRegistries := globalRegistries
	globalRegistries = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalRegistries = originalRegistries
		globalMutex.Unlock()
	}()

	ctx := t.Context()
	mgr := NewManager(memory.NewBucket())
	mgr.CreateSecret(ctx, "my-secret", Value("value"))
	mockReg := &mockRegistry{manager: mgr}
	Register(`^test://`, mockReg)

	value, info, err := Get(ctx, "test://location/my-secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if string(value) != "value" {
		t.Errorf("expected value 'value', got %q", value)
	}

	if info.Name != "my-secret" {
		t.Errorf("expected name 'my-secret', got %q", info.Name)
	}
}

func TestUpdateGlobalFunction(t *testing.T) {
	globalMutex.Lock()
	originalRegistries := globalRegistries
	globalRegistries = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalRegistries = originalRegistries
		globalMutex.Unlock()
	}()

	ctx := t.Context()
	mgr := NewManager(memory.NewBucket())
	mgr.CreateSecret(ctx, "my-secret", Value("old-value"))
	mockReg := &mockRegistry{manager: mgr}
	Register(`^test://`, mockReg)

	info, err := Update(ctx, "test://location/my-secret", Value("new-value"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if info.Name != "my-secret" {
		t.Errorf("expected name 'my-secret', got %q", info.Name)
	}

	// Verify value was updated via the API
	value, _, err := mgr.GetSecret(ctx, "my-secret")
	if err != nil {
		t.Fatalf("unexpected error getting secret: %v", err)
	}
	if string(value) != "new-value" {
		t.Errorf("expected updated value, got %q", value)
	}
}

func TestDeleteGlobalFunction(t *testing.T) {
	globalMutex.Lock()
	originalRegistries := globalRegistries
	globalRegistries = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalRegistries = originalRegistries
		globalMutex.Unlock()
	}()

	ctx := t.Context()
	mgr := NewManager(memory.NewBucket())
	mgr.CreateSecret(ctx, "my-secret", Value("value"))
	mockReg := &mockRegistry{manager: mgr}
	Register(`^test://`, mockReg)

	err := Delete(ctx, "test://location/my-secret")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify secret was deleted via the API
	_, _, err = mgr.GetSecret(ctx, "my-secret")
	if err != ErrNotFound {
		t.Error("expected secret to be deleted")
	}
}

func TestListGlobalFunction(t *testing.T) {
	globalMutex.Lock()
	originalRegistries := globalRegistries
	globalRegistries = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalRegistries = originalRegistries
		globalMutex.Unlock()
	}()

	ctx := t.Context()
	mgr := NewManager(memory.NewBucket())
	mgr.CreateSecret(ctx, "secret1", Value("value1"))
	mgr.CreateSecret(ctx, "secret2", Value("value2"))
	mockReg := &mockRegistry{manager: mgr}
	Register(`^test://`, mockReg)

	count := 0
	for _, err := range List(ctx, "test://location") {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		count++
	}

	if count != 2 {
		t.Errorf("expected 2 secrets, got %d", count)
	}
}

func TestInstall(t *testing.T) {
	// Save original adapters
	globalMutex.Lock()
	originalAdapters := globalAdapters
	globalAdapters = nil
	globalMutex.Unlock()

	defer func() {
		globalMutex.Lock()
		globalAdapters = originalAdapters
		globalMutex.Unlock()
	}()

	adapter := AdapterFunc(func(m Manager) Manager { return m })
	Install(adapter)

	globalMutex.RLock()
	count := len(globalAdapters)
	globalMutex.RUnlock()

	if count != 1 {
		t.Errorf("expected 1 adapter, got %d", count)
	}
}

func TestAdaptManager(t *testing.T) {
	base := NewManager(memory.NewBucket())

	callCount := 0
	adapter1 := AdapterFunc(func(m Manager) Manager {
		callCount++
		return m
	})
	adapter2 := AdapterFunc(func(m Manager) Manager {
		callCount++
		return m
	})

	result := AdaptManager(base, adapter1, adapter2)

	if result == nil {
		t.Fatal("expected non-nil result")
	}

	if callCount != 2 {
		t.Errorf("expected 2 adapter calls, got %d", callCount)
	}
}

func TestAdapterFunc(t *testing.T) {
	base := NewManager(memory.NewBucket())

	called := false
	adapter := AdapterFunc(func(m Manager) Manager {
		called = true
		return m
	})

	result := adapter.AdaptManager(base)

	if !called {
		t.Error("expected adapter function to be called")
	}

	if result != base {
		t.Error("expected adapter to return the same manager")
	}
}

// mockRegistry implements Registry for testing
type mockRegistry struct {
	manager Manager
}

func (r *mockRegistry) LoadManager(ctx context.Context, identifier string) (Manager, error) {
	return r.manager, nil
}

func (r *mockRegistry) ParseSecret(identifier string) (string, string, error) {
	// Simple parsing: test://location/name
	rest, ok := strings.CutPrefix(identifier, "test://")
	if !ok {
		return "", "", fmt.Errorf("invalid test identifier")
	}
	location, secretName, ok := strings.Cut(rest, "/")
	if !ok {
		// No secret name, just manager
		return identifier, "", nil
	}
	managerID := "test://" + location
	return managerID, secretName, nil
}
