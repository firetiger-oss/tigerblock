package secret

import (
	"testing"
	"time"
)

func TestCreateOptions(t *testing.T) {
	t.Run("Tags", func(t *testing.T) {
		tags := map[string]string{"env": "prod", "service": "api"}
		opts := NewCreateOptions(Tags(tags))

		if len(opts.Tags()) != 2 {
			t.Errorf("expected 2 tags, got %d", len(opts.Tags()))
		}
		if opts.Tags()["env"] != "prod" {
			t.Errorf("expected env=prod, got %s", opts.Tags()["env"])
		}
	})

	t.Run("Tag", func(t *testing.T) {
		opts := NewCreateOptions(Tag("env", "prod"), Tag("service", "api"))

		if len(opts.Tags()) != 2 {
			t.Errorf("expected 2 tags, got %d", len(opts.Tags()))
		}
		if opts.Tags()["env"] != "prod" {
			t.Errorf("expected env=prod, got %s", opts.Tags()["env"])
		}
	})

	t.Run("Description", func(t *testing.T) {
		opts := NewCreateOptions(Description("test description"))

		if opts.Description() != "test description" {
			t.Errorf("expected 'test description', got %q", opts.Description())
		}
	})

	t.Run("ExpiresAt", func(t *testing.T) {
		expiry := time.Now().Add(24 * time.Hour)
		opts := NewCreateOptions(ExpiresAt(expiry))

		if !opts.ExpiresAt().Equal(expiry) {
			t.Errorf("expected expiry %v, got %v", expiry, opts.ExpiresAt())
		}
	})

	t.Run("Combined", func(t *testing.T) {
		expiry := time.Now().Add(24 * time.Hour)
		opts := NewCreateOptions(
			Tag("env", "prod"),
			Description("combined test"),
			ExpiresAt(expiry),
		)

		if len(opts.Tags()) != 1 {
			t.Errorf("expected 1 tag, got %d", len(opts.Tags()))
		}
		if opts.Description() != "combined test" {
			t.Errorf("expected 'combined test', got %q", opts.Description())
		}
		if !opts.ExpiresAt().Equal(expiry) {
			t.Errorf("expected expiry %v, got %v", expiry, opts.ExpiresAt())
		}
	})
}

func TestGetOptions(t *testing.T) {
	t.Run("WithVersion", func(t *testing.T) {
		opts := NewGetOptions(WithVersion("v1"))

		if opts.Version() != "v1" {
			t.Errorf("expected version 'v1', got %q", opts.Version())
		}
	})

	t.Run("Empty", func(t *testing.T) {
		opts := NewGetOptions()

		if opts.Version() != "" {
			t.Errorf("expected empty version, got %q", opts.Version())
		}
	})
}

func TestUpdateOptions(t *testing.T) {
	t.Run("UpdateDescription", func(t *testing.T) {
		opts := NewUpdateOptions(UpdateDescription("updated description"))

		if opts.Description() != "updated description" {
			t.Errorf("expected 'updated description', got %q", opts.Description())
		}
	})

	t.Run("Empty", func(t *testing.T) {
		opts := NewUpdateOptions()

		if opts.Description() != "" {
			t.Errorf("expected empty description, got %q", opts.Description())
		}
	})
}

func TestListOptions(t *testing.T) {
	t.Run("NamePrefix", func(t *testing.T) {
		opts := NewListOptions(NamePrefix("test-"))

		if opts.NamePrefix() != "test-" {
			t.Errorf("expected prefix 'test-', got %q", opts.NamePrefix())
		}
	})

	t.Run("FilterByTag", func(t *testing.T) {
		opts := NewListOptions(FilterByTag("env", "prod"))

		if len(opts.Tags()) != 1 {
			t.Errorf("expected 1 tag, got %d", len(opts.Tags()))
		}
		if opts.Tags()["env"] != "prod" {
			t.Errorf("expected env=prod, got %s", opts.Tags()["env"])
		}
	})

	t.Run("FilterByTags", func(t *testing.T) {
		tags := map[string]string{"env": "prod", "service": "api"}
		opts := NewListOptions(FilterByTags(tags))

		if len(opts.Tags()) != 2 {
			t.Errorf("expected 2 tags, got %d", len(opts.Tags()))
		}
		if opts.Tags()["env"] != "prod" {
			t.Errorf("expected env=prod, got %s", opts.Tags()["env"])
		}
	})

	t.Run("MaxResults", func(t *testing.T) {
		opts := NewListOptions(MaxResults(10))

		if opts.MaxResults() != 10 {
			t.Errorf("expected max results 10, got %d", opts.MaxResults())
		}
	})

	t.Run("Combined", func(t *testing.T) {
		opts := NewListOptions(
			NamePrefix("test-"),
			FilterByTag("env", "prod"),
			MaxResults(5),
		)

		if opts.NamePrefix() != "test-" {
			t.Errorf("expected prefix 'test-', got %q", opts.NamePrefix())
		}
		if len(opts.Tags()) != 1 {
			t.Errorf("expected 1 tag, got %d", len(opts.Tags()))
		}
		if opts.MaxResults() != 5 {
			t.Errorf("expected max results 5, got %d", opts.MaxResults())
		}
	})
}

func TestListVersionOptions(t *testing.T) {
	t.Run("MaxVersions", func(t *testing.T) {
		opts := NewListVersionOptions(MaxVersions(10))

		if opts.MaxResults() != 10 {
			t.Errorf("expected max results 10, got %d", opts.MaxResults())
		}
	})

	t.Run("FilterByState", func(t *testing.T) {
		opts := NewListVersionOptions(FilterByState(VersionStateEnabled, VersionStateDisabled))

		if len(opts.States()) != 2 {
			t.Errorf("expected 2 states, got %d", len(opts.States()))
		}
		if opts.States()[0] != VersionStateEnabled {
			t.Errorf("expected first state to be enabled, got %v", opts.States()[0])
		}
	})

	t.Run("Combined", func(t *testing.T) {
		opts := NewListVersionOptions(
			MaxVersions(5),
			FilterByState(VersionStateEnabled),
		)

		if opts.MaxResults() != 5 {
			t.Errorf("expected max results 5, got %d", opts.MaxResults())
		}
		if len(opts.States()) != 1 {
			t.Errorf("expected 1 state, got %d", len(opts.States()))
		}
	})
}
