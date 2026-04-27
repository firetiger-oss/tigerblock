package file

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestLinkThenUnlink(t *testing.T) {
	dir := t.TempDir()

	t.Run("moves source to target and removes source", func(t *testing.T) {
		src := filepath.Join(dir, "move-src")
		dst := filepath.Join(dir, "move-dst")
		if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
			t.Fatal(err)
		}

		if err := linkThenUnlink(src, dst); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if _, err := os.Stat(src); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("expected source removed, stat err = %v", err)
		}
		got, err := os.ReadFile(dst)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "hello" {
			t.Errorf("expected target content preserved, got %q", got)
		}
	})

	t.Run("returns fs.ErrExist when target exists", func(t *testing.T) {
		src := filepath.Join(dir, "exists-src")
		dst := filepath.Join(dir, "exists-dst")
		if err := os.WriteFile(src, []byte("new"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, []byte("old"), 0o644); err != nil {
			t.Fatal(err)
		}

		err := linkThenUnlink(src, dst)
		if !errors.Is(err, fs.ErrExist) {
			t.Fatalf("expected fs.ErrExist, got %v", err)
		}

		// Source must remain so a caller can surface the error without
		// losing the in-flight write.
		if _, statErr := os.Stat(src); statErr != nil {
			t.Errorf("expected source preserved on failure, stat err = %v", statErr)
		}
		got, err := os.ReadFile(dst)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "old" {
			t.Errorf("expected target unchanged, got %q", got)
		}
	})

	t.Run("returns unlink error and rolls back target", func(t *testing.T) {
		src := filepath.Join(dir, "rollback-src")
		dst := filepath.Join(dir, "rollback-dst")
		if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
			t.Fatal(err)
		}

		unlinkErr := errors.New("unlink failed")
		err := linkThenUnlinkWithOps(src, dst, os.Link, func(name string) error {
			if name == src {
				return unlinkErr
			}
			return os.Remove(name)
		})
		if !errors.Is(err, unlinkErr) {
			t.Fatalf("expected unlink error, got %v", err)
		}

		if _, statErr := os.Stat(src); statErr != nil {
			t.Fatalf("expected source preserved after rollback, stat err = %v", statErr)
		}
		if _, statErr := os.Stat(dst); !errors.Is(statErr, fs.ErrNotExist) {
			t.Fatalf("expected target removed by rollback, stat err = %v", statErr)
		}
	})

	t.Run("returns unlink and rollback errors when rollback fails", func(t *testing.T) {
		src := filepath.Join(dir, "rollback-fail-src")
		dst := filepath.Join(dir, "rollback-fail-dst")
		if err := os.WriteFile(src, []byte("hello"), 0o644); err != nil {
			t.Fatal(err)
		}

		unlinkErr := errors.New("unlink failed")
		rollbackErr := errors.New("rollback failed")
		err := linkThenUnlinkWithOps(src, dst, os.Link, func(name string) error {
			switch name {
			case src:
				return unlinkErr
			case dst:
				return rollbackErr
			default:
				return os.Remove(name)
			}
		})
		if !errors.Is(err, unlinkErr) {
			t.Fatalf("expected unlink error, got %v", err)
		}
		if !errors.Is(err, rollbackErr) {
			t.Fatalf("expected rollback error, got %v", err)
		}

		if _, statErr := os.Stat(src); statErr != nil {
			t.Fatalf("expected source to remain, stat err = %v", statErr)
		}
		if _, statErr := os.Stat(dst); statErr != nil {
			t.Fatalf("expected target to remain when rollback fails, stat err = %v", statErr)
		}
	})
}

func TestIsRenameFlagUnsupported(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"einval", unix.EINVAL, true},
		{"enosys", unix.ENOSYS, true},
		{"eopnotsupp", unix.EOPNOTSUPP, true},
		{"fs.ErrInvalid", fs.ErrInvalid, true},
		{"fs.ErrExist is not unsupported", fs.ErrExist, false},
		{"nil", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRenameFlagUnsupported(tc.err); got != tc.want {
				t.Errorf("isRenameFlagUnsupported(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
