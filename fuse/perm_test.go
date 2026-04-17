package fuse

import (
	"io/fs"
	"testing"

	storage "github.com/firetiger-oss/storage"
)

func TestReadModeRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata map[string]string
		fallback uint32
		want     uint32
	}{
		{"empty falls back", nil, 0o644, 0o644},
		{"missing key falls back", map[string]string{"other": "1"}, 0o755, 0o755},
		{"octal parses", map[string]string{"mode": "600"}, 0, 0o600},
		{"with leading zero parses", map[string]string{"mode": "0755"}, 0, 0o755},
		{"sticky bit preserved", map[string]string{"mode": "1755"}, 0, 0o1755},
		{"setuid bit preserved", map[string]string{"mode": "4755"}, 0, 0o4755},
		{"masks above 0o7777", map[string]string{"mode": "170755"}, 0, 0o755},
		{"bad string falls back", map[string]string{"mode": "not-a-number"}, 0o644, 0o644},
		{"empty string falls back", map[string]string{"mode": ""}, 0o600, 0o600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := readMode(tc.metadata, tc.fallback)
			if got != tc.want {
				t.Errorf("readMode(%v, %o) = %o, want %o", tc.metadata, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestReadID(t *testing.T) {
	for _, tc := range []struct {
		name     string
		metadata map[string]string
		key      string
		fallback uint32
		want     uint32
	}{
		{"empty falls back", nil, "uid", 1000, 1000},
		{"parsed", map[string]string{"uid": "42"}, "uid", 0, 42},
		{"different key", map[string]string{"gid": "42"}, "uid", 99, 99},
		{"bad value falls back", map[string]string{"uid": "negative"}, "uid", 7, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := readID(tc.metadata, tc.key, tc.fallback)
			if got != tc.want {
				t.Errorf("readID(%v, %q, %d) = %d, want %d", tc.metadata, tc.key, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestModeBitsFromFS(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   fs.FileMode
		want uint32
	}{
		{"plain perm", 0o644, 0o644},
		{"dir bit stripped", fs.ModeDir | 0o755, 0o755},
		{"setuid", fs.ModeSetuid | 0o755, 0o4755},
		{"setgid", fs.ModeSetgid | 0o755, 0o2755},
		{"sticky", fs.ModeSticky | 0o755, 0o1755},
		{"all special", fs.ModeSetuid | fs.ModeSetgid | fs.ModeSticky | 0o755, 0o7755},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := modeBitsFromFS(tc.in)
			if got != tc.want {
				t.Errorf("modeBitsFromFS(%v) = %o, want %o", tc.in, got, tc.want)
			}
		})
	}
}

func TestPutOptionRoundTrip(t *testing.T) {
	opts := []storage.PutOption{
		modePutOption(0o640),
		uidPutOption(1001),
		gidPutOption(2002),
	}
	m := storage.NewPutOptions(opts...).Metadata()
	if got := readMode(m, 0); got != 0o640 {
		t.Errorf("mode: got %o, want %o", got, 0o640)
	}
	if got := readID(m, metadataKeyUID, 0); got != 1001 {
		t.Errorf("uid: got %d, want 1001", got)
	}
	if got := readID(m, metadataKeyGID, 0); got != 2002 {
		t.Errorf("gid: got %d, want 2002", got)
	}
}
