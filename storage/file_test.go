package storage_test

import (
	"bytes"
	"context"
	"io"
	"io/fs"
	"strings"
	"testing"
	"testing/fstest"
	"testing/iotest"

	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/storage/memory"
)

type prefixFS struct {
	base   fs.FS
	prefix string
}

func (fsys *prefixFS) Open(name string) (fs.File, error) {
	return fsys.base.Open(fsys.prefix + name)
}

// TestFS tests the fs.FS implementation backed by a storage.Bucket.
//
// We only validate a structure with top-level objects because object stores
// don't have a concept of directories, which makes some of the tests impossible
// to pass (e.g., we can't guess the mode of a valid upfront without listing its
// content to determine if it's a leaf object).
func TestFS(t *testing.T) {
	bucket := new(memory.Bucket)
	bucket.PutObject(t.Context(), "test-1", strings.NewReader("ABC"))
	bucket.PutObject(t.Context(), "test-2", strings.NewReader("DE"))
	bucket.PutObject(t.Context(), "test-3", strings.NewReader("FGHIJKL"))

	fsys := storage.FS(t.Context(), storage.SingleBucketRegistry(bucket))

	if err := fstest.TestFS(&prefixFS{base: fsys, prefix: ":memory:"},
		"test-1",
		"test-2",
		"test-3",
	); err != nil {
		t.Error(err)
	}
}

func TestFile(t *testing.T) {
	bucket := new(memory.Bucket)
	bucket.PutObject(t.Context(), "test", strings.NewReader("hello, world!"))

	file := storage.NewFile(context.Background(), bucket, "test", 13)
	if file.Size() != 13 {
		t.Fatalf("unexpected file size: %d != %d", file.Size(), 13)
	}
	if file.Name() != ":memory:test" {
		t.Fatalf("unexpected file name: %q != %q", file.Name(), ":memory:test")
	}

	if b, err := io.ReadAll(io.NewSectionReader(file, 0, 5)); err != nil {
		t.Fatal("unexpected error reading file:", err)
	} else if string(b) != "hello" {
		t.Fatalf("unexpected file data: %q != %q", b, "hello")
	}

	if b, err := io.ReadAll(io.NewSectionReader(file, 7, 5)); err != nil {
		t.Fatal("unexpected error reading file:", err)
	} else if string(b) != "world" {
		t.Fatalf("unexpected file data: %q != %q", b, "world")
	}
}

func TestFileSeekAndWriteTo(t *testing.T) {
	content := []byte("hello, world!")
	bucket := new(memory.Bucket)
	bucket.PutObject(t.Context(), "test", bytes.NewReader(content))

	fsys := storage.FS(t.Context(), storage.SingleBucketRegistry(bucket))
	f, err := fsys.Open(":memory:test")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// iotest.TestReader exercises Read, ReadAt, and Seek
	if err := iotest.TestReader(f.(io.Reader), content); err != nil {
		t.Fatal(err)
	}

	// Test WriteTo separately since iotest doesn't cover it
	f2, _ := fsys.Open(":memory:test")
	defer f2.Close()

	var buf bytes.Buffer
	wt := f2.(io.WriterTo)
	n, err := wt.WriteTo(&buf)
	if err != nil {
		t.Fatal("WriteTo error:", err)
	}
	if n != int64(len(content)) {
		t.Fatalf("WriteTo wrote %d bytes, want %d", n, len(content))
	}
	if buf.String() != string(content) {
		t.Fatalf("WriteTo content = %q, want %q", buf.String(), content)
	}

	// Test WriteTo after Seek
	f3, _ := fsys.Open(":memory:test")
	defer f3.Close()

	seeker := f3.(io.Seeker)
	if _, err := seeker.Seek(7, io.SeekStart); err != nil {
		t.Fatal("Seek error:", err)
	}

	buf.Reset()
	_, err = f3.(io.WriterTo).WriteTo(&buf)
	if err != nil {
		t.Fatal("WriteTo after Seek error:", err)
	}
	if buf.String() != "world!" {
		t.Fatalf("WriteTo after Seek = %q, want %q", buf.String(), "world!")
	}

	// Test Seek with io.SeekEnd
	f4, _ := fsys.Open(":memory:test")
	defer f4.Close()

	seeker = f4.(io.Seeker)
	pos, err := seeker.Seek(-6, io.SeekEnd)
	if err != nil {
		t.Fatal("Seek from end error:", err)
	}
	if pos != 7 {
		t.Fatalf("Seek from end position = %d, want 7", pos)
	}

	// Test Seek with invalid whence
	_, err = seeker.Seek(0, 999)
	if err == nil {
		t.Fatal("expected error for invalid whence")
	}

	// Test Seek to negative position
	_, err = seeker.Seek(-100, io.SeekStart)
	if err == nil {
		t.Fatal("expected error for negative position")
	}

	// Test WriteTo at EOF
	f5, _ := fsys.Open(":memory:test")
	defer f5.Close()

	seeker = f5.(io.Seeker)
	seeker.Seek(0, io.SeekEnd) // seek to end

	buf.Reset()
	n, err = f5.(io.WriterTo).WriteTo(&buf)
	if err != nil {
		t.Fatal("WriteTo at EOF error:", err)
	}
	if n != 0 {
		t.Fatalf("WriteTo at EOF wrote %d bytes, want 0", n)
	}
}
