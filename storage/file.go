package storage

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"path"
	"strings"
	"time"

	"github.com/firetiger-oss/tigerblock/uri"
)

// Named constants for unix permission bits.
const (
	fileModeRead    fs.FileMode = 0444
	fileModeWrite   fs.FileMode = 0222
	fileModeExecute fs.FileMode = 0111
)

// FS constructs a fs.FS using the given registry to load objects from buckets
// based on their URI location.
//
// The context is used for all operations on the registry and buckets that it
// loads, cancelling it will abort all inflight I/O on the filesystem.
func FS(ctx context.Context, reg Registry) fs.FS {
	return &fileSystem{
		ctx: ctx,
		reg: reg,
	}
}

type fileSystem struct {
	ctx context.Context
	reg Registry
}

func (fsys *fileSystem) Open(name string) (fs.File, error) {
	if strings.HasPrefix(name, ":memory:") { // for testing/fstest
		path := name[8:]
		if !fs.ValidPath(path) {
			return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
		}
	}
	bucket, objectKey, err := loadBucketAndObjectKey(fsys.ctx, fsys.reg, name)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	// Enforce the io/fs.FS contract: Open must reject any name where
	// !fs.ValidPath(name). storage.ValidObjectKey is looser than fs.ValidPath
	// (it accepts trailing-slash directory markers), so we re-apply the
	// stricter fs.ValidPath check at this boundary.
	if !fs.ValidPath(objectKey) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrInvalid}
	}
	var f fs.File
	if objectKey == "." {
		f = &dir{
			ctx:    fsys.ctx,
			bucket: bucket,
		}
	} else {
		f = &file{
			File: File{
				ctx:    fsys.ctx,
				bucket: bucket,
				key:    objectKey,
				size:   -1,
			},
		}
	}
	return f, nil
}

type dir struct {
	ctx    context.Context
	key    string
	bucket Bucket
	next   func() (Object, error, bool)
	stop   func()
	last   string
}

func (f *dir) Close() error {
	if f.stop != nil {
		f.stop()
		f.stop = nil
	}
	return nil
}

func (f *dir) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (f *dir) Stat() (fs.FileInfo, error) {
	return dirInfo{dir: f}, nil
}

func (f *dir) ReadDir(n int) ([]fs.DirEntry, error) {
	if f.next == nil {
		f.next, f.stop = iter.Pull2(f.bucket.ListObjects(f.ctx, KeyPrefix(f.key)))
	}
	// Note: this is very inefficient because objects are listed recursively
	// by object stores, unlike directories that only hold the list of entries
	// they contain. We only use this function in testing scenarios so it
	// shouldn't matter in practice.
	var entries []fs.DirEntry
	for n <= 0 || len(entries) < n {
		object, err, ok := f.next()
		if !ok {
			if n <= 0 {
				break
			}
			return entries, io.EOF
		}
		if err != nil {
			return entries, err
		}
		name, sub, _ := strings.Cut(object.Key, "/")
		if name != f.last {
			f.last = name
			mode := fs.FileMode(0)
			if sub != "" {
				mode |= fs.ModeDir
			}
			entries = append(entries, &dirEntry{
				dir:  f,
				key:  object.Key,
				mode: mode,
			})
		}
	}
	return entries, nil
}

type dirInfo struct{ dir *dir }

func (info dirInfo) Name() string       { return path.Base(info.dir.key) }
func (info dirInfo) Size() int64        { return 0 }
func (info dirInfo) Mode() fs.FileMode  { return fileModeRead | fileModeExecute | fs.ModeDir }
func (info dirInfo) ModTime() time.Time { return time.Time{} }
func (info dirInfo) IsDir() bool        { return true }
func (info dirInfo) Sys() any           { return nil }

type dirEntry struct {
	dir  *dir
	key  string
	mode fs.FileMode
}

func (ent *dirEntry) Name() string      { return path.Base(ent.key) }
func (ent *dirEntry) IsDir() bool       { return ent.mode.IsDir() }
func (ent *dirEntry) Type() fs.FileMode { return ent.mode.Type() }
func (ent *dirEntry) Info() (fs.FileInfo, error) {
	if ent.IsDir() {
		return dirInfo{dir: ent.dir}, nil
	}
	object, err := ent.dir.bucket.HeadObject(ent.dir.ctx, ent.key)
	if err != nil {
		return nil, err
	}
	file := &file{
		File: File{
			ctx:    ent.dir.ctx,
			bucket: ent.dir.bucket,
			key:    ent.key,
			size:   object.Size,
		},
		time: object.LastModified,
	}
	return fileInfo{file: file}, nil
}

type file struct {
	File
	time time.Time
	body io.ReadCloser
	seek int64
}

func (f *file) Close() error {
	if f.body == nil {
		return nil
	} else {
		return f.body.Close()
	}
}

func (f *file) Read(b []byte) (int, error) {
	if f.body == nil {
		var opts []GetOption
		if f.seek > 0 {
			if _, err := f.Stat(); err != nil {
				return 0, err
			}
			if f.seek >= f.size {
				return 0, io.EOF
			}
			opts = append(opts, BytesRange(f.seek, f.size-1))
		}
		body, object, err := f.bucket.GetObject(f.ctx, f.key, opts...)
		if err != nil {
			return 0, err
		}
		f.body = body
		f.size = object.Size
		f.time = object.LastModified
	}
	n, err := f.body.Read(b)
	f.seek += int64(n)
	return n, err
}

func (f *file) Stat() (fs.FileInfo, error) {
	if f.size < 0 {
		object, err := f.bucket.HeadObject(f.ctx, f.key)
		if err != nil {
			return nil, err
		}
		f.size = object.Size
		f.time = object.LastModified
	}
	return fileInfo{file: f}, nil
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = f.seek + offset
	case io.SeekEnd:
		if _, err := f.Stat(); err != nil {
			return 0, err
		}
		newOffset = f.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	if newOffset < 0 {
		return 0, fmt.Errorf("negative position: %d", newOffset)
	}

	if newOffset != f.seek && f.body != nil {
		f.body.Close()
		f.body = nil
	}

	f.seek = newOffset
	return f.seek, nil
}

func (f *file) WriteTo(w io.Writer) (int64, error) {
	if f.body == nil {
		var opts []GetOption
		if f.seek > 0 {
			if _, err := f.Stat(); err != nil {
				return 0, err
			}
			if f.seek >= f.size {
				return 0, nil
			}
			opts = append(opts, BytesRange(f.seek, f.size-1))
		}
		body, object, err := f.bucket.GetObject(f.ctx, f.key, opts...)
		if err != nil {
			return 0, err
		}
		f.body = body
		f.size = object.Size
		f.time = object.LastModified
	}
	n, err := io.Copy(w, f.body)
	f.seek += n
	return n, err
}

type fileInfo struct{ file *file }

func (info fileInfo) Name() string       { return info.file.key }
func (info fileInfo) Size() int64        { return info.file.size }
func (info fileInfo) Mode() fs.FileMode  { return fileModeRead }
func (info fileInfo) ModTime() time.Time { return info.file.time }
func (info fileInfo) IsDir() bool        { return false }
func (info fileInfo) Sys() any           { return nil }

type File struct {
	ctx    context.Context
	bucket Bucket
	key    string
	size   int64
}

func loadBucketAndObjectKey(ctx context.Context, store Registry, location string) (Bucket, string, error) {
	bucketType, bucketName, objectKey := uri.Split(location)
	bucket, err := store.LoadBucket(ctx, uri.Join(bucketType, bucketName))
	return bucket, objectKey, err
}

func OpenFile(ctx context.Context, store Registry, location string, size int64) (*File, error) {
	bucket, objectKey, err := loadBucketAndObjectKey(ctx, store, location)
	if err != nil {
		return nil, err
	}
	return NewFile(ctx, bucket, objectKey, size), nil
}

func NewFile(ctx context.Context, bucket Bucket, key string, size int64) *File {
	return &File{
		ctx:    ctx,
		bucket: bucket,
		key:    key,
		size:   size,
	}
}

func (f *File) ReadAt(b []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("offset out of range: %d/%d", off, f.size)
	}
	if len(b) == 0 {
		return 0, nil
	}
	if f.ctx.Err() != nil {
		return 0, context.Cause(f.ctx)
	}
	end := (off + int64(len(b))) - 1
	r, object, err := f.bucket.GetObject(f.ctx, f.key, BytesRange(off, end))
	if err != nil {
		return 0, err
	}
	defer r.Close()
	n, err := io.ReadFull(r, b)
	if off+int64(n) == object.Size {
		err = io.EOF
	}
	return n, err
}

func (f *File) Name() string {
	bucketType, bucketName, objectURI := uri.Split(f.bucket.Location())
	return uri.Join(bucketType, bucketName, objectURI, f.key)
}

func (f *File) Size() int64 {
	return f.size
}

func (f *File) Bucket() Bucket {
	return f.bucket
}

func (f *File) Key() string {
	return f.key
}

func (f *File) Context() context.Context {
	return f.ctx
}

func (f *File) WithContext(ctx context.Context) *File {
	return NewFile(ctx, f.bucket, f.key, f.size)
}

var (
	_ io.ReaderAt = (*File)(nil)
	_ io.ReaderAt = (*file)(nil)
	_ io.Seeker   = (*file)(nil)
	_ io.WriterTo = (*file)(nil)
)
