package storage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/firetiger-oss/tigerblock/internal/sequtil"
)

// NewBucketFromFS returns a read-only Bucket backed by the given fs.FS.
//
// Object keys map directly onto fs paths (no leading slash, "/" separated,
// the same shape accepted by fs.ValidPath). Because fs.FS is a read-only
// abstraction, every write operation (Create, PutObject, DeleteObject,
// DeleteObjects, CopyObject) returns ErrBucketReadOnly, and presigning is not
// supported.
func NewBucketFromFS(fsys fs.FS) Bucket {
	return NewBucketFromNewFS(func(context.Context) fs.FS { return fsys })
}

// NewBucketFromNewFS is like NewBucketFromFS but obtains the underlying fs.FS
// from newFS on each operation, passing the context that was given to the
// Bucket method. This lets the fs.FS be derived from the caller's context
// (for example to scope it to a request, a transaction, or a tenant) instead
// of being fixed when the bucket is constructed.
//
// newFS is called once per Bucket operation and must not return nil.
func NewBucketFromNewFS(newFS func(context.Context) fs.FS) Bucket {
	return fsBucket{newFS: newFS}
}

type fsBucket struct {
	newFS func(context.Context) fs.FS
}

func (b fsBucket) Location() string { return ":fs:" }

func (b fsBucket) Access(ctx context.Context) error {
	return context.Cause(ctx)
}

func (b fsBucket) Create(ctx context.Context) error {
	return cmp.Or(context.Cause(ctx), ErrBucketReadOnly)
}

func (b fsBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return ObjectInfo{}, err
	}
	info, err := b.statObject(ctx, key)
	if err != nil {
		return ObjectInfo{}, err
	}
	return objectInfo(info), nil
}

func (b fsBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, ObjectInfo{}, err
	}
	if err := validFSObjectKey(key); err != nil {
		return nil, ObjectInfo{}, err
	}

	f, err := b.newFS(ctx).Open(key)
	if err != nil {
		return nil, ObjectInfo{}, fsError(key, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, ObjectInfo{}, fsError(key, err)
	}
	if info.IsDir() {
		f.Close()
		return nil, ObjectInfo{}, fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}

	size := info.Size()
	getOptions := NewGetOptions(options...)
	start, end, ok := getOptions.BytesRange()
	if !ok {
		return f, objectInfo(info), nil
	}

	if err := ValidObjectRange(key, start, end); err != nil {
		f.Close()
		return nil, ObjectInfo{}, err
	}
	if end < 0 {
		end = size - 1
	}

	reader, err := seekTo(f, min(start, size))
	if err != nil {
		f.Close()
		return nil, ObjectInfo{}, fsError(key, err)
	}

	length := max(min(end+1, size)-start, 0)
	return &struct {
		io.Reader
		io.Closer
	}{
		Reader: io.LimitReader(reader, length),
		Closer: f,
	}, objectInfo(info), nil
}

func (b fsBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	return ObjectInfo{}, cmp.Or(context.Cause(ctx), ValidObjectKey(key), ErrBucketReadOnly)
}

func (b fsBucket) DeleteObject(ctx context.Context, key string) error {
	return cmp.Or(context.Cause(ctx), ValidObjectKey(key), ErrBucketReadOnly)
}

func (b fsBucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for key, err := range objects {
			if !yield(key, cmp.Or(err, context.Cause(ctx), ErrBucketReadOnly)) {
				return
			}
		}
	}
}

func (b fsBucket) CopyObject(ctx context.Context, from, to string, options ...PutOption) error {
	return cmp.Or(context.Cause(ctx), ValidObjectKey(from), ValidObjectKey(to), ErrBucketReadOnly)
}

func (b fsBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	listOptions := NewListOptions(options...)
	return sequtil.Limit(b.listObjects(ctx, listOptions), listOptions.MaxKeys())
}

// listObjects walks the underlying fs.FS lazily, one directory at a time, and
// yields objects in lexical key order — the same ordering the other backends
// (and S3) guarantee. Only the entries of the directories on the current path
// are held in memory; the full listing is never materialized.
//
// Note that fs.WalkDir cannot be used directly: it visits directory contents
// before lexically-smaller sibling files (e.g. it yields "data/x" before
// "data.json", because '/' > '.'), which does not match key ordering. The
// custom walk below sorts each directory's entries as if directory names had a
// trailing "/", which makes the traversal order equal to sorting the full keys.
func (b fsBucket) listObjects(ctx context.Context, listOptions *ListOptions) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		if err := context.Cause(ctx); err != nil {
			yield(Object{}, err)
			return
		}
		l := &fsLister{
			ctx:        ctx,
			fsys:       b.newFS(ctx),
			prefix:     listOptions.KeyPrefix(),
			delimiter:  listOptions.KeyDelimiter(),
			startAfter: listOptions.StartAfter(),
			yield:      yield,
		}
		// walk returns false on either consumer stop or error; only an error
		// still needs to be reported to the consumer.
		if !l.walk(".", "") && l.err != nil {
			yield(Object{}, l.err)
		}
	}
}

type fsLister struct {
	ctx        context.Context
	fsys       fs.FS
	prefix     string
	delimiter  string
	startAfter string
	yield      func(Object, error) bool

	err            error
	lastPrefix     string // last common prefix emitted, for adjacent dedup
	lastPrefixSeen bool
}

// walk recurses into dir, whose object keys are rooted at keyPrefix ("" for the
// FS root, otherwise the directory path followed by "/"). It returns false to
// unwind the whole walk, either because the consumer stopped or because err was
// set.
func (l *fsLister) walk(dir, keyPrefix string) bool {
	if err := context.Cause(l.ctx); err != nil {
		l.err = err
		return false
	}
	entries, err := fs.ReadDir(l.fsys, dir)
	if err != nil {
		l.err = err
		return false
	}
	// Order entries so that the resulting full keys come out byte-sorted:
	// a directory contributes keys under name+"/", so it must sort as if its
	// name had a trailing "/".
	slices.SortFunc(entries, func(a, b fs.DirEntry) int {
		return strings.Compare(sortName(a), sortName(b))
	})

	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() {
			childKeyPrefix := keyPrefix + name + "/"
			// Prune subtrees that cannot contain a key matching the prefix.
			if !strings.HasPrefix(childKeyPrefix, l.prefix) && !strings.HasPrefix(l.prefix, childKeyPrefix) {
				continue
			}
			// If a delimiter already falls within this directory's key prefix,
			// every key beneath it collapses to a single common prefix, so we
			// emit it once and skip descending entirely.
			if l.delimiter != "" && strings.HasPrefix(childKeyPrefix, l.prefix) {
				if i := strings.Index(childKeyPrefix[len(l.prefix):], l.delimiter); i >= 0 {
					if !l.emitCommonPrefix(childKeyPrefix[:len(l.prefix)+i+len(l.delimiter)]) {
						return false
					}
					continue
				}
			}
			if !l.walk(path.Join(dir, name), childKeyPrefix) {
				return false
			}
			continue
		}

		key := keyPrefix + name
		if !strings.HasPrefix(key, l.prefix) {
			continue
		}
		if l.delimiter != "" {
			if i := strings.Index(key[len(l.prefix):], l.delimiter); i >= 0 {
				if !l.emitCommonPrefix(key[:len(l.prefix)+i+len(l.delimiter)]) {
					return false
				}
				continue
			}
		}
		info, err := entry.Info()
		if err != nil {
			l.err = err
			return false
		}
		if key > l.startAfter && !l.yield(Object{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		}, nil) {
			return false
		}
	}
	return true
}

// emitCommonPrefix yields a delimiter-collapsed common prefix, deduplicating
// against the previous one. Because the walk is globally sorted, all keys
// sharing a common prefix are contiguous, so tracking just the last one
// suffices. It returns false only when the consumer stops.
func (l *fsLister) emitCommonPrefix(prefix string) bool {
	if l.lastPrefixSeen && l.lastPrefix == prefix {
		return true
	}
	l.lastPrefix = prefix
	l.lastPrefixSeen = true
	if prefix <= l.startAfter {
		return true
	}
	return l.yield(Object{Key: prefix}, nil)
}

// sortName returns the name used to order a directory entry so that the
// resulting full keys are byte-sorted (see walk).
func sortName(entry fs.DirEntry) string {
	if entry.IsDir() {
		return entry.Name() + "/"
	}
	return entry.Name()
}

func (b fsBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		// An fs.FS has no change-notification mechanism, so we emit the
		// current listing once and then block until the context is canceled.
		for object, err := range b.ListObjects(ctx, options...) {
			if !yield(object, err) {
				return
			}
		}
		<-ctx.Done()
		yield(Object{}, context.Cause(ctx))
	}
}

func (b fsBucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...GetOption) (string, error) {
	return "", cmp.Or(context.Cause(ctx), ValidObjectKey(key), ErrPresignNotSupported)
}

func (b fsBucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...PutOption) (string, error) {
	return "", cmp.Or(context.Cause(ctx), ValidObjectKey(key), ErrBucketReadOnly)
}

func (b fsBucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return "", cmp.Or(context.Cause(ctx), ValidObjectKey(key), ErrPresignNotSupported)
}

func (b fsBucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	return "", cmp.Or(context.Cause(ctx), ValidObjectKey(key), ErrBucketReadOnly)
}

func (b fsBucket) statObject(ctx context.Context, key string) (fs.FileInfo, error) {
	if err := validFSObjectKey(key); err != nil {
		return nil, err
	}
	info, err := fs.Stat(b.newFS(ctx), key)
	if err != nil {
		return nil, fsError(key, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	return info, nil
}

// validFSObjectKey applies the regular object-key validation and then the
// stricter fs.ValidPath check, since fs paths reject the trailing-slash
// directory markers that ValidObjectKey tolerates.
func validFSObjectKey(key string) error {
	if err := ValidObjectKey(key); err != nil {
		return err
	}
	if !fs.ValidPath(key) || key == "." {
		return fmt.Errorf("%w (%s)", ErrInvalidObjectKey, key)
	}
	return nil
}

func objectInfo(info fs.FileInfo) ObjectInfo {
	return ObjectInfo{
		Size:         info.Size(),
		LastModified: info.ModTime(),
	}
}

// fsError maps fs errors onto the storage error vocabulary.
func fsError(key string, err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("%s: %w", key, ErrObjectNotFound)
	}
	return err
}

// seekTo positions r at offset, using io.Seeker when available and otherwise
// discarding the leading bytes.
func seekTo(r io.Reader, offset int64) (io.Reader, error) {
	if offset == 0 {
		return r, nil
	}
	if seeker, ok := r.(io.Seeker); ok {
		if _, err := seeker.Seek(offset, io.SeekStart); err != nil {
			return nil, err
		}
		return r, nil
	}
	if _, err := io.CopyN(io.Discard, r, offset); err != nil && err != io.EOF {
		return nil, err
	}
	return r, nil
}

var _ Bucket = fsBucket{}
