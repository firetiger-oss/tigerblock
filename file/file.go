package file

import (
	"cmp"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/backoff"
	"github.com/firetiger-oss/storage/internal/sequtil"
	"github.com/fsnotify/fsnotify"
	"golang.org/x/sys/unix"
)

const (
	xattrCacheControl    = "user.storage.cache-control"
	xattrContentType     = "user.storage.content-type"
	xattrContentEncoding = "user.storage.content-encoding"
	xattrETag            = "user.storage.etag"
	xattrMetadata        = "user.storage.metadata"
	tempFileSuffix       = ".C848CADBC89F4F129E6249F61F11C78B.tmp"
	tempFilePattern      = ".*" + tempFileSuffix
)

func init() {
	storage.Register("file", NewRegistry("/"))
}

func NewRegistry(workingDirectory string) storage.Registry {
	return storage.RegistryFunc(func(ctx context.Context, bucket string) (storage.Bucket, error) {
		root := workingDirectory
		if bucket != "" {
			root = filepath.Join(workingDirectory, bucket)
		}
		return NewBucket(root), nil
	})
}

func NewBucket(workingDirectory string) *Bucket {
	return &Bucket{root: workingDirectory}
}

func bytesRangeReadCloser(f *os.File, start, end int64) io.ReadCloser {
	return struct {
		io.Reader
		io.Closer
	}{
		Reader: io.NewSectionReader(f, start, (end+1)-start),
		Closer: f,
	}
}

type Bucket struct {
	root string
}

func (b *Bucket) Location() string {
	return "file://" + strings.TrimSuffix(b.root, "/") + "/"
}

func (b *Bucket) Access(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	return b.stat()
}

func (b *Bucket) Create(ctx context.Context) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	_, err := os.Stat(b.root)
	if err == nil {
		return storage.ErrBucketExist
	}
	return os.MkdirAll(b.root, 0777)
}

func (b *Bucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := b.stat(); err != nil {
		return storage.ObjectInfo{}, err
	}
	f, err := os.Open(b.path(key))
	if err != nil {
		if isErrNotExist(err) {
			err.(*os.PathError).Err = storage.ErrObjectNotFound
		}
		return storage.ObjectInfo{}, err
	}
	defer f.Close()
	return readObjectInfo(f)
}

func (b *Bucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return nil, storage.ObjectInfo{}, err
	}
	if err := b.stat(); err != nil {
		return nil, storage.ObjectInfo{}, err
	}

	f, err := os.Open(b.path(key))
	if err != nil {
		if isErrNotExist(err) {
			err.(*os.PathError).Err = storage.ErrObjectNotFound
		}
		return nil, storage.ObjectInfo{}, err
	}
	closeFile := true
	defer func() {
		if closeFile {
			f.Close()
		}
	}()

	object, err := readObjectInfo(f)
	if err != nil {
		if errors.Is(err, syscall.EISDIR) {
			err = storage.ErrObjectNotFound
		}
		return nil, storage.ObjectInfo{}, err
	}

	getOptions := storage.NewGetOptions(options...)
	body := io.ReadCloser(f)
	if start, end, ok := getOptions.BytesRange(); ok {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			return nil, storage.ObjectInfo{}, err
		}
		if _, err := f.Seek(start, io.SeekCurrent); err != nil {
			return nil, storage.ObjectInfo{}, err
		}
		body = bytesRangeReadCloser(f, start, end)
	}

	closeFile = false
	return body, object, nil
}

func (b *Bucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	if err := context.Cause(ctx); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := b.stat(); err != nil {
		return storage.ObjectInfo{}, err
	}

	path := b.path(key)
	dir, base := filepath.Split(path)

	if err := os.MkdirAll(dir, 0777); err != nil {
		return storage.ObjectInfo{}, err
	}

	putOptions := storage.NewPutOptions(options...)
	ifMatch := putOptions.IfMatch()
	ifNoneMatch := putOptions.IfNoneMatch()
	switch ifNoneMatch {
	case "", "*":
	default:
		return storage.ObjectInfo{}, storage.ErrInvalidObjectTag
	}

	temp, err := os.CreateTemp(dir, base+tempFilePattern)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	removeTemp := true
	defer func() {
		if removeTemp {
			os.Remove(temp.Name())
		}
		temp.Close()
	}()

	etag := md5.New()
	if _, err := io.Copy(io.MultiWriter(temp, etag), value); err != nil {
		return storage.ObjectInfo{}, err
	}
	if _, err := temp.Seek(0, io.SeekStart); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := temp.Chmod(0644); err != nil {
		return storage.ObjectInfo{}, err
	}
	s, err := temp.Stat()
	if err != nil {
		return storage.ObjectInfo{}, err
	}

	object := storage.ObjectInfo{
		CacheControl:    putOptions.CacheControl(),
		ContentType:     putOptions.ContentType(),
		ContentEncoding: putOptions.ContentEncoding(),
		Metadata:        putOptions.Metadata(),
		ETag:            hex.EncodeToString(etag.Sum(nil)),
	}

	if err := writeObjectInfo(temp, object); err != nil {
		return storage.ObjectInfo{}, err
	}

	switch {
	case ifNoneMatch != "":
		if err := renameIfNotExist(temp.Name(), path); err != nil {
			return storage.ObjectInfo{}, storage.ErrObjectNotMatch
		}

	case ifMatch != "":
		currentObjectFile, err := os.Open(path)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				err = storage.ErrObjectNotMatch
			}
			return storage.ObjectInfo{}, err
		}
		defer currentObjectFile.Close()

		currentFd := int(currentObjectFile.Fd())
		if err := unix.Flock(currentFd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return storage.ObjectInfo{}, storage.ErrObjectNotMatch
		}
		defer unix.Flock(currentFd, unix.LOCK_UN)

		currentObjectInfo, err := readObjectInfo(currentObjectFile)
		if err != nil {
			return storage.ObjectInfo{}, err
		}
		if currentObjectInfo.ETag != ifMatch {
			return storage.ObjectInfo{}, storage.ErrObjectNotMatch
		}

		fallthrough
	default:
		if err := os.Rename(temp.Name(), path); err != nil {
			return storage.ObjectInfo{}, err
		}
	}

	removeTemp = false
	object.Size = s.Size()
	object.LastModified = s.ModTime()
	return object, nil
}

func (b *Bucket) DeleteObject(ctx context.Context, key string) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(key); err != nil {
		return err
	}
	if err := b.stat(); err != nil {
		return err
	}
	filePath := b.path(key)
	if err := os.Remove(filePath); err != nil {
		if !isErrNotExist(err) {
			return err
		}
	}
	b.removeEmptyDirectories(filepath.Dir(filePath))
	return nil
}

func (b *Bucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		dirs := make(map[string]struct{})
		defer func() {
			for dir := range dirs {
				b.removeEmptyDirectories(dir)
			}
		}()

		for key, err := range objects {
			err = cmp.Or(err, context.Cause(ctx), b.stat(), storage.ValidObjectKey(key))

			if err == nil {
				filePath := b.path(key)
				if err = os.Remove(filePath); err != nil && isErrNotExist(err) {
					err = nil
				}
				if err == nil {
					dirs[filepath.Dir(filePath)] = struct{}{}
				}
			}

			if !yield(key, err) {
				return
			}
		}
	}
}

func (b *Bucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(from); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(to); err != nil {
		return err
	}
	if err := b.stat(); err != nil {
		return err
	}

	// Open source file
	srcPath := b.path(from)
	srcFile, err := os.Open(srcPath)
	if err != nil {
		if isErrNotExist(err) {
			err.(*os.PathError).Err = storage.ErrObjectNotFound
		}
		return err
	}
	defer srcFile.Close()

	// Read source metadata
	srcInfo, err := readObjectInfo(srcFile)
	if err != nil {
		if errors.Is(err, syscall.EISDIR) {
			err = storage.ErrObjectNotFound
		}
		return err
	}

	// Build merged options (source metadata with overrides)
	putOptions := storage.NewPutOptions(options...)
	var mergedOpts []storage.PutOption

	if cc := putOptions.CacheControl(); cc != "" {
		mergedOpts = append(mergedOpts, storage.CacheControl(cc))
	} else if srcInfo.CacheControl != "" {
		mergedOpts = append(mergedOpts, storage.CacheControl(srcInfo.CacheControl))
	}

	if ct := putOptions.ContentType(); ct != "application/octet-stream" {
		mergedOpts = append(mergedOpts, storage.ContentType(ct))
	} else if srcInfo.ContentType != "" {
		mergedOpts = append(mergedOpts, storage.ContentType(srcInfo.ContentType))
	}

	if ce := putOptions.ContentEncoding(); ce != "" {
		mergedOpts = append(mergedOpts, storage.ContentEncoding(ce))
	} else if srcInfo.ContentEncoding != "" {
		mergedOpts = append(mergedOpts, storage.ContentEncoding(srcInfo.ContentEncoding))
	}

	// Merge metadata maps (overrides win)
	for k, v := range srcInfo.Metadata {
		mergedOpts = append(mergedOpts, storage.Metadata(k, v))
	}
	for k, v := range putOptions.Metadata() {
		mergedOpts = append(mergedOpts, storage.Metadata(k, v))
	}

	// Reset file position for reading content
	if _, err := srcFile.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Use PutObject for the copy (handles temp file, directories, etc.)
	_, err = b.PutObject(ctx, to, srcFile, mergedOpts...)
	return err
}

func (b *Bucket) ListObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	listOptions := storage.NewListOptions(options...)

	seq := func(yield func(storage.Object, error) bool) {
		if err := context.Cause(ctx); err != nil {
			yield(storage.Object{}, err)
			return
		}
		if err := b.stat(); err != nil {
			yield(storage.Object{}, err)
			return
		}

		delimiter := listOptions.KeyDelimiter()
		prefix := listOptions.KeyPrefix()
		dirPath := "."
		if i := strings.LastIndexByte(prefix, '/'); i >= 0 {
			dirPath = prefix[:i]
		}

		switch delimiter {
		case "", "/":
		default:
			yield(storage.Object{}, fmt.Errorf("unsupported delimiter for file storage: %q (must be '/')", delimiter))
			return
		}

		if delimiter != "" {
			b.listObjectsShallow(ctx, dirPath, yield, listOptions)
		} else {
			b.listObjectsRecursive(ctx, dirPath, yield, listOptions)
		}
	}

	return sequtil.Limit(seq, listOptions.MaxKeys())
}

func (b *Bucket) WatchObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		if err := context.Cause(ctx); err != nil {
			yield(storage.Object{}, err)
			return
		}

		if err := b.stat(); err != nil {
			yield(storage.Object{}, err)
			return
		}

		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			yield(storage.Object{}, err)
			return
		}
		defer watcher.Close()

		listOptions := storage.NewListOptions(options...)
		prefix := listOptions.KeyPrefix()
		delimiter := listOptions.KeyDelimiter()

		baseDirPath := "."
		if i := strings.LastIndexByte(prefix, '/'); i >= 0 {
			baseDirPath = prefix[:i]
		}

		dirPath := filepath.Join(b.root, baseDirPath)

		for _, err := range backoff.Seq(ctx) {
			if err != nil {
				yield(storage.Object{}, err)
				return
			}
			_, err := os.Stat(dirPath)
			if err == nil {
				break
			}
			if !errors.Is(err, fs.ErrNotExist) {
				yield(storage.Object{}, err)
				return
			}
		}

		if delimiter == "/" {
			err = watcher.Add(dirPath)
		} else {
			err = filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if d.IsDir() {
					if err := watcher.Add(path); err != nil && !isErrNotExist(err) {
						return err
					}
				}
				return nil
			})
		}

		if err != nil && !isErrNotExist(err) {
			yield(storage.Object{}, err)
			return
		}

		for object, err := range b.ListObjects(ctx, options...) {
			if !yield(object, err) {
				return
			}
		}

		for {
			select {
			case <-ctx.Done():
				return

			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if strings.HasSuffix(event.Name, tempFileSuffix) {
					continue
				}
				relativePath, _ := filepath.Rel(b.root, event.Name)
				objectKey := filepath.ToSlash(relativePath)

				if event.Has(fsnotify.Create | fsnotify.Write) {
					s, err := os.Stat(event.Name)
					if err != nil {
						if !isErrNotExist(err) && !yield(storage.Object{}, err) {
							return
						}
					} else if s.IsDir() {
						if delimiter == "" {
							if err := watcher.Add(event.Name); err != nil {
								yield(storage.Object{}, err)
								return
							}
						} else {
							if !yield(storage.Object{Key: objectKey + "/"}, nil) {
								return
							}
						}
					} else {
						if !yield(storage.Object{
							Key:          objectKey,
							Size:         s.Size(),
							LastModified: s.ModTime(),
						}, nil) {
							return
						}
					}
				}

				if event.Has(fsnotify.Remove) {
					if !yield(storage.Object{
						Key:          objectKey,
						Size:         -1, // deletion marker
						LastModified: time.Now(),
					}, nil) {
						return
					}
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				if !yield(storage.Object{}, err) {
					return
				}
			}
		}
	}
}

func (b *Bucket) listObjectsShallow(ctx context.Context, dirPath string, yield func(storage.Object, error) bool, listOptions *storage.ListOptions) {
	prefix := listOptions.KeyPrefix()
	startAfter := listOptions.StartAfter()

	entries, err := fs.ReadDir(os.DirFS(b.root), dirPath)
	if err != nil {
		if !isErrNotExist(err) {
			yield(storage.Object{}, err)
		}
		return
	}

	for _, entry := range entries {
		fileName := entry.Name()

		if strings.HasSuffix(fileName, tempFileSuffix) {
			continue
		}

		key := path.Join(dirPath, fileName)
		if !strings.HasPrefix(key, prefix) || key <= startAfter {
			continue
		}

		if entry.IsDir() {
			if !yield(storage.Object{Key: key + "/"}, nil) {
				return
			}
			continue
		}

		info, err := entry.Info()
		if err != nil {
			if !isErrNotExist(err) {
				yield(storage.Object{}, err)
				return
			}
			continue
		}

		object := storage.Object{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		}

		if !yield(object, nil) {
			return
		}
	}
}

func (b *Bucket) listObjectsRecursive(ctx context.Context, dirPath string, yield func(storage.Object, error) bool, listOptions *storage.ListOptions) {
	prefix := listOptions.KeyPrefix()
	startAfter := listOptions.StartAfter()

	err := fs.WalkDir(os.DirFS(b.root), dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, tempFileSuffix) {
			return nil
		}
		if !strings.HasPrefix(path, prefix) || path <= startAfter {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			if !isErrNotExist(err) {
				return err
			}
			return nil
		}

		object := storage.Object{
			Key:          path,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		}

		if !yield(object, nil) {
			return fs.SkipAll
		}
		return nil
	})

	if err != nil && !isErrNotExist(err) {
		yield(storage.Object{}, err)
		return
	}
}

func (b *Bucket) path(key string) string {
	return filepath.Join(b.root, filepath.FromSlash(key))
}

func (b *Bucket) stat() error {
	s, err := os.Stat(b.root)
	if err != nil {
		return err
	}
	if !s.IsDir() {
		return fmt.Errorf("file bucket location is not a directory: %s", b.root)
	}
	return nil
}

func isErrNotExist(err error) bool {
	return errors.Is(err, fs.ErrNotExist)
}

// removeEmptyDirectories recursively removes empty directories up to the bucket root
func (b *Bucket) removeEmptyDirectories(dir string) {
	// Don't remove the bucket root itself
	if dir == b.root || dir == filepath.Dir(b.root) || dir == "." || dir == "/" {
		return
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}

	if len(entries) == 0 {
		if err := os.Remove(dir); err == nil {
			b.removeEmptyDirectories(filepath.Dir(dir))
		}
	}
}

func readObjectInfo(f *os.File) (storage.ObjectInfo, error) {
	s, err := f.Stat()
	if err != nil {
		return storage.ObjectInfo{}, err
	}

	object := storage.ObjectInfo{
		Size:         s.Size(),
		LastModified: s.ModTime(),
	}

	fd := int(f.Fd())
	fb := make([]byte, 256)
	for attr, value := range map[string]*string{
		xattrCacheControl:    &object.CacheControl,
		xattrContentType:     &object.ContentType,
		xattrContentEncoding: &object.ContentEncoding,
		xattrETag:            &object.ETag,
	} {
		size, err := unix.Fgetxattr(fd, attr, fb)
		if err == nil && size > 0 {
			*value = string(fb[:size])
		} else if err != nil && !isErrAttrNotExist(err) {
			return storage.ObjectInfo{}, fmt.Errorf("%s: reading %s: %w", f.Name(), attr, err)
		}
	}

	if size, err := unix.Fgetxattr(fd, xattrMetadata, nil); err == nil && size > 0 {
		data := make([]byte, size)
		n, err := unix.Fgetxattr(fd, xattrMetadata, data)
		if err != nil {
			return storage.ObjectInfo{}, fmt.Errorf("%s: reading metadata xattr: %w", f.Name(), err)
		}
		if err := json.Unmarshal(data[:n], &object.Metadata); err != nil {
			return storage.ObjectInfo{}, fmt.Errorf("%s: parsing metadata: %w", f.Name(), err)
		}
	} else if err != nil && !isErrAttrNotExist(err) {
		return storage.ObjectInfo{}, fmt.Errorf("%s: checking metadata xattr size: %w", f.Name(), err)
	}

	return object, nil
}

func writeObjectInfo(f *os.File, object storage.ObjectInfo) error {
	fd := int(f.Fd())
	for attr, value := range map[string]string{
		xattrCacheControl:    object.CacheControl,
		xattrContentType:     object.ContentType,
		xattrContentEncoding: object.ContentEncoding,
		xattrETag:            object.ETag,
	} {
		if value != "" {
			if err := unix.Fsetxattr(fd, attr, []byte(value), 0); err != nil {
				return fmt.Errorf("setting xattr %s: %w", attr, err)
			}
		}
	}

	if object.Metadata != nil {
		metadata, err := json.Marshal(object.Metadata)
		if err != nil {
			return fmt.Errorf("marshaling metadata: %w", err)
		}
		if err := unix.Fsetxattr(fd, xattrMetadata, metadata, 0); err != nil {
			return fmt.Errorf("setting metadata xattr: %w", err)
		}
	}

	return nil
}

func (b *Bucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	// For file:// storage, "presigning" just returns the absolute file path.
	// There's no authentication to embed, so we return a file:// URL directly.
	return "file://" + b.path(key), nil
}

func (b *Bucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...storage.PutOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "file://" + b.path(key), nil
}

func (b *Bucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "file://" + b.path(key), nil
}

func (b *Bucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "file://" + b.path(key), nil
}
