# CLAUDE.md - Repository Management Guide

This file contains information to help Claude (and other AI assistants) understand and work effectively with this repository.

## Project Overview

**Name**: storage
**Type**: Go library/package
**Purpose**: Unified interface for cloud object storage providers (S3, GCS, file system, memory, HTTP)
**License**: Apache 2.0

## Architecture

### Core Interface

The `Bucket` interface (`storage.go`) defines 14 methods for storage operations:

**Metadata Operations:**
- `Location() string` - Returns the bucket URI
- `Access(ctx) error` - Verifies bucket accessibility
- `Create(ctx) error` - Creates a new bucket

**Object Operations:**
- `HeadObject(ctx, key) (ObjectInfo, error)` - Retrieves metadata without body
- `GetObject(ctx, key, options...) (io.ReadCloser, ObjectInfo, error)` - Retrieves object content
- `PutObject(ctx, key, value, options...) (ObjectInfo, error)` - Stores an object
- `DeleteObject(ctx, key) error` - Removes an object
- `DeleteObjects(ctx, objects) iter.Seq2[string, error]` - Batch delete with streaming
- `CopyObject(ctx, from, to, options...) error` - Copies within bucket
- `ListObjects(ctx, options...) iter.Seq2[Object, error]` - Lists objects
- `WatchObjects(ctx, options...) iter.Seq2[Object, error]` - Watches for changes

**Presigned URL Operations:**
- `PresignGetObject(ctx, key, expiration, options...) (string, error)`
- `PresignPutObject(ctx, key, expiration, options...) (string, error)`
- `PresignHeadObject(ctx, key) (string, error)`
- `PresignDeleteObject(ctx, key) (string, error)`

### Key Data Types

```go
// Object - minimal metadata for listings
type Object struct {
    Key          string
    Size         int64
    LastModified time.Time
}

// ObjectInfo - full metadata
type ObjectInfo struct {
    CacheControl    string
    ContentType     string
    ContentEncoding string
    ETag            string
    Size            int64
    LastModified    time.Time
    Metadata        map[string]string
}
```

### Storage Backends

| Backend | URI Format | Description |
|---------|------------|-------------|
| `s3/` | `s3://bucket-name/path` | Amazon S3 |
| `gs/` | `gs://bucket-name/path` | Google Cloud Storage |
| `file/` | `file:///path` or `/path` | Local file system |
| `memory/` | `:memory:` | In-memory (testing) |
| `http/` | `http://host/path` | HTTP/HTTPS with S3-compatible server |

### Adapters

Adapters wrap buckets to add functionality:

| File | Purpose |
|------|---------|
| `cache.go` | In-memory caching with LRU and TTL |
| `prefix.go` | Mount bucket at a key prefix |
| `readonly.go` | Make bucket read-only |
| `instrument.go` | OpenTelemetry tracing |
| `mount.go` | Mount different buckets at different prefixes |
| `merge.go` | Combine multiple buckets into one |
| `empty.go` | Read-only empty bucket |

### Key Patterns

- **Registry Pattern**: Backends register with `storage.Register(scheme, registry)`
- **Adapter Pattern**: `storage.AdaptBucket(bucket, adapters...)` wraps buckets
- **Options Pattern**: `GetOptions`, `PutOptions`, `ListOptions` for configuration
- **Iterator Pattern**: Uses Go 1.23+ `iter.Seq2[T, error]` for streaming

### Error Codes

```go
var (
    ErrBucketExist         // Bucket already exists
    ErrBucketNotFound      // Bucket doesn't exist
    ErrBucketReadOnly      // Write on read-only bucket
    ErrObjectNotFound      // Object doesn't exist
    ErrObjectNotMatch      // ETag mismatch (conditional write)
    ErrInvalidObjectKey    // Invalid object key
    ErrInvalidObjectTag    // Invalid metadata tag
    ErrInvalidRange        // Invalid byte range
    ErrPresignNotSupported // Backend doesn't support presigning
    ErrPresignRedirect     // Presign requires redirect
    ErrTooManyRequests     // Rate limited
)
```

## Supporting Packages

### cache/
Caching implementations:
- `Cache[K,V]` - Generic cache with singleflight deduplication
- `SeqCache[K,V]` - Iterator-aware caching for `iter.Seq2`
- `LRU[K,V]` - LRU cache with promise-based async loading
- `TTL[K,V]` - LRU with time-to-live expiration

### backoff/
Retry logic:
- `Exponential()` - Exponential backoff strategy (100ms → 200ms → 400ms...)
- `FullJitter(strategy)` - Adds randomization to prevent thundering herd
- `Watch[T](ctx, fn)` - Polls and yields only when values change

### concurrent/
- `WithLimit(ctx, n)` - Sets concurrency limit in context
- `Limit(ctx)` - Retrieves limit (default: 10, max: 1000)

### uri/
- `Split(uri) (scheme, location, path)` - Parses storage URIs
- `Join(scheme, location, path) string` - Constructs URIs
- `Clean(path)` - Normalizes paths

### internal/oteltrace/
OpenTelemetry integration:
- `Start(ctx, name, attrs...)` - Creates spans
- `RecordError(span, err)` - Records errors on spans
- `RecordSeq(seq)` - Wraps iterators with telemetry

### internal/sequtil/
Iterator utilities:
- `Collect(seq)` - Consumes iterator into slice
- `Limit(seq, n)` - Limits to N items
- `Transform(seq, fn)` - Maps values
- `Merge(seqs...)` - Merges multiple sequences

### test/
- `TestStorage(t, loadBucket)` - Runs 30+ test scenarios
- `TestManager(t, loadManager)` - Tests secret managers

## Backend-Specific Notes

### S3 Backend (`s3/`)
- Uses AWS SDK v2
- Supports multipart uploads via SDK manager
- Presigned URLs with lazy client initialization
- Path-style URLs: set `AWS_S3_USE_PATH_STYLE=true`
- Fake S3 client for testing: `s3/fakes3/`

### Google Cloud Storage (`gs/`)
- Dual client architecture:
  - GCS client for reads
  - Custom `gsclient` for streaming uploads
- V4 signing for presigned URLs
- Automatic credential detection

### File System (`file/`)
- Metadata stored in extended attributes:
  - `user.storage.cache-control`
  - `user.storage.content-type`
  - `user.storage.content-encoding`
  - `user.storage.etag`
  - `user.storage.metadata` (JSON)
- Atomic writes via temp files
- File watching via fsnotify
- Platform-specific: Darwin (`file_darwin.go`), Linux (`file_linux.go`)

### Memory Backend (`memory/`)
- Thread-safe with `sync.RWMutex`
- Listener pattern for watch operations
- MD5-based ETag generation
- Presigning returns `ErrPresignNotSupported`

### HTTP Backend (`http/`)
- Full CRUD operations (not read-only)
- S3-compatible server: `BucketHandler`
- Supports ListObjectsV1 (marker) and V2 (continuation-token)
- Batch delete up to 1000 objects
- Optional presigned URL support via `secret.Signer`

## File Structure

```
/
├── storage.go              # Main interface and global functions
├── options.go              # Option types (Get, Put, List)
├── cache.go                # Cache adapter
├── prefix.go               # Prefix adapter
├── readonly.go             # Read-only adapter
├── mount.go                # Mount adapter
├── merge.go                # Merge adapter
├── empty.go                # Empty bucket adapter
├── watch.go                # Generic watch implementation
├── instrument.go           # OpenTelemetry instrumentation
├── log.go                  # Structured logging adapter
├── file.go                 # fs.FS interface for buckets
├── backoff/                # Retry strategies
├── cache/                  # Cache implementations
├── cmd/                    # Command line tools
├── concurrent/             # Concurrency utilities
├── file/                   # File system backend
├── gs/                     # Google Cloud Storage backend
│   └── gsclient/           # Custom streaming upload client
├── http/                   # HTTP backend and S3-compatible server
├── memory/                 # In-memory backend
├── notification/           # Notification system
├── s3/                     # Amazon S3 backend
│   └── fakes3/             # Fake S3 client for testing
├── secret/                 # Secret management (signing, etc.)
├── internal/
│   ├── oteltrace/          # OpenTelemetry utilities
│   └── sequtil/            # Iterator utilities
├── test/                   # Test utilities
└── uri/                    # URI handling
```

## Development Guidelines

### Testing
```bash
# Run all tests
go test ./...

# Run tests with race detection
go test -race ./...

# Run specific package tests
go test ./s3
```

### Dependencies
- **AWS SDK v2**: S3 integration
- **Google Cloud Storage**: GCS integration
- **fsnotify**: File system watching
- **OpenTelemetry**: Tracing
- **kway-go**: K-way merge for sorted lists

### Adding a New Backend

1. Create package directory (e.g., `azure/`)
2. Implement `storage.Bucket` interface (all 14 methods)
3. Create `NewRegistry() storage.Registry`
4. Register in `init()`:
   ```go
   func init() {
       storage.Register("azure", NewRegistry())
   }
   ```
5. Add tests using `test.TestStorage(t, loadBucket)`

### Common Issues

1. **Import for side effects**: `import _ "github.com/firetiger-oss/storage/s3"`
2. **URI trailing slashes**: Automatically handled by registry
3. **Context cancellation**: Always respect `ctx.Done()`

## Compatibility

- **Go 1.24.4+** (uses Go 1.23+ iterator patterns `iter.Seq2`)
- Backward compatible API design
- Semantic versioning

## Security

- Never commit credentials
- Use IAM roles for cloud access
- Validate object keys to prevent path traversal
- Consider encryption at rest and in transit
