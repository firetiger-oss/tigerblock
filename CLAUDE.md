# CLAUDE.md - Repository Management Guide

This file contains information to help Claude (and other AI assistants) understand and work effectively with this repository.

## Project Overview

**Name**: tigerblock
**Module**: `github.com/firetiger-oss/tigerblock`
**Type**: Go library/toolkit
**Purpose**: Batteries-included toolkit for building applications on top of object storage in Go (storage abstraction, secrets, notifications, OCI/oras integration, CLI)
**License**: Apache 2.0

## Architecture

### Storage Package (`storage/`)

The `storage` package (`github.com/firetiger-oss/tigerblock/storage`) provides a unified interface for cloud object storage providers. The `Bucket` interface (`storage/storage.go`) defines 14 methods for storage operations:

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
| `storage/s3/` | `s3://bucket-name/path` | Amazon S3 |
| `storage/r2/` | `r2://bucket-name/path` | Cloudflare R2 |
| `storage/gs/` | `gs://bucket-name/path` | Google Cloud Storage |
| `storage/file/` | `file:///path` or `/path` | Local file system |
| `storage/memory/` | `:memory:` | In-memory (testing) |
| `storage/http/` | `http://host/path` | HTTP/HTTPS with S3-compatible server |

### Adapters

Adapters wrap buckets to add functionality (all under `storage/`):

| File | Purpose |
|------|---------|
| `storage/cache.go` | In-memory caching with LRU and TTL |
| `storage/prefix.go` | Mount bucket at a key prefix |
| `storage/readonly.go` | Make bucket read-only |
| `storage/instrument.go` | OpenTelemetry tracing |
| `storage/mount.go` | Mount different buckets at different prefixes |
| `storage/merge.go` | Combine multiple buckets into one |
| `storage/empty.go` | Read-only empty bucket |
| `storage/overlay.go` | Layered bucket overlay |

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

### storage/cache/
Caching implementations:
- `Cache[K,V]` - Generic cache with singleflight deduplication
- `SeqCache[K,V]` - Iterator-aware caching for `iter.Seq2`
- `LRU[K,V]` - LRU cache with promise-based async loading
- `TTL[K,V]` - LRU with time-to-live expiration

### storage/backoff/
Retry logic:
- `Exponential()` - Exponential backoff strategy (100ms → 200ms → 400ms...)
- `FullJitter(strategy)` - Adds randomization to prevent thundering herd
- `Watch[T](ctx, fn)` - Polls and yields only when values change

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

### secret/
Pluggable secret manager with backends for AWS Secrets Manager, GCP Secret Manager, environment variables, files, and bucket-backed stores. Includes signing helpers under `secret/authn/` (basic, bearer, sigv4).

### notification/
Bucket change notification handling with backends for AWS EventBridge, GCP Pub/Sub, and Cloudflare Queues.

### oras/
OCI/ORAS integration: read and write OCI artifacts on top of any `storage.Bucket`.

### cmd/tigerblock/
CLI tool (`tigerblock`) with subcommands `cp`, `ls`, `rm`, `serve`, `stat`.

## Backend-Specific Notes

### S3 Backend (`storage/s3/`)
- Uses AWS SDK v2
- Supports multipart uploads via SDK manager
- Presigned URLs with lazy client initialization
- Path-style URLs: set `AWS_S3_USE_PATH_STYLE=true`
- Fake S3 client for testing: `storage/s3/fakes3/`

### Google Cloud Storage (`storage/gs/`)
- Dual client architecture:
  - GCS client for reads
  - Custom `gsclient` (`storage/gs/gsclient/`) for streaming uploads
- V4 signing for presigned URLs
- Automatic credential detection

### File System (`storage/file/`)
- Metadata stored in extended attributes:
  - `user.storage.cache-control`
  - `user.storage.content-type`
  - `user.storage.content-encoding`
  - `user.storage.etag`
  - `user.storage.metadata` (JSON)
- Atomic writes via temp files
- File watching via fsnotify
- Platform-specific: Darwin (`file_darwin.go`), Linux (`file_linux.go`)

### Memory Backend (`storage/memory/`)
- Thread-safe with `sync.RWMutex`
- Listener pattern for watch operations
- MD5-based ETag generation
- Presigning returns `ErrPresignNotSupported`

### HTTP Backend (`storage/http/`)
- Full CRUD operations (not read-only)
- S3-compatible server: `BucketHandler`
- Supports ListObjectsV1 (marker) and V2 (continuation-token)
- Batch delete up to 1000 objects
- Optional presigned URL support via `secret.Signer`

## File Structure

```
/
├── storage/                # Storage abstraction (package storage)
│   ├── storage.go          # Main interface and global functions
│   ├── options.go          # Option types (Get, Put, List)
│   ├── cache.go            # Cache adapter
│   ├── prefix.go           # Prefix adapter
│   ├── readonly.go         # Read-only adapter
│   ├── mount.go            # Mount adapter
│   ├── merge.go            # Merge adapter
│   ├── empty.go            # Empty bucket adapter
│   ├── overlay.go          # Overlay adapter
│   ├── watch.go            # Generic watch implementation
│   ├── instrument.go       # OpenTelemetry instrumentation
│   ├── log.go              # Structured logging adapter
│   ├── file.go             # fs.FS interface for buckets
│   ├── backoff/            # Retry strategies
│   ├── cache/              # Cache implementations
│   ├── file/               # File system backend
│   ├── fuse/               # FUSE mount backend
│   ├── gs/                 # Google Cloud Storage backend
│   │   └── gsclient/       # Custom streaming upload client
│   ├── http/               # HTTP backend and S3-compatible server
│   ├── memory/             # In-memory backend
│   ├── r2/                 # Cloudflare R2 backend
│   └── s3/                 # Amazon S3 backend
│       └── fakes3/         # Fake S3 client for testing
├── cmd/
│   └── tigerblock/         # CLI tool
├── notification/           # Bucket change notifications
│   ├── aws/                # AWS EventBridge
│   ├── cloudflare/         # Cloudflare Queues
│   ├── gcp/                # GCP Pub/Sub
│   └── serve/              # HTTP delivery
├── oras/                   # OCI/ORAS integration
├── secret/                 # Secret management
│   ├── authn/              # Authentication signers (basic, bearer, sigv4)
│   ├── aws/                # AWS Secrets Manager
│   ├── bucket/             # Bucket-backed secret store
│   ├── env/                # Environment variable secrets
│   ├── file/               # File-backed secrets
│   ├── gcp/                # GCP Secret Manager
│   ├── gs/                 # GCS-backed secrets
│   ├── http/               # HTTP-backed secrets
│   └── s3/                 # S3-backed secrets
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
go test ./storage/s3
```

### Dependencies
- **AWS SDK v2**: S3 integration
- **Google Cloud Storage**: GCS integration
- **github.com/firetiger-oss/concurrent**: Structured concurrency primitives (pipelines, queues, concurrency limits). Use this package for all concurrent work instead of raw goroutines.
- **fsnotify**: File system watching
- **OpenTelemetry**: Tracing
- **kway-go**: K-way merge for sorted lists

### Adding a New Backend

1. Create package directory under `storage/` (e.g., `storage/azure/`)
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

1. **Import for side effects**: `import _ "github.com/firetiger-oss/tigerblock/storage/s3"`
2. **URI trailing slashes**: Automatically handled by registry
3. **Context cancellation**: Always respect `ctx.Done()`

## Compatibility

- **Go 1.25+** (uses Go 1.23+ iterator patterns `iter.Seq2`)
- Backward compatible API design
- Semantic versioning

## Security

- Never commit credentials
- Use IAM roles for cloud access
- Validate object keys to prevent path traversal
- Consider encryption at rest and in transit
