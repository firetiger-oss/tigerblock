# tigerblock [![CI](https://github.com/firetiger-oss/tigerblock/actions/workflows/ci.yml/badge.svg)](https://github.com/firetiger-oss/tigerblock/actions/workflows/ci.yml) [![Go Reference](https://pkg.go.dev/badge/github.com/firetiger-oss/tigerblock.svg)](https://pkg.go.dev/github.com/firetiger-oss/tigerblock)

<p align="center">
  <img width="500" height="335" alt="Gemini_Generated_Image_msys2umsys2umsys" src="https://github.com/user-attachments/assets/2c8da1bf-8ac2-41cc-84ca-c16d4cbb91b6" />
</p>

Batteries-included toolkit for building applications on top of object storage in Go.

## Motivation

Object storage is one of the most powerful building blocks available to
application developers — infinitely scalable, durable, and cheap. There are
already Go packages that abstract away provider differences behind a common
interface, and that part is relatively straightforward. What's harder is
everything else you need to actually build on top of object storage: presigned
URLs, caching layers, bucket notifications, secret storage, observability,
and composable middleware — the kind of infrastructure that every serious
application ends up reimplementing from scratch.

`tigerblock` ships all of that in one cohesive toolkit. At its core is
a single [`Bucket`](https://pkg.go.dev/github.com/firetiger-oss/tigerblock/storage#Bucket)
interface covering S3, Cloudflare R2, Google Cloud Storage, the local file system,
HTTP, and in-memory storage — pick a URI, import a driver, and go. But the real value is
what's built on top: composable adapters for caching, prefixing,
instrumentation, and read-only access; first-class presigned URL support across
backends; bucket change notifications; and a secret management layer. Streaming
operations return `iter.Seq2` iterators that plug straight into range loops and
the standard library, keeping everything idiomatic.

Whether you are building a data pipeline, a media service, or a CLI tool that
needs to talk to the cloud, `tigerblock` is designed to let you focus on your
application instead of the plumbing underneath it.

## Usage

### [storage.LoadBucket](https://pkg.go.dev/github.com/firetiger-oss/tigerblock/storage#LoadBucket)

Load a bucket by URI. The scheme selects the backend — import the backend
package for side effects to register it.

```go
import (
    "github.com/firetiger-oss/tigerblock/storage"
    _ "github.com/firetiger-oss/tigerblock/storage/s3"   // register s3:// scheme
    _ "github.com/firetiger-oss/tigerblock/storage/gs"   // register gs:// scheme
    _ "github.com/firetiger-oss/tigerblock/storage/file" // register file:// scheme
)

bucket, err := storage.LoadBucket(ctx, "s3://my-bucket")
```

### [storage.GetObject](https://pkg.go.dev/github.com/firetiger-oss/tigerblock/storage#GetObject) / [storage.PutObject](https://pkg.go.dev/github.com/firetiger-oss/tigerblock/storage#PutObject)

Top-level convenience functions operate directly on object URIs without
loading a bucket first.

```go
// Write an object
_, err := storage.PutObject(ctx, "s3://my-bucket/path/to/file.txt",
    strings.NewReader("Hello, World!"),
    storage.ContentType("text/plain"),
)

// Read it back
reader, info, err := storage.GetObject(ctx, "s3://my-bucket/path/to/file.txt")
defer reader.Close()
```

### [storage.ListObjects](https://pkg.go.dev/github.com/firetiger-oss/tigerblock/storage#ListObjects)

List objects under a prefix. Results stream as an iterator.

```go
for object, err := range storage.ListObjects(ctx, "s3://my-bucket/logs/") {
    if err != nil {
        return err
    }
    fmt.Printf("%s (%d bytes)\n", object.Key, object.Size)
}
```

### [storage.AdaptBucket](https://pkg.go.dev/github.com/firetiger-oss/tigerblock/storage#AdaptBucket)

Wrap a bucket with adapters to add caching, prefixing, instrumentation,
or read-only protection.

```go
bucket = storage.AdaptBucket(bucket,
    storage.WithPrefix("data/"),
    storage.NewCache(),
    storage.WithInstrumentation(),
)

readOnly := storage.ReadOnlyBucket(bucket)
```

### Backends

| Backend | URI | Import |
|---------|-----|--------|
| Amazon S3 | `s3://bucket/prefix` | `_ "github.com/firetiger-oss/tigerblock/storage/s3"` |
| Cloudflare R2 | `r2://bucket/prefix` | `_ "github.com/firetiger-oss/tigerblock/storage/r2"` |
| Google Cloud Storage | `gs://bucket/prefix` | `_ "github.com/firetiger-oss/tigerblock/storage/gs"` |
| Local file system | `file:///path` | `_ "github.com/firetiger-oss/tigerblock/storage/file"` |
| In-memory | `:memory:` | `_ "github.com/firetiger-oss/tigerblock/storage/memory"` |
| HTTP (S3-compatible) | `http://host/path` | `_ "github.com/firetiger-oss/tigerblock/storage/http"` |

## CLI (`t4`)

`t4` is the command line interface that ships with `tigerblock`. It exposes
the same backends used by the library — pick a URI and the matching scheme
selects the backend.

### Install

```sh
go install github.com/firetiger-oss/tigerblock/cmd/t4@latest
```

### Help

Run `t4 --help` to see the list of available commands and global flags:

```text
$ t4 --help
A command line interface for listing, copying, and managing objects across storage backends (S3, GCS, file, memory, HTTP).

Usage:
  t4 [command]

Available Commands:
  completion  Generate the autocompletion script for the specified shell
  cp          Copy objects between storage locations
  help        Help about any command
  ls          List objects at bucket URIs
  rm          Delete objects at URIs
  serve       Serve a storage bucket over HTTP
  stat        Show detailed object information

Flags:
      --basic-auth string     HTTP basic auth credentials in username:password format
      --bearer-token string   HTTP bearer token for authentication
  -h, --help                  help for t4

Use "t4 [command] --help" for more information about a command.
```

Each subcommand has its own help (`t4 ls --help`, `t4 serve --help`, ...).

### `t4 ls`

List objects at one or more bucket URIs.

```sh
# List the top level of a bucket (uses "/" as the delimiter by default)
t4 ls s3://my-bucket/

# Long format: size, last-modified, key
t4 ls -l s3://my-bucket/logs/

# Recurse and emit JSON
t4 ls -r -o json s3://my-bucket/logs/

# Cap the number of results
t4 ls -n 100 gs://my-bucket/data/
```

### `t4 serve`

Start an HTTP server that exposes a bucket via the S3-compatible handler from
`storage/http`. Useful for sharing a local directory, an in-memory bucket, or
fronting a remote bucket behind authentication.

```sh
# Serve a local directory on :8184
t4 serve file:///var/data

# Serve a bucket on a custom port
t4 serve --http :9000 s3://my-bucket

# Require HTTP basic auth, with the password loaded from a secret store
t4 serve --basic-auth-username alice \
         --basic-auth-secret-id env://MY_PASSWORD \
         file:///var/data

# Require a bearer token loaded from AWS Secrets Manager
t4 serve --bearer-token-secret-id awssm://my-secret \
         s3://my-bucket
```

## Contributing

Contributions are welcome! To get started:

1. Ensure you have Go 1.25+ installed
2. Run `go test ./...` to verify tests pass

Please report bugs and feature requests via [GitHub Issues](https://github.com/firetiger-oss/tigerblock/issues).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
