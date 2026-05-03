package http

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/firetiger-oss/tigerblock/internal/sequtil"
	"github.com/firetiger-oss/tigerblock/secret"
	basicauth "github.com/firetiger-oss/tigerblock/secret/authn/basic"
	bearerauth "github.com/firetiger-oss/tigerblock/secret/authn/bearer"
	"github.com/firetiger-oss/tigerblock/storage"
	"github.com/firetiger-oss/tigerblock/uri"
)

func init() {
	register("http")
	register("https")
}

func register(scheme string) {
	storage.Register(scheme, NewRegistry(scheme))
}

func NewRegistry(scheme string, options ...BucketOption) storage.Registry {
	return storage.RegistryFunc(func(ctx context.Context, host string) (storage.Bucket, error) {
		// `//` after the bucket-name suffix marks the boundary
		// between bucket address and object key for path-style
		// multi-bucket addressing. Cut directly to bypass uri.Split's
		// Clean (which would collapse the marker), and skip past the
		// scheme separator when host arrives scheme-prefixed (the
		// storage layer rejoin form is schemeless, but direct
		// callers may pass the full URI).
		searchStart := 0
		if i := strings.Index(host, "://"); i >= 0 {
			searchStart = i + 3
		}
		if idx := strings.Index(host[searchStart:], "//"); idx >= 0 {
			boundary := searchStart + idx
			bucketLoc := host[:boundary]
			keyPrefix := host[boundary+2:]
			bucketHost := bucketLoc
			if !strings.Contains(bucketLoc, "://") {
				bucketHost = scheme + "://" + bucketLoc
			}
			b := NewBucket(bucketHost, options...)
			if keyPrefix != "" {
				if !strings.HasSuffix(keyPrefix, "/") {
					keyPrefix += "/"
				}
				return storage.WithPrefix(keyPrefix).AdaptBucket(b), nil
			}
			return b, nil
		}
		_, location, prefix := uri.Split(host)
		return NewBucket(uri.Join(scheme, location, prefix), options...), nil
	})
}

func NewBucket(host string, options ...BucketOption) storage.Bucket {
	bucket := &Bucket{
		client:   http.DefaultClient,
		listType: "2",
		host:     host,
		header:   http.Header{},
	}
	for _, option := range options {
		option(bucket)
	}
	return bucket
}

type BucketOption func(*Bucket)

func WithClient(client *http.Client) BucketOption {
	return func(b *Bucket) { b.client = client }
}

func WithHeader(name, value string) BucketOption {
	return func(b *Bucket) { b.header.Set(name, value) }
}

func WithListType(listType string) BucketOption {
	return func(b *Bucket) { b.listType = listType }
}

func WithSigner(signer secret.Signer) BucketOption {
	return func(b *Bucket) { b.signer = signer }
}

func WithBasicAuth(username, password string) BucketOption {
	return func(b *Bucket) {
		base := b.client.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		b.client = &http.Client{Transport: basicauth.NewTransport(base, username, password)}
	}
}

func WithBearerToken(token string) BucketOption {
	return func(b *Bucket) {
		base := b.client.Transport
		if base == nil {
			base = http.DefaultTransport
		}
		b.client = &http.Client{Transport: bearerauth.NewTransport(base, token)}
	}
}

// WithBasePath appends a path segment to the bucket host so every
// subsequent request is rooted under it. Useful when the bucket is
// mounted under a name on a multi-bucket server (e.g. `t4 serve
// nexus=...` exposes nexus at /nexus/), since the storage URI parser
// otherwise folds the path into the list-prefix and addresses the
// server root.
func WithBasePath(path string) BucketOption {
	path = strings.Trim(path, "/")
	return func(b *Bucket) {
		if path == "" {
			return
		}
		b.host = strings.TrimRight(b.host, "/") + "/" + path
	}
}

type Bucket struct {
	client   *http.Client
	listType string
	host     string
	header   http.Header
	signer   secret.Signer
}

func escapeKey(key string) string {
	segments := strings.Split(key, "/")
	for i, s := range segments {
		segments[i] = url.PathEscape(s)
	}
	return strings.Join(segments, "/")
}

func (b *Bucket) newRequest(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	r, err := http.NewRequestWithContext(ctx, method, b.host+"/"+path, body)
	if err != nil {
		return nil, err
	}

	maps.Copy(r.Header, b.header)

	if r.ContentLength == 0 && r.Body != nil {
		r.ContentLength = -1
	}
	if body != nil {
		// Wrap the request body because the http.RoundTripper contract says
		// that the body might be closed concurrently after RoundTrip returns.
		// This would cause the body argument ot be used concurrently after
		// returning from methods like PutObject, wich would result in race
		// conditions. The bodyReadCloser type ensures that when the body is
		// closed, the underlying io.Reader is released and will not be used
		// anymore by the RoundTrip method, all of this is done with proper
		// synchronization to prevent data races.
		r.Body = &bodyReadCloser{body: r.Body}
	} else if r.Body == nil {
		r.Body = http.NoBody
		r.ContentLength = 0
	}

	return r, nil
}

type bodyReadCloser struct {
	body io.ReadCloser
	lock sync.Mutex
}

func (r *bodyReadCloser) Close() error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.body == nil {
		return nil
	}
	defer func() { r.body = nil }()
	return r.body.Close()
}

func (r *bodyReadCloser) Read(b []byte) (int, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if r.body == nil {
		return 0, io.EOF
	}
	return r.body.Read(b)
}

func (b *Bucket) Location() string {
	// Emit the path-style `//` marker when b.host has a path
	// component so the URI round-trips through uri.Split + Join +
	// SingleBucketRegistry's identity comparison. The trailing `//`
	// resolves to the bucket-only path-style form on Split and
	// matches what Join produces from (scheme, multi-segment-loc).
	if scheme, host, ok := strings.Cut(b.host, "://"); ok && strings.Contains(host, "/") {
		return scheme + "://" + host + "//"
	}
	return b.host
}

func (b *Bucket) Access(ctx context.Context) error {
	req, err := b.newRequest(ctx, http.MethodOptions, "", nil)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()

	res, err := b.client.Do(req)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return makeIcebergError(req, res, nil)
	}
	return nil
}

func (b *Bucket) Create(ctx context.Context) error {
	req, err := b.newRequest(ctx, http.MethodPut, "", nil)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()

	res, err := b.client.Do(req)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return makeIcebergError(req, res, nil)
	}
	return nil
}

func (b *Bucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	req, err := b.newRequest(ctx, http.MethodHead, escapeKey(key), nil)
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()

	res, err := b.client.Do(req)
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(req, nil, err)
	}
	defer res.Body.Close()

	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusPartialContent:
	default:
		return storage.ObjectInfo{}, makeIcebergError(req, res, nil)
	}

	return parseObjectInfo(res)
}

func (b *Bucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return nil, storage.ObjectInfo{}, err
	}

	req, err := b.newRequest(ctx, http.MethodGet, escapeKey(key), nil)
	if err != nil {
		return nil, storage.ObjectInfo{}, makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()

	getOptions := storage.NewGetOptions(options...)
	if start, end, ok := getOptions.BytesRange(); ok {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			return nil, storage.ObjectInfo{}, err
		}
		req.Header.Set("Range", BytesRange(start, end))
	}

	res, err := b.client.Do(req)
	if err != nil {
		return nil, storage.ObjectInfo{}, makeIcebergError(req, nil, err)
	}
	closeBody := true
	defer func() {
		if closeBody {
			res.Body.Close()
		}
	}()

	switch res.StatusCode {
	case http.StatusOK:
	case http.StatusPartialContent:
	case http.StatusRequestedRangeNotSatisfiable:
		if _, _, hasRange := getOptions.BytesRange(); hasRange {
			info := storage.ObjectInfo{Size: parseContentRangeTotal(res.Header)}
			return io.NopCloser(bytes.NewReader(nil)), info, nil
		}
		return nil, storage.ObjectInfo{}, makeIcebergError(req, res, nil)
	default:
		return nil, storage.ObjectInfo{}, makeIcebergError(req, res, nil)
	}

	object, err := parseObjectInfo(res)
	if err != nil {
		return nil, storage.ObjectInfo{}, makeIcebergError(req, nil, err)
	}

	closeBody = false
	return res.Body, object, nil
}

func (b *Bucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	req, err := b.newRequest(ctx, http.MethodPut, escapeKey(key), value)
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()

	putOptions := storage.NewPutOptions(options...)
	setHeaderIfNotEmpty(req.Header, "Cache-Control", putOptions.CacheControl())
	setHeaderIfNotEmpty(req.Header, "Content-Type", putOptions.ContentType())
	setHeaderIfNotEmpty(req.Header, "Content-Encoding", putOptions.ContentEncoding())
	setHeaderIfNotEmpty(req.Header, "If-Match", putOptions.IfMatch())
	setHeaderIfNotEmpty(req.Header, "If-None-Match", putOptions.IfNoneMatch())
	setObjectMetadata(req.Header, putOptions.Metadata())
	if sum, ok := putOptions.ChecksumSHA256(); ok {
		req.Header.Set("x-amz-checksum-sha256", base64.StdEncoding.EncodeToString(sum[:]))
	}

	res, err := b.client.Do(req)
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(req, nil, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return storage.ObjectInfo{}, makeIcebergError(req, res, nil)
	}

	object := storage.ObjectInfo{
		Metadata:        putOptions.Metadata(),
		CacheControl:    putOptions.CacheControl(),
		ContentType:     putOptions.ContentType(),
		ContentEncoding: putOptions.ContentEncoding(),
		ETag:            res.Header.Get("Etag"),
	}

	object.LastModified, _ = parseLastModified(res.Header)
	object.Size, _ = parseObjectSize(res.Header)
	if object.Size < 0 {
		object.Size = req.ContentLength
	}
	return object, nil
}

func (b *Bucket) DeleteObject(ctx context.Context, key string) error {
	if err := storage.ValidObjectKey(key); err != nil {
		return err
	}

	req, err := b.newRequest(ctx, http.MethodDelete, escapeKey(key), nil)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()

	res, err := b.client.Do(req)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return makeIcebergError(req, res, nil)
	}

	return nil
}

func (b *Bucket) DeleteObjects(ctx context.Context, objects iter.Seq2[string, error]) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for chunk, err := range sequtil.Chunk(objects, 1000) {
			if err != nil {
				if !yield("", err) {
					return
				}
				continue
			}

			for i, key := range chunk {
				if err := storage.ValidObjectKey(key); err != nil {
					if !yield(key, err) {
						return
					}
					chunk[i] = ""
				}
			}

			chunk = slices.DeleteFunc(chunk, func(key string) bool { return key == "" })
			if len(chunk) == 0 {
				continue
			}

			slices.Sort(chunk)
			request := DeleteObjectsRequest{
				Quiet:   false,
				Objects: make([]DeleteObject, len(chunk)),
			}

			for i, key := range chunk {
				request.Objects[i].Key = key
			}

			buffer := new(bytes.Buffer)
			if err := xml.NewEncoder(buffer).Encode(request); err != nil {
				for _, key := range chunk {
					if !yield(key, err) {
						return
					}
				}
				continue
			}

			req, err := b.newRequest(ctx, http.MethodPost, "?delete=", buffer)
			if err != nil {
				httpErr := makeIcebergError(req, nil, err)
				for _, key := range chunk {
					if !yield(key, httpErr) {
						return
					}
				}
				continue
			}
			req.Header.Set("Content-Type", "application/xml")

			res, err := b.client.Do(req)
			if err != nil {
				req.Body.Close()
				httpErr := makeIcebergError(req, nil, err)
				for _, key := range chunk {
					if !yield(key, httpErr) {
						return
					}
				}
				continue
			}

			if res.StatusCode != http.StatusOK {
				req.Body.Close()
				res.Body.Close()
				httpErr := makeIcebergError(req, res, nil)
				for _, key := range chunk {
					if !yield(key, httpErr) {
						return
					}
				}
				continue
			}

			var result DeleteObjectsResult
			if err := xml.NewDecoder(res.Body).Decode(&result); err != nil {
				req.Body.Close()
				res.Body.Close()
				parseErr := fmt.Errorf("parsing delete response: %w", err)
				for _, key := range chunk {
					if !yield(key, parseErr) {
						return
					}
				}
				continue
			}
			req.Body.Close()
			res.Body.Close()

			slices.SortFunc(result.Errors, func(a, b DeleteError) int {
				return strings.Compare(a.Key, b.Key)
			})

			for _, key := range chunk {
				i, found := slices.BinarySearchFunc(result.Errors, key,
					func(a DeleteError, b string) int { return strings.Compare(a.Key, b) })
				var keyError error
				if found {
					keyError = fmt.Errorf("%s: %s",
						result.Errors[i].Code,
						result.Errors[i].Message)
				}
				if !yield(key, keyError) {
					return
				}
			}
		}
	}
}

func (b *Bucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	if err := storage.ValidObjectKey(from); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(to); err != nil {
		return err
	}

	req, err := b.newRequest(ctx, http.MethodPut, escapeKey(to), nil)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()

	// Extract bucket name from host URL for x-amz-copy-source header
	_, bucketName, _ := uri.Split(b.host)
	req.Header.Set("X-Amz-Copy-Source", "/"+bucketName+"/"+escapeKey(from))

	putOptions := storage.NewPutOptions(options...)
	setHeaderIfNotEmpty(req.Header, "Cache-Control", putOptions.CacheControl())
	setHeaderIfNotEmpty(req.Header, "Content-Type", putOptions.ContentType())
	setHeaderIfNotEmpty(req.Header, "Content-Encoding", putOptions.ContentEncoding())
	setObjectMetadata(req.Header, putOptions.Metadata())

	res, err := b.client.Do(req)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return makeIcebergError(req, res, nil)
	}

	return nil
}

func (b *Bucket) ListObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		listOptions := storage.NewListOptions(options...)
		delimiter := listOptions.KeyDelimiter()
		prefix := listOptions.KeyPrefix()
		startAfter := listOptions.StartAfter()
		totalMaxKeys := listOptions.MaxKeys()
		totalCount := 0

		for {
			count, limit, err := (func() (count, limit int, err error) {
				requestMaxKeys := listObjectsMaxKeys
				if totalMaxKeys > 0 {
					remaining := totalMaxKeys - totalCount
					if remaining <= 0 {
						return 0, 0, nil // No more objects needed
					}
					if remaining < requestMaxKeys {
						requestMaxKeys = remaining
					}
				}
				q := url.Values{
					"list-type": {b.listType},
					"max-keys":  {strconv.Itoa(requestMaxKeys)},
				}
				if delimiter != "" {
					q.Set("delimiter", delimiter)
				}
				if prefix != "" {
					q.Set("prefix", prefix)
				}
				if startAfter != "" {
					switch b.listType {
					case "2":
						q.Set("start-after", startAfter)
					default:
						q.Set("marker", startAfter)
					}
				}

				req, err := b.newRequest(ctx, http.MethodGet, "?"+q.Encode(), nil)
				if err != nil {
					return 0, 0, err
				}
				defer req.Body.Close()

				res, err := b.client.Do(req)
				if err != nil {
					return 0, 0, makeIcebergError(req, nil, err)
				}
				defer res.Body.Close()

				if res.StatusCode != http.StatusOK {
					return 0, 0, makeIcebergError(req, res, nil)
				}
				limit, err = strconv.Atoi(res.Header.Get("Max-Keys"))
				if err != nil {
					return 0, 0, makeIcebergError(req, res, err)
				}

				for object, err := range readListBucketResult(res.Body) {
					if err != nil {
						return 0, 0, makeIcebergError(req, nil, err)
					}
					object.Key = strings.TrimPrefix(object.Key, b.host)
					object.Key = strings.TrimPrefix(object.Key, "/")
					if !yield(object, nil) {
						break
					}
					count++
					startAfter = object.Key
				}
				return count, limit, nil
			})()

			if err != nil {
				yield(storage.Object{}, err)
				return
			}

			if count < limit {
				return // EOF
			}

			if totalCount += count; totalMaxKeys > 0 && totalCount >= totalMaxKeys {
				return
			}
		}
	}
}

func (b *Bucket) WatchObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return storage.WatchObjects(ctx, b, options...)
}

func makeIcebergError(req *http.Request, res *http.Response, err error) error {
	if err != nil {
		return makeError(req, err)
	}
	switch res.StatusCode {
	case http.StatusBadRequest:
		// Distinguish BadDigest (checksum mismatch) from generic
		// validation errors by parsing the S3-style XML body.
		err = storage.ErrInvalidObjectTag
		if s3err, e := ReadS3Error(res.Body); e == nil && s3err.Code == "BadDigest" {
			err = storage.ErrChecksumMismatch
		}
	case http.StatusNotFound:
		err = storage.ErrObjectNotFound
	case http.StatusPreconditionFailed:
		err = storage.ErrObjectNotMatch
	case http.StatusConflict:
		err = storage.ErrBucketExist
	case http.StatusForbidden:
		err = storage.ErrBucketReadOnly
	case http.StatusTooManyRequests:
		err = storage.ErrTooManyRequests
	default:
		b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		err = errors.New(string(b))
	}
	return makeError(req, fmt.Errorf("%d: %w", res.StatusCode, err))
}

func makeError(req *http.Request, err error) error {
	return fmt.Errorf("%s: %w", req.URL, err)
}

func (b *Bucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	if b.signer == nil {
		return "", storage.ErrPresignNotSupported
	}
	u, err := url.Parse(b.host + "/" + escapeKey(key))
	if err != nil {
		return "", err
	}
	return b.signer.Sign(ctx, http.MethodGet, u, time.Now().Add(expiration))
}

func (b *Bucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...storage.PutOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	if b.signer == nil {
		return "", storage.ErrPresignNotSupported
	}
	u, err := url.Parse(b.host + "/" + escapeKey(key))
	if err != nil {
		return "", err
	}
	return b.signer.Sign(ctx, http.MethodPut, u, time.Now().Add(expiration))
}

func (b *Bucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}

func (b *Bucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}
