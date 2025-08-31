package http

import (
	"bytes"
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"iter"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/uri"
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

type Bucket struct {
	client   *http.Client
	listType string
	host     string
	header   http.Header
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

	req, err := b.newRequest(ctx, http.MethodHead, key, nil)
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

	req, err := b.newRequest(ctx, http.MethodGet, key, nil)
	if err != nil {
		return nil, storage.ObjectInfo{}, makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()

	getOptions := storage.NewGetOptions(options...)
	if start, end, ok := getOptions.BytesRange(); ok {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			return nil, storage.ObjectInfo{}, err
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
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

	req, err := b.newRequest(ctx, http.MethodPut, key, value)
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

	req, err := b.newRequest(ctx, http.MethodDelete, key, nil)
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

func (b *Bucket) DeleteObjects(ctx context.Context, keys []string) error {
	for _, key := range keys {
		if err := storage.ValidObjectKey(key); err != nil {
			return err
		}
	}

	request := DeleteObjectsRequest{
		Quiet:   true,
		Objects: make([]DeleteObject, len(keys)),
	}

	for i, key := range keys {
		request.Objects[i].Key = key
	}

	buffer := new(bytes.Buffer)
	if err := xml.NewEncoder(buffer).Encode(request); err != nil {
		return err
	}

	req, err := b.newRequest(ctx, http.MethodPost, "?delete=", buffer)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer req.Body.Close()
	req.Header.Set("Content-Type", "application/xml")

	res, err := b.client.Do(req)
	if err != nil {
		return makeIcebergError(req, nil, err)
	}
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		return makeIcebergError(req, res, nil)
	}

	var result DeleteObjectsResult
	if err := xml.NewDecoder(res.Body).Decode(&result); err != nil {
		return fmt.Errorf("parsing delete response: %w", err)
	}

	if len(result.Errors) > 0 {
		var errs []error
		for _, e := range result.Errors {
			errs = append(errs, fmt.Errorf("failed to delete %s: %s - %s", e.Key, e.Code, e.Message))
		}
		return errors.Join(errs...)
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
		err = storage.ErrInvalidObjectTag
	case http.StatusNotFound:
		err = storage.ErrObjectNotFound
	case http.StatusPreconditionFailed:
		err = storage.ErrObjectNotMatch
	case http.StatusConflict:
		err = storage.ErrBucketExist
	case http.StatusForbidden:
		err = storage.ErrBucketReadOnly
	default:
		b, _ := io.ReadAll(io.LimitReader(res.Body, 1024))
		err = errors.New(string(b))
	}
	return makeError(req, fmt.Errorf("%d: %w", res.StatusCode, err))
}

func makeError(req *http.Request, err error) error {
	return fmt.Errorf("%s: %w", req.URL, err)
}

func (b *Bucket) PresignGetObject(ctx context.Context, key string, options ...storage.GetOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}

func (b *Bucket) PresignPutObject(ctx context.Context, key string, options ...storage.PutOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}

func (b *Bucket) PresignHeadObject(ctx context.Context, key string) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}

func (b *Bucket) PresignDeleteObject(ctx context.Context, key string) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}
	return "", storage.ErrPresignNotSupported
}
