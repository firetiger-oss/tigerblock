package s3

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	nethttp "net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/firetiger-oss/tigerblock/backoff"
	"github.com/firetiger-oss/tigerblock/cache"
	"github.com/firetiger-oss/tigerblock/internal/sequtil"
	"github.com/firetiger-oss/tigerblock/storage"
	storagehttp "github.com/firetiger-oss/tigerblock/storage/http"
	"github.com/firetiger-oss/tigerblock/uri"
)

func init() {
	storage.Register("s3", NewRegistry())
}

type Client interface {
	CreateBucket(context.Context, *s3.CreateBucketInput, ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	HeadBucket(context.Context, *s3.HeadBucketInput, ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
	HeadObject(context.Context, *s3.HeadObjectInput, ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(context.Context, *s3.GetObjectInput, ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(context.Context, *s3.PutObjectInput, ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	CopyObject(context.Context, *s3.CopyObjectInput, ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
	DeleteObject(context.Context, *s3.DeleteObjectInput, ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(context.Context, *s3.DeleteObjectsInput, ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListObjectsV2(context.Context, *s3.ListObjectsV2Input, ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	UploadPart(context.Context, *s3.UploadPartInput, ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CreateMultipartUpload(context.Context, *s3.CreateMultipartUploadInput, ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	CompleteMultipartUpload(context.Context, *s3.CompleteMultipartUploadInput, ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	AbortMultipartUpload(context.Context, *s3.AbortMultipartUploadInput, ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

func lookupEnvBool(name string) bool {
	return os.Getenv(name) == "true"
}

func NewRegistry(options ...func(*s3.Options)) storage.Registry {
	var cachedClient cache.Value[*s3.Client]
	return storage.RegistryFunc(func(ctx context.Context, bucket string) (storage.Bucket, error) {
		client, err := cachedClient.Load(func() (*s3.Client, error) {
			c, err := config.LoadDefaultConfig(ctx)
			if err != nil {
				return nil, err
			}
			defaultOptions := []func(*s3.Options){
				func(o *s3.Options) {
					o.UsePathStyle = o.UsePathStyle ||
						lookupEnvBool("AWS_S3_USE_PATH_STYLE") ||
						lookupEnvBool("AWS_S3_FORCE_PATH_STYLE")
				},
			}
			return s3.NewFromConfig(c, append(defaultOptions, options...)...), nil
		})
		if err != nil {
			return nil, err
		}
		bucketName, prefix, _ := strings.Cut(bucket, "/")
		b := NewBucket(client, bucketName)
		if prefix != "" {
			if !strings.HasSuffix(prefix, "/") {
				prefix += "/"
			}
			return storage.Prefix(b, prefix), nil
		}
		return b, nil
	})
}

func NewBucket(client Client, bucket string, options ...BucketOption) *Bucket {
	b := &Bucket{client: client, bucket: bucket}
	for _, opt := range options {
		opt(b)
	}
	return b
}

type BucketOption func(*Bucket)

func WithPresignOptions(options ...func(*s3.PresignOptions)) BucketOption {
	return func(b *Bucket) {
		for _, opt := range options {
			opt(&b.presignOptions)
		}
	}
}

type Bucket struct {
	client Client
	bucket string
	// presign configuration and client (lazily initialized)
	presignOptions s3.PresignOptions
	presignClient  cache.Value[*s3.PresignClient]
}

func (b *Bucket) Location() string {
	return uri.Join("s3", b.bucket)
}

func (b *Bucket) Access(ctx context.Context) error {
	_, err := b.client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(b.bucket),
	})
	if err != nil {
		return makeIcebergError(err)
	}
	return nil
}

func (b *Bucket) Create(ctx context.Context) error {
	_, err := b.client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(b.bucket),
	})
	if err != nil {
		return makeIcebergError(err)
	}
	return nil
}

func (b *Bucket) HeadObject(ctx context.Context, key string) (storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	in := newHeadObjectInput(b.bucket, key)
	out, err := b.client.HeadObject(ctx, in)
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(err)
	}

	object := storage.ObjectInfo{
		CacheControl:    aws.ToString(out.CacheControl),
		ContentType:     aws.ToString(out.ContentType),
		ContentEncoding: aws.ToString(out.ContentEncoding),
		ETag:            aws.ToString(out.ETag),
		Size:            aws.ToInt64(out.ContentLength),
		LastModified:    aws.ToTime(out.LastModified),
		Metadata:        out.Metadata,
	}

	if out.ContentRange != nil {
		object.Size, err = parseObjectSizeFromContentRange(aws.ToString(out.ContentRange))
		if err != nil {
			return object, err
		}
	}

	return object, nil
}

func (b *Bucket) GetObject(ctx context.Context, key string, options ...storage.GetOption) (io.ReadCloser, storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return nil, storage.ObjectInfo{}, err
	}

	getOptions := storage.NewGetOptions(options...)
	if start, end, ok := getOptions.BytesRange(); ok {
		if err := storage.ValidObjectRange(key, start, end); err != nil {
			return nil, storage.ObjectInfo{}, err
		}
	}

	in := newGetObjectInput(b.bucket, key, options...)

	out, err := b.client.GetObject(ctx, in)
	if err != nil {
		if _, _, hasRange := getOptions.BytesRange(); hasRange {
			if info, ok := rangeNotSatisfiableResponse(err); ok {
				return io.NopCloser(bytes.NewReader(nil)), info, nil
			}
		}
		return nil, storage.ObjectInfo{}, makeIcebergError(err)
	}

	object := storage.ObjectInfo{
		CacheControl:    aws.ToString(out.CacheControl),
		ContentType:     aws.ToString(out.ContentType),
		ContentEncoding: aws.ToString(out.ContentEncoding),
		ETag:            aws.ToString(out.ETag),
		Size:            aws.ToInt64(out.ContentLength),
		LastModified:    aws.ToTime(out.LastModified),
		Metadata:        out.Metadata,
	}

	if out.ContentRange != nil {
		object.Size, err = parseObjectSizeFromContentRange(aws.ToString(out.ContentRange))
		if err != nil {
			out.Body.Close()
			return nil, object, err
		}
	}

	return out.Body, object, nil
}

func parseObjectSizeFromContentRange(contentRange string) (int64, error) {
	if !strings.HasPrefix(contentRange, "bytes ") {
		return -1, fmt.Errorf("invalid content range does not start with 'bytes': %s", contentRange)
	}
	_, total, ok := strings.Cut(contentRange, "/")
	if !ok {
		return -1, fmt.Errorf("invalid content range does not contain total size: %s", contentRange)
	}
	size, err := strconv.ParseInt(total, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("invalid content range has malformed total size: %s: %w", contentRange, err)
	}
	return size, nil
}

// rangeNotSatisfiableResponse inspects an error returned by GetObject
// and, if it corresponds to a 416 Range Not Satisfiable response,
// returns the ObjectInfo synthesised from the Content-Range header.
// The caller should return an empty reader with that ObjectInfo so
// tail reads past end of object behave like reads of a zero-byte file.
func rangeNotSatisfiableResponse(err error) (storage.ObjectInfo, bool) {
	var respErr *smithyhttp.ResponseError
	if !errors.As(err, &respErr) {
		return storage.ObjectInfo{}, false
	}
	if respErr.HTTPStatusCode() != nethttp.StatusRequestedRangeNotSatisfiable {
		return storage.ObjectInfo{}, false
	}
	info := storage.ObjectInfo{Size: -1}
	if respErr.Response != nil {
		if cr := respErr.Response.Header.Get("Content-Range"); cr != "" {
			if size, perr := parseObjectSizeFromContentRange(cr); perr == nil {
				info.Size = size
			}
		}
	}
	return info, true
}

func (b *Bucket) PutObject(ctx context.Context, key string, value io.Reader, options ...storage.PutOption) (storage.ObjectInfo, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	lastModified := time.Now()
	putOptions := storage.NewPutOptions(options...)
	contentLength, err := putOptions.ContentLength(value)
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(err)
	}

	// Validate IfNoneMatch before building the request
	if ifNoneMatch := putOptions.IfNoneMatch(); ifNoneMatch != "" && ifNoneMatch != "*" {
		return storage.ObjectInfo{}, fmt.Errorf("%s: %s: %w", key, ifNoneMatch, storage.ErrInvalidObjectTag)
	}

	req := newPutObjectInput(b.bucket, key, options...)
	req.Body = value // Add the body which is not handled by the helper

	usePathStyle := false
	if s3Client, _ := b.client.(*s3.Client); s3Client != nil {
		usePathStyle = s3Client.Options().UsePathStyle
	}
	_, seekable := value.(io.Seeker)
	// The AWS SDK's signer needs a seekable body when we dispatch through
	// PutObject. Known-length non-seekable readers hit:
	//
	//  "failed to seek body to start, request stream is not seekable"
	//
	// Normalize that case by spooling to disk so we preserve the direct
	// PutObject path without buffering the entire payload in memory.
	//
	// We also keep the existing path-style + unknown-length workaround below.
	if !seekable && contentLength >= 0 {
		tmpbuf, err := os.CreateTemp("", "s3.object.*")
		if err != nil {
			return storage.ObjectInfo{}, makeIcebergError(err)
		}
		defer os.Remove(tmpbuf.Name())
		defer tmpbuf.Close()

		if _, err := tmpbuf.ReadFrom(value); err != nil {
			return storage.ObjectInfo{}, makeIcebergError(err)
		}
		if _, err := tmpbuf.Seek(0, io.SeekStart); err != nil {
			return storage.ObjectInfo{}, makeIcebergError(err)
		}

		req.Body = tmpbuf
	}

	// This is a very specific case where these three conditions are met:
	// 1. the client is a *native* S3 client
	// 2. the client was configured to use path-style URLs
	// 3. the value was a stream and we could not determine its length
	//
	// When using path-style, we disable mutli-part uploads because we are
	// likely to be talking to a server that is not actually S3, and does
	// not support it. However, the S3 client will try to checksum the object
	// body, which is going to fail due to not being able to seek through the
	// stream. This is the error that we hit:
	//
	//  "failed to seek body to start, request stream is not seekable"
	//
	// To work around this issue, we buffer the entire stream in memory to
	// determine the length and can then comply with the PutObject method.
	// Buffering is risky here because we could be running out of space,
	// which is why we set a limit on the size of the buffer.
	//
	// That being said, there are no production use cases where we expect to
	// fall into this condition: either we are actually connecting to S3 and we
	// would not use path-style, or we would access another s3-compatible object
	// store using the http client instead, which does not have this limitation
	// because it doesn't compute checksums of the object body.
	if usePathStyle && contentLength < 0 {
		tmpbuf, err := os.CreateTemp("", "s3.object.*")
		if err != nil {
			return storage.ObjectInfo{}, makeIcebergError(err)
		}
		defer os.Remove(tmpbuf.Name())
		defer tmpbuf.Close()

		n, err := tmpbuf.ReadFrom(value)
		if err != nil {
			return storage.ObjectInfo{}, makeIcebergError(err)
		}
		if _, err := tmpbuf.Seek(0, io.SeekStart); err != nil {
			return storage.ObjectInfo{}, makeIcebergError(err)
		}

		req.Body = tmpbuf
		contentLength = n
	}

	if contentLength >= 0 {
		req.ContentLength = aws.Int64(contentLength)

		out, err := b.client.PutObject(ctx, req)
		if err != nil {
			return storage.ObjectInfo{}, makeIcebergError(err)
		}

		object := storage.ObjectInfo{
			ContentType:     putOptions.ContentType(),
			ContentEncoding: putOptions.ContentEncoding(),
			CacheControl:    putOptions.CacheControl(),
			ETag:            aws.ToString(out.ETag),
			Size:            contentLength,
			LastModified:    lastModified,
			Metadata:        putOptions.Metadata(),
		}
		return object, nil
	}

	body := &contentLengthReader{R: req.Body}
	req.Body = body

	out, err := manager.NewUploader(b.client).Upload(ctx, req)
	if err != nil {
		return storage.ObjectInfo{}, makeIcebergError(err)
	}

	object := storage.ObjectInfo{
		ContentType:     putOptions.ContentType(),
		ContentEncoding: putOptions.ContentEncoding(),
		CacheControl:    putOptions.CacheControl(),
		ETag:            aws.ToString(out.ETag),
		Size:            body.N,
		LastModified:    lastModified,
		Metadata:        putOptions.Metadata(),
	}
	return object, nil
}

type contentLengthReader struct {
	R io.Reader
	N int64
}

func (r *contentLengthReader) Read(b []byte) (int, error) {
	n, err := r.R.Read(b)
	r.N += int64(n)
	return n, err
}

func (b *Bucket) DeleteObject(ctx context.Context, key string) error {
	if err := storage.ValidObjectKey(key); err != nil {
		return err
	}

	in := newDeleteObjectInput(b.bucket, key)
	_, err := b.client.DeleteObject(ctx, in)
	if err != nil {
		return makeIcebergError(err)
	}
	return nil
}

func (b *Bucket) CopyObject(ctx context.Context, from, to string, options ...storage.PutOption) error {
	if err := storage.ValidObjectKey(from); err != nil {
		return err
	}
	if err := storage.ValidObjectKey(to); err != nil {
		return err
	}

	putOptions := storage.NewPutOptions(options...)

	// Get source object metadata for merging
	srcInfo, err := b.HeadObject(ctx, from)
	if err != nil {
		return err
	}

	// Build CopyObject input with merged metadata
	input := &s3.CopyObjectInput{
		Bucket:            aws.String(b.bucket),
		CopySource:        aws.String(b.bucket + "/" + from),
		Key:               aws.String(to),
		MetadataDirective: types.MetadataDirectiveReplace,
	}

	// Merge metadata: source metadata with overrides from options
	if cc := putOptions.CacheControl(); cc != "" {
		input.CacheControl = aws.String(cc)
	} else if srcInfo.CacheControl != "" {
		input.CacheControl = aws.String(srcInfo.CacheControl)
	}

	if ct := putOptions.ContentType(); ct != "application/octet-stream" {
		input.ContentType = aws.String(ct)
	} else if srcInfo.ContentType != "" {
		input.ContentType = aws.String(srcInfo.ContentType)
	}

	if ce := putOptions.ContentEncoding(); ce != "" {
		input.ContentEncoding = aws.String(ce)
	} else if srcInfo.ContentEncoding != "" {
		input.ContentEncoding = aws.String(srcInfo.ContentEncoding)
	}

	// Merge metadata maps (overrides win)
	metadata := make(map[string]string)
	for k, v := range srcInfo.Metadata {
		metadata[k] = v
	}
	for k, v := range putOptions.Metadata() {
		metadata[k] = v
	}
	if len(metadata) > 0 {
		input.Metadata = metadata
	}

	_, err = b.client.CopyObject(ctx, input)
	if err != nil {
		return makeIcebergError(err)
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
			identifiers := make([]types.ObjectIdentifier, len(chunk))
			for i := range chunk {
				identifiers[i] = types.ObjectIdentifier{Key: &chunk[i]}
			}

			resp, err := b.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: &b.bucket,
				Delete: &types.Delete{Objects: identifiers},
			})

			if err != nil {
				chunkError := makeIcebergError(err)
				for _, key := range chunk {
					if !yield(key, chunkError) {
						return
					}
				}
				continue
			}

			slices.SortFunc(resp.Errors, func(a, b types.Error) int {
				return strings.Compare(aws.ToString(a.Key), aws.ToString(b.Key))
			})

			for _, key := range chunk {
				i, found := slices.BinarySearchFunc(resp.Errors, key,
					func(a types.Error, b string) int { return strings.Compare(aws.ToString(a.Key), b) })
				var keyError error
				if found {
					keyError = fmt.Errorf("%s: %s",
						aws.ToString(resp.Errors[i].Code),
						aws.ToString(resp.Errors[i].Message))
				}
				if !yield(key, keyError) {
					return
				}
			}
		}
	}
}

type listedObject struct {
	key          string
	etag         string
	size         int64
	lastModified time.Time
}

func (b *Bucket) listObjects(ctx context.Context, listOptions *storage.ListOptions) iter.Seq2[listedObject, error] {
	return func(yield func(listedObject, error) bool) {
		listRequest := &s3.ListObjectsV2Input{
			Bucket: aws.String(b.bucket),
		}

		if delimiter := listOptions.KeyDelimiter(); delimiter != "" {
			listRequest.Delimiter = aws.String(delimiter)
		}
		if prefix := listOptions.KeyPrefix(); prefix != "" {
			listRequest.Prefix = aws.String(prefix)
		}
		if startAfter := listOptions.StartAfter(); startAfter != "" {
			listRequest.StartAfter = aws.String(startAfter)
		}

		pages := s3.NewListObjectsV2Paginator(b.client, listRequest)
		objects := make([]listedObject, 0, 100)

		for pages.HasMorePages() {
			p, err := pages.NextPage(ctx)
			if err != nil {
				yield(listedObject{}, makeIcebergError(err))
				return
			}

			for _, commonPrefix := range p.CommonPrefixes {
				objects = append(objects, listedObject{
					key: aws.ToString(commonPrefix.Prefix),
				})
			}

			for _, content := range p.Contents {
				objects = append(objects, listedObject{
					key:          aws.ToString(content.Key),
					etag:         aws.ToString(content.ETag),
					size:         aws.ToInt64(content.Size),
					lastModified: aws.ToTime(content.LastModified),
				})
			}

			slices.SortFunc(objects, func(a, b listedObject) int {
				return strings.Compare(a.key, b.key)
			})

			for _, object := range objects {
				if !yield(object, nil) {
					return
				}
			}

			objects = objects[:0]
		}
	}
}

func (b *Bucket) ListObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	listOptions := storage.NewListOptions(options...)
	listObjects := func(yield func(storage.Object, error) bool) {
		for object, err := range b.listObjects(ctx, listOptions) {
			if !yield(storage.Object{
				Key:          object.key,
				Size:         object.size,
				LastModified: object.lastModified,
			}, err) {
				return
			}
		}
	}
	return sequtil.Limit(listObjects, listOptions.MaxKeys())
}

func (b *Bucket) WatchObjects(ctx context.Context, options ...storage.ListOption) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		type versionedObject struct {
			object  listedObject
			version int
		}

		currentObjects := make(map[string]versionedObject)
		currentVersion := 0
		listOptions := storage.NewListOptions(options...)

		for {
		backoffLoop:
			for _, err := range backoff.Seq(ctx) {
				if err != nil { // context canceled
					return
				}

				var changeCount int
				for object, err := range b.listObjects(ctx, listOptions) {
					if err != nil {
						if !yield(storage.Object{}, err) {
							return
						}
						continue backoffLoop
					}

					current, exists := currentObjects[object.key]
					if !exists ||
						object.size != current.object.size ||
						object.etag != current.object.etag ||
						object.lastModified.After(current.object.lastModified) {
						if !yield(storage.Object{
							Key:          object.key,
							Size:         object.size,
							LastModified: object.lastModified,
						}, nil) {
							return
						}
						changeCount++
					}

					currentObjects[object.key] = versionedObject{
						object:  object,
						version: currentVersion,
					}
				}

				var deletedObjects []listedObject
				for key, object := range currentObjects {
					if object.version < currentVersion {
						deletedObjects = append(deletedObjects, object.object)
						delete(currentObjects, key)
					}
				}

				if len(deletedObjects) > 0 {
					deletionTime := time.Now()

					slices.SortFunc(deletedObjects, func(a, b listedObject) int {
						return -strings.Compare(a.key, b.key)
					})

					for _, object := range deletedObjects {
						if !yield(storage.Object{
							Key:          object.key,
							Size:         -1, // deletion marker
							LastModified: deletionTime,
						}, nil) {
							return
						}
						changeCount++
					}
				}

				currentVersion++
				if changeCount > 0 {
					break // continue to outer loop to reset backoff delay
				}
			}
		}
	}
}

func (b *Bucket) PresignGetObject(ctx context.Context, key string, expiration time.Duration, options ...storage.GetOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}

	presignClient, err := b.loadPresignClient()
	if err != nil {
		return "", err
	}

	request, err := presignClient.PresignGetObject(ctx, newGetObjectInput(b.bucket, key, options...),
		func(po *s3.PresignOptions) {
			// Don't overwrite the entire struct - preserve existing middleware
			// Only copy specific fields from b.presignOptions if they're set
			if b.presignOptions.Expires != 0 && expiration == 0 {
				po.Expires = b.presignOptions.Expires
			} else {
				po.Expires = expiration
			}
			// Copy ClientOptions if set, appending to existing ones
			if len(b.presignOptions.ClientOptions) > 0 {
				po.ClientOptions = append(po.ClientOptions, b.presignOptions.ClientOptions...)
			}
		})
	if err != nil {
		return "", makeIcebergError(err)
	}
	return request.URL, nil
}

func (b *Bucket) PresignPutObject(ctx context.Context, key string, expiration time.Duration, options ...storage.PutOption) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}

	presignClient, err := b.loadPresignClient()
	if err != nil {
		return "", err
	}

	request, err := presignClient.PresignPutObject(ctx, newPutObjectInput(b.bucket, key, options...),
		func(po *s3.PresignOptions) {
			// Don't overwrite the entire struct - preserve existing middleware
			// Only copy specific fields from b.presignOptions if they're set
			if b.presignOptions.Expires != 0 && expiration == 0 {
				po.Expires = b.presignOptions.Expires
			} else {
				po.Expires = expiration
			}
			// Copy ClientOptions if set, appending to existing ones
			if len(b.presignOptions.ClientOptions) > 0 {
				po.ClientOptions = append(po.ClientOptions, b.presignOptions.ClientOptions...)
			}
		})
	if err != nil {
		return "", makeIcebergError(err)
	}
	return request.URL, nil
}

func (b *Bucket) PresignHeadObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}

	presignClient, err := b.loadPresignClient()
	if err != nil {
		return "", err
	}

	request, err := presignClient.PresignHeadObject(ctx, newHeadObjectInput(b.bucket, key),
		func(po *s3.PresignOptions) {
			if b.presignOptions.Expires != 0 && expiration == 0 {
				po.Expires = b.presignOptions.Expires
			} else {
				po.Expires = expiration
			}
			if len(b.presignOptions.ClientOptions) > 0 {
				po.ClientOptions = append(po.ClientOptions, b.presignOptions.ClientOptions...)
			}
		})
	if err != nil {
		return "", makeIcebergError(err)
	}
	return request.URL, nil
}

func (b *Bucket) PresignDeleteObject(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if err := storage.ValidObjectKey(key); err != nil {
		return "", storage.ErrInvalidObjectKey
	}

	presignClient, err := b.loadPresignClient()
	if err != nil {
		return "", err
	}

	request, err := presignClient.PresignDeleteObject(ctx, newDeleteObjectInput(b.bucket, key),
		func(po *s3.PresignOptions) {
			if b.presignOptions.Expires != 0 && expiration == 0 {
				po.Expires = b.presignOptions.Expires
			} else {
				po.Expires = expiration
			}
			if len(b.presignOptions.ClientOptions) > 0 {
				po.ClientOptions = append(po.ClientOptions, b.presignOptions.ClientOptions...)
			}
		})
	if err != nil {
		return "", makeIcebergError(err)
	}
	return request.URL, nil
}

func (b *Bucket) loadPresignClient() (*s3.PresignClient, error) {
	return b.presignClient.Load(func() (*s3.PresignClient, error) {
		s3Client, ok := b.client.(*s3.Client)
		if !ok {
			return nil, storage.ErrPresignNotSupported
		}
		return s3.NewPresignClient(s3Client, func(opts *s3.PresignOptions) {
			// Don't overwrite the entire struct - preserve existing middleware
			// Only copy specific fields from b.presignOptions if they're set
			if b.presignOptions.Expires != 0 {
				opts.Expires = b.presignOptions.Expires
			}
			// Append ClientOptions if set, don't replace
			if len(b.presignOptions.ClientOptions) > 0 {
				opts.ClientOptions = append(opts.ClientOptions, b.presignOptions.ClientOptions...)
			}
		}), nil
	})
}

func newGetObjectInput(bucket, key string, options ...storage.GetOption) *s3.GetObjectInput {
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
	getOptions := storage.NewGetOptions(options...)
	if start, end, ok := getOptions.BytesRange(); ok {
		input.Range = aws.String(storagehttp.BytesRange(start, end))
	}
	return input
}

func newPutObjectInput(bucket, key string, options ...storage.PutOption) *s3.PutObjectInput {
	putOptions := storage.NewPutOptions(options...)

	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		ContentType: aws.String(putOptions.ContentType()),
		Metadata:    putOptions.Metadata(),
	}

	if cacheControl := putOptions.CacheControl(); cacheControl != "" {
		input.CacheControl = aws.String(cacheControl)
	}

	if contentEncoding := putOptions.ContentEncoding(); contentEncoding != "" {
		input.ContentEncoding = aws.String(contentEncoding)
	}

	if ifMatch := putOptions.IfMatch(); ifMatch != "" {
		input.IfMatch = aws.String(ifMatch)
	} else if ifNoneMatch := putOptions.IfNoneMatch(); ifNoneMatch == "*" {
		input.IfNoneMatch = aws.String("*")
	}

	if sum, ok := putOptions.ChecksumSHA256(); ok {
		input.ChecksumAlgorithm = types.ChecksumAlgorithmSha256
		input.ChecksumSHA256 = aws.String(base64.StdEncoding.EncodeToString(sum[:]))
	}

	return input
}

func newHeadObjectInput(bucket, key string) *s3.HeadObjectInput {
	return &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
}

func newDeleteObjectInput(bucket, key string) *s3.DeleteObjectInput {
	return &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}
}

func makeIcebergError(err error) error {
	// Check for operation errors that indicate unsupported signer types
	var operationError *smithy.OperationError
	if errors.As(err, &operationError) {
		if strings.Contains(operationError.Error(), "unsupported signer type") {
			return errors.Join(storage.ErrPresignNotSupported, err)
		}
	}

	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return errors.Join(storage.ErrObjectNotFound, err)
	}
	var notFound *types.NotFound
	if errors.As(err, &notFound) {
		return errors.Join(storage.ErrObjectNotFound, err)
	}
	var bucketAlreadyExists *types.BucketAlreadyExists
	if errors.As(err, &bucketAlreadyExists) {
		return errors.Join(storage.ErrBucketExist, err)
	}
	var apiError smithy.APIError
	if errors.As(err, &apiError) {
		switch apiError.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return errors.Join(storage.ErrObjectNotMatch, err)
		case "TooManyRequestsException", "SlowDown", "503 SlowDown":
			return errors.Join(storage.ErrTooManyRequests, err)
		case "BadDigest":
			// Returned when a precomputed x-amz-checksum-* value
			// (or Content-MD5) does not match what S3 calculated
			// from the body. XAmzContentSHA256Mismatch is
			// intentionally not mapped here — that's a SigV4
			// request-signing failure, not a body-integrity check.
			// See https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
			return errors.Join(storage.ErrChecksumMismatch, err)
		}
	}
	return err
}
