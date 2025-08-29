package fakes3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/memory"
	"github.com/google/uuid"
)

type Client struct {
	memory.Bucket
	bucket           string
	created          bool
	multipartMutex   sync.Mutex
	multipartUploads map[string]*multipartUpload
}

func NewClient(bucket string) *Client {
	return &Client{bucket: bucket}
}

type multipartUpload struct {
	mutex sync.Mutex
	key   string
	data  bytes.Buffer
}

func (c *Client) CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	if bucket := aws.ToString(params.Bucket); bucket != c.bucket {
		return nil, fmt.Errorf("cannot create bucket: %s", bucket)
	}
	if !c.created {
		return nil, new(types.BucketAlreadyExists)
	}
	c.created = true
	return new(s3.CreateBucketOutput), nil
}

func (c *Client) HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if bucket := aws.ToString(params.Bucket); bucket != c.bucket {
		return nil, fmt.Errorf("cannot head bucket: %s", bucket)
	}
	return new(s3.HeadBucketOutput), nil
}

func (c *Client) HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	object, err := c.Bucket.HeadObject(ctx, aws.ToString(params.Key))
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, &types.NoSuchKey{Message: params.Key}
		}
		return nil, err
	}
	out := &s3.HeadObjectOutput{
		ContentType:     aws.String(object.ContentType),
		ContentEncoding: aws.String(object.ContentEncoding),
		CacheControl:    aws.String(object.CacheControl),
		ETag:            aws.String(object.ETag),
		ContentLength:   aws.Int64(object.Size),
		LastModified:    aws.Time(object.LastModified),
		Metadata:        object.Metadata,
	}
	return out, nil
}

func (c *Client) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	var options []storage.GetOption
	if params.Range != nil {
		var start, end int64
		if _, err := fmt.Sscanf(aws.ToString(params.Range), "bytes=%d-%d", &start, &end); err != nil {
			return nil, fmt.Errorf("reading bytes range: %w", err)
		}
		options = append(options, storage.BytesRange(start, end))
	}
	body, object, err := c.Bucket.GetObject(ctx, aws.ToString(params.Key), options...)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, &types.NoSuchKey{Message: params.Key}
		}
		return nil, err
	}
	return &s3.GetObjectOutput{
		Body:            body,
		ContentLength:   aws.Int64(object.Size),
		ContentEncoding: aws.String(object.ContentEncoding),
		ContentType:     aws.String(object.ContentType),
		CacheControl:    aws.String(object.CacheControl),
		ETag:            aws.String(object.ETag),
		LastModified:    aws.Time(object.LastModified),
		Metadata:        object.Metadata,
	}, nil
}

func (c *Client) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}

	var options []storage.PutOption
	if params.ContentType != nil {
		options = append(options, storage.ContentType(aws.ToString(params.ContentType)))
	}
	if params.ContentEncoding != nil {
		options = append(options, storage.ContentEncoding(aws.ToString(params.ContentEncoding)))
	}
	if params.CacheControl != nil {
		options = append(options, storage.CacheControl(aws.ToString(params.CacheControl)))
	}
	switch {
	case params.IfNoneMatch != nil:
		options = append(options, storage.IfNoneMatch(aws.ToString(params.IfNoneMatch)))
	case params.IfMatch != nil:
		options = append(options, storage.IfMatch(aws.ToString(params.IfMatch)))
	}
	for key, value := range params.Metadata {
		options = append(options, storage.Metadata(key, value))
	}

	object, err := c.Bucket.PutObject(ctx, aws.ToString(params.Key), params.Body, options...)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotMatch) {
			return nil, &smithy.GenericAPIError{Code: "PreconditionFailed"}
		}
		return nil, err
	}
	out := &s3.PutObjectOutput{
		ETag: aws.String(object.ETag),
		Size: aws.Int64(object.Size),
	}
	return out, nil
}

func (c *Client) DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if err := c.Bucket.DeleteObject(ctx, aws.ToString(params.Key)); err != nil {
		return nil, err
	}
	return &s3.DeleteObjectOutput{}, nil
}

func (c *Client) DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	keys := make([]string, len(params.Delete.Objects))
	for i, obj := range params.Delete.Objects {
		keys[i] = aws.ToString(obj.Key)
	}
	if err := c.Bucket.DeleteObjects(ctx, keys); err != nil {
		return nil, err
	}
	return &s3.DeleteObjectsOutput{}, nil
}

func (c *Client) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}

	options := []storage.ListOption{}
	if params.Delimiter != nil {
		options = append(options, storage.KeyDelimiter(aws.ToString(params.Delimiter)))
	}
	if params.Prefix != nil {
		options = append(options, storage.KeyPrefix(aws.ToString(params.Prefix)))
	}
	if params.StartAfter != nil {
		options = append(options, storage.StartAfter(aws.ToString(params.StartAfter)))
	}

	output := new(s3.ListObjectsV2Output)
	for obj, err := range c.Bucket.ListObjects(ctx, options...) {
		if err != nil {
			return nil, err
		}
		if params.Delimiter != nil && strings.HasSuffix(obj.Key, aws.ToString(params.Delimiter)) {
			output.CommonPrefixes = append(output.CommonPrefixes, types.CommonPrefix{
				Prefix: aws.String(obj.Key),
			})
		} else {
			output.Contents = append(output.Contents, types.Object{
				Key:          aws.String(obj.Key),
				Size:         aws.Int64(obj.Size),
				LastModified: aws.Time(obj.LastModified),
			})
		}
	}
	return output, nil
}

func (c *Client) CreateMultipartUpload(ctx context.Context, params *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	id := uuid.New().String()
	c.multipartMutex.Lock()
	defer c.multipartMutex.Unlock()
	if c.multipartUploads == nil {
		c.multipartUploads = make(map[string]*multipartUpload)
	}
	c.multipartUploads[id] = &multipartUpload{key: aws.ToString(params.Key)}
	return &s3.CreateMultipartUploadOutput{UploadId: aws.String(id)}, nil
}

func (c *Client) UploadPart(ctx context.Context, params *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	err := c.withUpload(
		aws.ToString(params.UploadId),
		aws.ToString(params.Key),
		func(id string, upload *multipartUpload) error {
			_, err := upload.data.ReadFrom(params.Body)
			return err
		},
	)
	if err != nil {
		return nil, err
	}
	return &s3.UploadPartOutput{}, nil
}

func (c *Client) CompleteMultipartUpload(ctx context.Context, params *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	err := c.withUpload(
		aws.ToString(params.UploadId),
		aws.ToString(params.Key),
		func(id string, upload *multipartUpload) error {
			if _, err := c.Bucket.PutObject(ctx, upload.key, bytes.NewReader(upload.data.Bytes())); err != nil {
				return err
			}
			c.deleteUpload(id)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return &s3.CompleteMultipartUploadOutput{}, nil
}

func (c *Client) AbortMultipartUpload(ctx context.Context, params *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	err := c.withUpload(
		aws.ToString(params.UploadId),
		aws.ToString(params.Key),
		func(id string, _ *multipartUpload) error {
			c.deleteUpload(id)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return &s3.AbortMultipartUploadOutput{}, nil
}

func (c *Client) withUpload(id, key string, do func(string, *multipartUpload) error) error {
	upload, err := c.lockUpload(id, key)
	if err != nil {
		return err
	}
	defer upload.mutex.Unlock()
	return do(id, upload)
}

func (c *Client) lockUpload(id, key string) (*multipartUpload, error) {
	c.multipartMutex.Lock()
	defer c.multipartMutex.Unlock()
	upload := c.multipartUploads[id]
	if upload == nil {
		return nil, fmt.Errorf("multipart upload %s does not exist", id)
	}
	if key != upload.key {
		return nil, fmt.Errorf("multipart upload %s has key %q instead of %q", id, key, upload.key)
	}
	upload.mutex.Lock()
	return upload, nil
}

func (c *Client) deleteUpload(id string) {
	c.multipartMutex.Lock()
	delete(c.multipartUploads, id)
	c.multipartMutex.Unlock()
}
