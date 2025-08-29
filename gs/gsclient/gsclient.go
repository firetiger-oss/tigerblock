package gsclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	gcloud "cloud.google.com/go/storage"
	"github.com/firetiger-oss/storage"
	"github.com/google/uuid"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
)

type Client struct {
	httpClient               *http.Client
	escapedBucketName        string
	streamingChunkSize       int
	streamingChunkBufferPool sync.Pool
}

type Option func(*Client)

// WithHTTPClient replaces the internal [http.Client] used to communicate with GCS,
// also replacing internal auth middleware.
//
// Useful for testing.
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		c.httpClient = httpClient
	}
}

// WithStreamingChunkSize sets the size of the buffer created for each streaming chunk.
//
// From the GCS docs:
// > The chunk size should be a multiple of 256 KiB (256 x 1024 bytes), unless it's the last
// > chunk that completes the upload. Larger chunk sizes typically make uploads faster, but note
// > that there's a tradeoff between speed and memory usage. It's recommended that you use at
// > least 8 MiB for the chunk size.
//
// See also DefaultStreamingChunkSize.
func WithStreamingChunkSize(size int) Option {
	return func(c *Client) {
		if size%(256*1024) != 0 {
			panic("chunk size must be a multiple of 256KiB")
		}
		c.streamingChunkSize = size
	}
}

const DefaultStreamingChunkSize = 8 * 1024 * 1024

func NewHTTPClientWithDefaultCredentials(ctx context.Context, baseClient *http.Client) (*http.Client, error) {
	creds, err := google.FindDefaultCredentials(ctx, gcloud.ScopeFullControl)
	if err != nil {
		return nil, err
	}
	httpClient := *baseClient
	httpClient.Transport = &oauth2.Transport{
		Source: oauth2.ReuseTokenSource(nil, creds.TokenSource),
		Base:   httpClient.Transport,
	}
	return &httpClient, nil
}

func NewGoogleCloudStorageClient(ctx context.Context, bucket string, options ...Option) (*Client, error) {
	client := &Client{
		escapedBucketName:  url.PathEscape(bucket),
		streamingChunkSize: DefaultStreamingChunkSize,
	}
	for _, option := range options {
		option(client)
	}
	if client.httpClient == nil {
		var err error
		client.httpClient, err = NewHTTPClientWithDefaultCredentials(ctx, http.DefaultClient)
		if err != nil {
			return nil, err
		}
	}
	return client, nil
}

func (c *Client) beginResumableUpload(ctx context.Context, key string, putOptions *storage.PutOptions) (string, error) {
	metadataBody := struct {
		CacheControl    string            `json:"cacheControl,omitempty"`
		ContentType     string            `json:"contentType,omitempty"`
		ContentEncoding string            `json:"contentEncoding,omitempty"`
		Metadata        map[string]string `json:"metadata,omitempty"`
	}{
		CacheControl:    putOptions.CacheControl(),
		ContentType:     putOptions.ContentType(),
		ContentEncoding: putOptions.ContentEncoding(),
		Metadata:        putOptions.Metadata(),
	}
	metadataBytes, err := json.Marshal(metadataBody)
	if err != nil {
		return "", err
	}

	reqURLQuery, err := func() (url.Values, error) {
		q := make(url.Values, 4)
		q.Set("fields", "cacheControl,contentType,contentEncoding,generation,size,updated,metadata")
		q.Set("name", key)
		q.Set("uploadType", "resumable")
		if ifMatch := putOptions.IfMatch(); ifMatch != "" {
			if generation, err := strconv.ParseInt(ifMatch, 16, 64); err != nil {
				return nil, fmt.Errorf("%s: %s: %w", key, ifMatch, storage.ErrInvalidObjectTag)
			} else {
				q.Set("ifGenerationMatch", fmt.Sprint(generation))
			}
		}
		if ifNoneMatch := putOptions.IfNoneMatch(); ifNoneMatch != "" {
			if ifNoneMatch != "*" {
				return nil, fmt.Errorf("%s: %s: %w", key, ifNoneMatch, storage.ErrInvalidObjectTag)
			} else {
				q.Set("ifGenerationMatch", "0")
			}
		}
		return q, nil
	}()
	if err != nil {
		return "", err
	}

	u := "https://storage.googleapis.com/upload/storage/v1/b/" + c.escapedBucketName + "/o"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(metadataBytes))
	if err != nil {
		return "", err
	}
	req.URL.RawQuery = reqURLQuery.Encode()
	req.Header.Set("Content-Type", "application/json; charset=UTF-8")
	req.ContentLength = int64(len(metadataBytes))
	req.Header.Set("X-Upload-Content-Type", putOptions.ContentType())

	res, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	if res.StatusCode >= 400 {
		resBodyBytes, err := io.ReadAll(res.Body)
		if err != nil {
			return "", err
		}
		if err := unmarshalGSError(resBodyBytes); err != nil {
			return "", err
		} else {
			return "", errors.New(string(resBodyBytes))
		}
	}

	sessionURI := res.Header.Get("Location")
	if sessionURI == "" {
		return "", errors.New("resumable GCS upload session URI not found")
	}
	return sessionURI, nil
}

func (c *Client) sendResumableChunk(ctx context.Context, sessionURI string, data []byte, offset int64, lastChunk bool) (storage.ObjectInfo, error) {
	// https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sessionURI, bytes.NewReader(data))
	if err != nil {
		return storage.ObjectInfo{}, err
	}

	// Content-Range format per standards:
	// - https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Range
	contentRangeRange := strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+int64(len(data))-1, 10)
	contentRangeSize := "*"
	if lastChunk {
		contentRangeSize = strconv.FormatInt(offset+int64(len(data)), 10)
	}
	req.Header.Set("Content-Range", "bytes "+contentRangeRange+"/"+contentRangeSize)
	req.ContentLength = int64(len(data))

	res, err := c.httpClient.Do(req)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	defer res.Body.Close()

	if !lastChunk && res.StatusCode == http.StatusPermanentRedirect {
		// GCS docs:
		// If the request succeeds, the server responds with 308 Resume Incomplete.
		return storage.ObjectInfo{}, nil
	}

	resBodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	if lastChunk && res.StatusCode/100 == 2 {
		// GCS docs:
		// Once the upload completes in its entirety, you receive a 200 OK or 201 Created response,
		// along with any metadata associated with the resource.
		var resBody struct {
			CacheControl    string            `json:"cacheControl,omitempty"`
			ContentType     string            `json:"contentType,omitempty"`
			ContentEncoding string            `json:"contentEncoding,omitempty"`
			Generation      string            `json:"generation,omitempty"`
			Size            string            `json:"size,omitempty"`
			Updated         time.Time         `json:"updated,omitempty"`
			Metadata        map[string]string `json:"metadata,omitempty"`
		}
		if err := json.Unmarshal(resBodyBytes, &resBody); err != nil {
			return storage.ObjectInfo{}, fmt.Errorf("%s: invalid GCS response: %w", sessionURI, err)
		}

		generation, err := strconv.ParseInt(resBody.Generation, 10, 64)
		if err != nil {
			return storage.ObjectInfo{}, fmt.Errorf("%s: invalid generation in GCS response: %q: %w", sessionURI, resBody.Generation, err)
		}

		size, err := strconv.ParseInt(resBody.Size, 10, 64)
		if err != nil {
			return storage.ObjectInfo{}, fmt.Errorf("%s: invalid size in GCS response: %q: %w", sessionURI, resBody.Size, err)
		}

		object := storage.ObjectInfo{
			CacheControl:    resBody.CacheControl,
			ContentType:     resBody.ContentType,
			ContentEncoding: resBody.ContentEncoding,
			ETag:            makeETag(generation),
			Size:            size,
			LastModified:    resBody.Updated,
			Metadata:        resBody.Metadata,
		}
		return object, nil
	}

	if err := unmarshalGSError(resBodyBytes); err != nil {
		return storage.ObjectInfo{}, err
	}
	return storage.ObjectInfo{}, errors.New(string(resBodyBytes))
}

func (c *Client) cancelResumableUpload(ctx context.Context, sessionURI string) error {
	// https://cloud.google.com/storage/docs/performing-resumable-uploads#cancel-upload
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, sessionURI, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Length", "0")

	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode == 499 {
		return nil
	}

	resBodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if err := unmarshalGSError(resBodyBytes); err != nil {
		return err
	} else {
		return errors.New(string(resBodyBytes))
	}
}

// PutObjectStreaming uploads an object to GCS through multiple HTTP requests, in series.
// This is ideal when the content length is not known, as with uploading a parquet file as it is created.
// This operation uploads one chunk at a time, with each chunk stored in an in-memory buffer
// (see [WithStreamingChunkSize] for details).
func (c *Client) PutObjectStreaming(ctx context.Context, key string, data io.Reader, putOptions *storage.PutOptions) (storage.ObjectInfo, error) {
	// GCS resumable uploads to split object content into multiple HTTP requests:
	// - https://cloud.google.com/storage/docs/performing-resumable-uploads#chunked-upload
	// GCS streaming uploads are a special case of resumable uploads, allowing for unknown Content-Length
	// - https://cloud.google.com/storage/docs/streaming-uploads#stream_an_upload

	sessionURI, err := c.beginResumableUpload(ctx, key, putOptions)
	if err != nil {
		return storage.ObjectInfo{}, err
	}

	buffer, _ := c.streamingChunkBufferPool.Get().(*bytes.Buffer)
	if buffer == nil {
		buffer = bytes.NewBuffer(make([]byte, 0, c.streamingChunkSize))
	}
	defer func() {
		buffer.Reset()
		c.streamingChunkBufferPool.Put(buffer)
	}()

	var lastChunk bool
	var offset int64
	for {
		// GCS requires chunk size be a multiple of 256 KiB, so we need to fill this buffer for
		// every chunk.
		n, err := io.CopyN(buffer, data, int64(c.streamingChunkSize))
		if errors.Is(err, io.EOF) {
			err = nil
			lastChunk = true
		}
		if err != nil {
			_ = c.cancelResumableUpload(ctx, sessionURI)
			return storage.ObjectInfo{}, err
		}

		object, err := c.sendResumableChunk(ctx, sessionURI, buffer.Bytes(), offset, lastChunk)
		if err != nil {
			_ = c.cancelResumableUpload(ctx, sessionURI)
			return storage.ObjectInfo{}, err
		}
		if lastChunk {
			return object, nil
		}
		offset += n
		buffer.Reset()
	}
}

func newStreamingPutRequestBody(putOptions *storage.PutOptions, data io.Reader, dataContentLength int64, boundary string) (io.Reader, int64) {
	metadataBody := struct {
		CacheControl    string            `json:"cacheControl,omitempty"`
		ContentType     string            `json:"contentType,omitempty"`
		ContentEncoding string            `json:"contentEncoding,omitempty"`
		Metadata        map[string]string `json:"metadata,omitempty"`
	}{
		CacheControl:    putOptions.CacheControl(),
		ContentType:     putOptions.ContentType(),
		ContentEncoding: putOptions.ContentEncoding(),
		Metadata:        putOptions.Metadata(),
	}
	metadataBytes, err := json.Marshal(metadataBody)
	if err != nil {
		panic(err)
	}

	// Construct a multipart/related request body with zero-copy streaming for memory efficiency.
	//
	// Alternative approach using mime/multipart package would require unnecessary memory copies:
	// - mime/multipart.Writer requires an io.Writer interface, forcing us to buffer data
	// - We would need io.Pipe + goroutine to bridge io.Reader -> io.Writer -> io.Reader
	// - This creates extra copying and goroutine overhead
	//
	// Our implementation uses io.MultiReader to chain readers directly, enabling:
	// - Zero-copy streaming from caller's io.Reader through to HTTP request
	// - No intermediate buffering or goroutines required
	// - Direct reference passing preserves memory efficiency
	metadataHeader := "--" + boundary + "\r\n" +
		"Content-Type: application/json; charset=UTF-8\r\n" +
		"Content-Length: " + strconv.Itoa(len(metadataBytes)) + "\r\n" +
		"\r\n"

	dataHeader := "\r\n" +
		"--" + boundary + "\r\n" +
		"Content-Type: " + putOptions.ContentType() + "\r\n" +
		"Content-Length: " + strconv.FormatInt(dataContentLength, 10) + "\r\n" +
		"\r\n"

	closingBoundary := "\r\n" +
		"--" + boundary + "--\r\n"

	reqBody := io.MultiReader(
		strings.NewReader(metadataHeader),
		bytes.NewReader(metadataBytes),
		strings.NewReader(dataHeader),
		data,
		strings.NewReader(closingBoundary))

	return reqBody, int64(len(metadataHeader)+len(metadataBytes)+len(dataHeader)+len(closingBoundary)) + dataContentLength
}

// PutObjectSingleRequest uploads an object to GCS with a single HTTP request.
// This is ideal when low round-trip latency is critical, as with uploading many small objects.
// The caller is responsible for any desired buffering.
func (c *Client) PutObjectSingleRequest(ctx context.Context, key string, data io.Reader, dataContentLength int64, putOptions *storage.PutOptions) (storage.ObjectInfo, error) {
	// GCS docs put this in the "single-request upload" category, and also call it
	// "JSON API multipart upload (a single-request upload that includes object metadata)"
	// - https://cloud.google.com/storage/docs/uploads-downloads#uploads
	// - https://cloud.google.com/storage/docs/uploading-objects

	multipartBoundary := uuid.New().String()
	reqBody, reqBodyContentLength := newStreamingPutRequestBody(putOptions, data, dataContentLength, multipartBoundary)

	q := url.Values{
		"fields":     {"cacheControl,contentType,contentEncoding,generation,size,updated,metadata"},
		"name":       {key},
		"uploadType": {"multipart"},
	}
	if ifMatch := putOptions.IfMatch(); ifMatch != "" {
		if generation, err := strconv.ParseInt(ifMatch, 16, 64); err != nil {
			return storage.ObjectInfo{}, fmt.Errorf("%s: %s: %w", key, ifMatch, storage.ErrInvalidObjectTag)
		} else {
			q.Set("ifGenerationMatch", fmt.Sprint(generation))
		}
	}
	if ifNoneMatch := putOptions.IfNoneMatch(); ifNoneMatch != "" {
		if ifNoneMatch != "*" {
			return storage.ObjectInfo{}, fmt.Errorf("%s: %s: %w", key, ifNoneMatch, storage.ErrInvalidObjectTag)
		} else {
			q.Set("ifGenerationMatch", "0")
		}
	}

	u := "https://storage.googleapis.com/upload/storage/v1/b/" + c.escapedBucketName + "/o"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, reqBody)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	req.URL.RawQuery = q.Encode()
	req.Header.Set("Content-Type", "multipart/related; boundary="+multipartBoundary)
	req.ContentLength = reqBodyContentLength

	res, err := c.httpClient.Do(req)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	defer res.Body.Close()
	resBodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	if res.StatusCode >= 400 {
		if err := unmarshalGSError(resBodyBytes); err != nil {
			return storage.ObjectInfo{}, err
		} else {
			return storage.ObjectInfo{}, errors.New(string(resBodyBytes))
		}
	}

	var resBody struct {
		CacheControl    string            `json:"cacheControl,omitempty"`
		ContentType     string            `json:"contentType,omitempty"`
		ContentEncoding string            `json:"contentEncoding,omitempty"`
		Generation      string            `json:"generation,omitempty"`
		Size            string            `json:"size,omitempty"`
		Updated         time.Time         `json:"updated,omitempty"`
		Metadata        map[string]string `json:"metadata,omitempty"`
	}
	if err := json.Unmarshal(resBodyBytes, &resBody); err != nil {
		return storage.ObjectInfo{}, fmt.Errorf("%s: invalid GCS response: %w", key, err)
	}

	generation, err := strconv.ParseInt(resBody.Generation, 10, 64)
	if err != nil {
		return storage.ObjectInfo{}, fmt.Errorf("%s: invalid generation in GCS response: %q: %w", key, resBody.Generation, err)
	}

	size, err := strconv.ParseInt(resBody.Size, 10, 64)
	if err != nil {
		return storage.ObjectInfo{}, fmt.Errorf("%s: invalid size in GCS response: %q: %w", key, resBody.Size, err)
	}

	return storage.ObjectInfo{
		CacheControl:    resBody.CacheControl,
		ContentType:     resBody.ContentType,
		ContentEncoding: resBody.ContentEncoding,
		ETag:            makeETag(generation),
		Size:            size,
		LastModified:    resBody.Updated,
		Metadata:        resBody.Metadata,
	}, nil
}

func makeETag(generation int64) string {
	return fmt.Sprintf("%016x", generation)
}

func unmarshalGSError(responseBody []byte) error {
	var jerr struct {
		Error *googleapi.Error `json:"error,omitempty"`
	}
	err := json.Unmarshal(responseBody, &jerr)
	if err == nil && jerr.Error != nil {
		jerr.Error.Body = string(responseBody)
		return jerr.Error
	}
	return nil
}
