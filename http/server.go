package http

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/internal/sequtil"
	"github.com/firetiger-oss/storage/uri"
)

type HandlerOptions struct {
	location          string
	maxKeys           int
	presignRedirect   bool
	presignExpiration time.Duration
}

func NewHandlerOptions(options ...HandlerOption) *HandlerOptions {
	opts := &HandlerOptions{
		maxKeys:           listObjectsMaxKeys,
		presignExpiration: 15 * time.Minute,
	}
	for _, option := range options {
		option(opts)
	}
	opts.maxKeys = max(opts.maxKeys, 0)
	opts.maxKeys = min(opts.maxKeys, listObjectsMaxKeys)
	return opts
}

type HandlerOption func(*HandlerOptions)

// WithLocation sets the base location for the bucket handler, which is injected
// as prefix to the object keys returned when listing bucket entries by calling
// ListObjects.
func WithLocation(location string) HandlerOption {
	if !strings.HasSuffix(location, "/") {
		location += "/"
	}
	return func(options *HandlerOptions) { options.location = location }
}

// WithMaxKeys sets the maximum number of keys to return when listing bucket
// entries. Note that it does not limit the number of objects listed by calling
// ListObjects, pagination is handled automatically by the client.
func WithMaxKeys(maxKeys int) HandlerOption {
	return func(options *HandlerOptions) { options.maxKeys = maxKeys }
}

// WithPresignRedirect enables presigned URL redirects. When enabled, the handler
// will generate a presigned URL on the underlying bucket and return a 307 redirect
// to that URL instead of serving the content directly.
func WithPresignRedirect(enabled bool) HandlerOption {
	return func(options *HandlerOptions) { options.presignRedirect = enabled }
}

// WithPresignExpiration sets the expiration duration for presigned URLs generated
// when handling ErrPresignRedirect responses from the bucket. The default is 15 minutes.
func WithPresignExpiration(expiration time.Duration) HandlerOption {
	return func(options *HandlerOptions) { options.presignExpiration = expiration }
}

func BucketHandler(b storage.Bucket, options ...HandlerOption) http.Handler {
	h := NewHandlerOptions(options...)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			handleHEAD(w, r, b, h)
		case http.MethodGet:
			handleGET(w, r, b, h)
		case http.MethodPut:
			handlePUT(w, r, b, h)
		case http.MethodPost:
			handlePOST(w, r, b, h)
		case http.MethodDelete:
			handleDELETE(w, r, b, h)
		case http.MethodOptions:
			handleOPTIONS(w, r, b, h)
		default:
			Error(w, "MethodNotAllowed", "The specified method is not allowed against this resource", r.URL.Path, http.StatusMethodNotAllowed)
		}
	})
}

type headResponseWriter struct {
	base          http.ResponseWriter
	status        int
	contentLength int64
}

func (w *headResponseWriter) WriteHeader(statusCode int) {
	if w.status == 0 {
		w.status = statusCode
	}
}

func (w *headResponseWriter) Header() http.Header {
	return w.base.Header()
}

func (w *headResponseWriter) Write(b []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	w.contentLength += int64(len(b))
	return len(b), nil
}

func (w *headResponseWriter) flush() {
	status := w.status
	if status == 0 {
		status = http.StatusOK
	}
	w.base.Header().Set("Content-Length", strconv.FormatInt(w.contentLength, 10))
	w.base.WriteHeader(status)
}

func makeKey(r *http.Request) string {
	return strings.TrimPrefix(r.URL.Path, "/")
}

func handleHEAD(w http.ResponseWriter, r *http.Request, b storage.Bucket, h *HandlerOptions) {
	if strings.HasSuffix(r.URL.Path, "/") {
		head := &headResponseWriter{base: w}
		handleGET(head, r, b, h)
		head.flush()
	} else {
		if h.presignRedirect {
			presignedURL, err := b.PresignHeadObject(r.Context(), makeKey(r), h.presignExpiration)
			if err != nil {
				writeError(w, err)
				return
			}
			w.Header().Set("Location", presignedURL)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}

		object, err := b.HeadObject(r.Context(), makeKey(r))
		if err != nil {
			if errors.Is(err, storage.ErrPresignRedirect) {
				presignedURL, presignErr := b.PresignHeadObject(r.Context(), makeKey(r), h.presignExpiration)
				if presignErr != nil {
					writeError(w, presignErr)
					return
				}
				w.Header().Set("Location", presignedURL)
				w.WriteHeader(http.StatusTemporaryRedirect)
				return
			}
			writeError(w, err)
			return
		}
		header := w.Header()
		setObject(header, object)
	}
}

type bytesRange struct {
	start int64
	end   int64
}

func (r *bytesRange) ContentLength(size int64) int64 {
	if r.start < 0 {
		return -r.start
	}
	if r.end >= 0 {
		return (r.end + 1) - r.start
	}
	return size - r.start
}

func (r *bytesRange) ContentRange(size int64) string {
	if r.start < 0 {
		return fmt.Sprintf("bytes %d-%d/%d", size+r.start, size-1, size)
	}
	if r.end >= 0 {
		return fmt.Sprintf("bytes %d-%d/%d", r.start, r.end, size)
	}
	return fmt.Sprintf("bytes %d-%d/%d", r.start, size-1, size)
}

func parseBytesRange(rangeHeader string) (*bytesRange, error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return nil, fmt.Errorf("invalid range format")
	}
	rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")

	var start, end int64
	// handle negative start without end, ex: bytes=-10
	if strings.HasPrefix(rangeStr, "-") {
		// handle negative start without end, ex: bytes=-10
		if _, err := fmt.Sscanf(rangeStr, "-%d", &start); err != nil {
			return nil, fmt.Errorf("reading bytes range: %w", err)
		}
		start = 0 - start
		end = -1
	} else if strings.HasSuffix(rangeStr, "-") {
		// handle open ended range, ex: bytes=10-
		if _, err := fmt.Sscanf(rangeStr, "%d-", &start); err != nil {
			return nil, fmt.Errorf("reading bytes range: %w", err)
		}
		end = -1
	} else {
		// handle regular range, ex: bytes=0-10
		if _, err := fmt.Sscanf(rangeStr, "%d-%d", &start, &end); err != nil {
			return nil, fmt.Errorf("reading bytes range: %w", err)
		}
		if start < 0 {
			return nil, fmt.Errorf("invalid range: negative start")
		}
	}

	if end > 0 && start < 0 {
		return nil, fmt.Errorf("invalid range: negative start")
	}

	if end > 0 && start > end {
		return nil, fmt.Errorf("invalid range: start > end")
	}

	return &bytesRange{start: start, end: end}, nil
}

func handleGET(w http.ResponseWriter, r *http.Request, b storage.Bucket, h *HandlerOptions) {
	if r.URL.Path == "/" {
		options := []storage.ListOption{}
		query := r.URL.Query()
		limit := h.maxKeys

		if delimiter := query.Get("delimiter"); delimiter != "" {
			options = append(options, storage.KeyDelimiter(delimiter))
		}

		if prefix := query.Get("prefix"); prefix != "" {
			options = append(options, storage.KeyPrefix(prefix))
		}

		var continuationToken string
		switch query.Get("list-type") {
		case "2":
			startAfter := query.Get("start-after")
			continuationToken = query.Get("continuation-token")
			if continuationToken != "" {
				startAfter = max(startAfter, continuationToken)
			}
			if startAfter != "" {
				options = append(options, storage.StartAfter(startAfter))
			}
		default:
			if marker := query.Get("marker"); marker != "" {
				options = append(options, storage.StartAfter(marker))
			}
		}

		if maxKeys := query.Get("max-keys"); maxKeys != "" {
			n, err := strconv.Atoi(maxKeys)
			if err != nil {
				Error(w, "InvalidArgument", "Invalid value for max-keys: "+maxKeys, "max-keys", http.StatusBadRequest)
				return
			}
			limit = min(limit, max(n, 0))
		}

		listObjects := b.ListObjects(r.Context(), options...)
		listObjects = sequtil.Limit(listObjects, limit)
		listObjects = sequtil.Transform(listObjects, func(object storage.Object) (storage.Object, error) {
			object.Key = h.location + object.Key
			return object, nil
		})

		header := w.Header()
		header.Set("Cache-Control", "no-cache")
		header.Set("Content-Type", "application/xml")
		header.Set("Max-Keys", strconv.Itoa(limit))

		listOptions := storage.NewListOptions(options...)
		_, bucketName, _ := uri.Split(b.Location())
		switch query.Get("list-type") {
		case "2":
			writeListObjectsXMLV2(w, bucketName, listOptions, limit, continuationToken, listObjects)
		default:
			writeListObjectsXML(w, bucketName, listOptions, limit, listObjects)
		}
	} else {
		var httpRange *bytesRange
		var options []storage.GetOption
		var err error

		if bytesRange := r.Header.Get("Range"); bytesRange != "" {
			httpRange, err = parseBytesRange(bytesRange)
			if err != nil {
				writeError(w, err)
				return
			}
			options = append(options, storage.BytesRange(httpRange.start, httpRange.end))
		}

		if h.presignRedirect {
			presignedURL, err := b.PresignGetObject(r.Context(), makeKey(r), time.Hour, options...)
			if err != nil {
				writeError(w, err)
				return
			}
			w.Header().Set("Location", presignedURL)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}

		reader, object, err := b.GetObject(r.Context(), makeKey(r), options...)
		if err != nil {
			if errors.Is(err, storage.ErrPresignRedirect) {
				presignedURL, presignErr := b.PresignGetObject(r.Context(), makeKey(r), h.presignExpiration, options...)
				if presignErr != nil {
					writeError(w, presignErr)
					return
				}
				// For file:// URLs, serve the content directly since HTTP clients
				// cannot follow redirects to file:// URLs.
				if strings.HasPrefix(presignedURL, "file://") {
					filePath := strings.TrimPrefix(presignedURL, "file://")
					serveLocalFile(w, r, filePath, httpRange)
					return
				}
				w.Header().Set("Location", presignedURL)
				w.WriteHeader(http.StatusTemporaryRedirect)
				return
			}
			writeError(w, err)
			return
		}
		defer reader.Close()

		header := w.Header()
		setObject(header, object)

		if httpRange != nil {
			setContentLength(header, httpRange.ContentLength(object.Size))
			setContentRange(header, httpRange.ContentRange(object.Size))
			w.WriteHeader(http.StatusPartialContent)
		}

		io.Copy(w, reader)
	}
}

func handlePUT(w http.ResponseWriter, r *http.Request, b storage.Bucket, h *HandlerOptions) {
	if r.URL.Path == "/" {
		if err := b.Create(r.Context()); err != nil {
			writeError(w, err)
		}
		return
	}

	options := make([]storage.PutOption, 0, 4)
	options = appendIfNotEmpty(options, r.Header, "Cache-Control", storage.CacheControl)
	options = appendIfNotEmpty(options, r.Header, "Content-Type", storage.ContentType)
	options = appendIfNotEmpty(options, r.Header, "Content-Encoding", storage.ContentEncoding)
	options = appendIfNotEmpty(options, r.Header, "If-Match", storage.IfMatch)
	options = appendIfNotEmpty(options, r.Header, "If-None-Match", storage.IfNoneMatch)

	if encoded := r.Header.Get("x-amz-checksum-sha256"); encoded != "" {
		raw, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil || len(raw) != sha256.Size {
			Error(w, "BadDigest", "The X-Amz-Checksum-Sha256 header is not a valid base64-encoded SHA-256.", encoded, http.StatusBadRequest)
			return
		}
		var sum [sha256.Size]byte
		copy(sum[:], raw)
		options = append(options, storage.ChecksumSHA256(sum))
	}

	for key, values := range r.Header {
		if len(values) > 0 && strings.HasPrefix(key, "X-Amz-Meta-") {
			key = strings.TrimPrefix(key, "X-Amz-Meta-")
			key = strings.ToLower(key)
			options = append(options, storage.Metadata(key, values[0]))
		}
	}

	// Handle CopyObject operation (PUT with X-Amz-Copy-Source header)
	if copySource := r.Header.Get("X-Amz-Copy-Source"); copySource != "" {
		handleCopyObject(w, r, b, h, copySource, options)
		return
	}

	if h.presignRedirect {
		presignedURL, err := b.PresignPutObject(r.Context(), makeKey(r), time.Hour, options...)
		if err != nil {
			writeError(w, err)
			return
		}
		w.Header().Set("Location", presignedURL)
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}

	object, err := b.PutObject(r.Context(), makeKey(r), r.Body, options...)
	if err != nil {
		if errors.Is(err, storage.ErrPresignRedirect) {
			presignedURL, presignErr := b.PresignPutObject(r.Context(), makeKey(r), h.presignExpiration, options...)
			if presignErr != nil {
				writeError(w, presignErr)
				return
			}
			w.Header().Set("Location", presignedURL)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}
		writeError(w, err)
		return
	}

	header := w.Header()
	setObjectSize(header, object.Size)
	setHeaderIfNotEmpty(header, "Etag", object.ETag)
	setHeaderIfNotEmpty(header, "Last-Modified", formatTime(object.LastModified))
}

func handleCopyObject(w http.ResponseWriter, r *http.Request, b storage.Bucket, h *HandlerOptions, copySource string, options []storage.PutOption) {
	// Parse copy source: format is "/bucket/key"
	copySource = strings.TrimPrefix(copySource, "/")
	sourceBucket, sourceKey, ok := strings.Cut(copySource, "/")
	if !ok {
		Error(w, "InvalidArgument", "Invalid x-amz-copy-source header", copySource, http.StatusBadRequest)
		return
	}
	sourceKey, err := url.PathUnescape(sourceKey)
	if err != nil {
		Error(w, "InvalidArgument", "Invalid x-amz-copy-source header: "+err.Error(), copySource, http.StatusBadRequest)
		return
	}
	destKey := makeKey(r)

	// Validate that the source bucket matches this server's bucket
	_, expectedBucket, _ := uri.Split(h.location)
	if expectedBucket != "" && sourceBucket != expectedBucket {
		Error(w, "AccessDenied", fmt.Sprintf("Cross-bucket copy not supported: source bucket %q does not match %q", sourceBucket, expectedBucket), copySource, http.StatusForbidden)
		return
	}

	if err := b.CopyObject(r.Context(), sourceKey, destKey, options...); err != nil {
		writeError(w, err)
		return
	}

	// Get the destination object info for the response
	object, err := b.HeadObject(r.Context(), destKey)
	if err != nil {
		writeError(w, err)
		return
	}

	// Return CopyObjectResult XML response
	header := w.Header()
	header.Set("Content-Type", "application/xml")
	setHeaderIfNotEmpty(header, "Etag", object.ETag)
	setHeaderIfNotEmpty(header, "Last-Modified", formatTime(object.LastModified))

	result := CopyObjectResult{
		ETag:         object.ETag,
		LastModified: formatTimeS3(object.LastModified), // S3 SDK expects RFC3339 format
	}
	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	encoder.Encode(result)
}

func handlePOST(w http.ResponseWriter, r *http.Request, b storage.Bucket, h *HandlerOptions) {
	if r.URL.Path != "/" {
		Error(w, "MethodNotAllowed", "The specified method is not allowed against this resource", r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	if !r.URL.Query().Has("delete") {
		Error(w, "MethodNotAllowed", "The specified method is not allowed against this resource", r.URL.Path, http.StatusMethodNotAllowed)
		return
	}

	keys, quiet, err := parseDeleteObjectsRequest(r.Body)
	if err != nil {
		Error(w, "MalformedXML", "The XML you provided was not well-formed", "", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/xml")

	var errors []DeleteError
	var successKeys []string

	for key, err := range b.DeleteObjects(r.Context(), sequtil.Values(keys)) {
		if err != nil {
			errors = append(errors, DeleteError{
				Key:     key,
				Code:    "InternalError",
				Message: err.Error(),
			})
		} else {
			successKeys = append(successKeys, key)
		}
	}

	if err := writeDeleteObjectsResult(w, successKeys, errors, quiet); err != nil {
		Error(w, "InternalError", "We encountered an internal error. Please try again.", "", http.StatusInternalServerError)
	}
}

func handleDELETE(w http.ResponseWriter, r *http.Request, b storage.Bucket, h *HandlerOptions) {
	if r.URL.Path != "/" {
		if h.presignRedirect {
			presignedURL, err := b.PresignDeleteObject(r.Context(), makeKey(r), h.presignExpiration)
			if err != nil {
				writeError(w, err)
				return
			}
			w.Header().Set("Location", presignedURL)
			w.WriteHeader(http.StatusTemporaryRedirect)
			return
		}

		if err := b.DeleteObject(r.Context(), makeKey(r)); err != nil {
			if errors.Is(err, storage.ErrPresignRedirect) {
				presignedURL, presignErr := b.PresignDeleteObject(r.Context(), makeKey(r), h.presignExpiration)
				if presignErr != nil {
					writeError(w, presignErr)
					return
				}
				w.Header().Set("Location", presignedURL)
				w.WriteHeader(http.StatusTemporaryRedirect)
				return
			}
			writeError(w, err)
		}
	} else {
		Error(w, "MethodNotAllowed", "The specified method is not allowed against this resource", r.URL.Path, http.StatusMethodNotAllowed)
	}
}

func handleOPTIONS(w http.ResponseWriter, r *http.Request, b storage.Bucket, h *HandlerOptions) {
	if err := b.Access(r.Context()); err != nil {
		writeError(w, err)
		return
	}
}

func appendIfNotEmpty[T any](options []T, header http.Header, key string, option func(string) T) []T {
	if value := header.Get(key); value != "" {
		options = append(options, option(value))
	}
	return options
}

func setObject(header http.Header, object storage.ObjectInfo) {
	setHeaderIfNotEmpty(header, "Cache-Control", object.CacheControl)
	setHeaderIfNotEmpty(header, "Content-Type", object.ContentType)
	setHeaderIfNotEmpty(header, "Content-Encoding", object.ContentEncoding)
	setHeaderIfNotEmpty(header, "Etag", object.ETag)
	setHeaderIfNotEmpty(header, "Last-Modified", formatTime(object.LastModified))
	setObjectMetadata(header, object.Metadata)
	setContentLength(header, object.Size)
	// This is non-compliant but sometimes AWS servers strip the
	// Content-Length and Content-Range headers, which are supposed
	// to carry the object size.
	setObjectSize(header, object.Size)
}

func setObjectMetadata(header http.Header, metadata map[string]string) {
	for key, value := range metadata {
		header.Set("X-Amz-Meta-"+key, value)
	}
}

func setObjectSize(header http.Header, size int64) {
	setHeaderInt64(header, "X-Amz-Object-Size", size)
}

func setContentLength(header http.Header, contentLength int64) {
	setHeaderInt64(header, "Content-Length", contentLength)
}

func setContentRange(header http.Header, contentRange string) {
	header.Set("Content-Range", contentRange)
}

func setHeaderInt64(header http.Header, key string, value int64) {
	header.Set(key, strconv.FormatInt(value, 10))
}

func writeDeleteObjectsResult(w io.Writer, deletedKeys []string, errors []DeleteError, quiet bool) error {
	result := DeleteObjectsResult{
		Xmlns: s3XMLNamespace,
	}

	if !quiet || len(errors) > 0 {
		for _, key := range deletedKeys {
			result.Deleted = append(result.Deleted, Deleted{Key: key})
		}
	}

	result.Errors = errors

	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	return encoder.Encode(result)
}

func writeError(w http.ResponseWriter, err error) {
	code, status := mapErrorToS3(err)
	// Extract resource from error if available
	// Example: "object/path.txt: not found" -> resource would be "object/path.txt"
	message, resource, _ := strings.Cut(err.Error(), ":")
	message = strings.TrimSpace(message)
	resource = strings.TrimSpace(resource)
	Error(w, code, message, resource, status)
}

// Error writes an S3-compatible XML error response.
func Error(w http.ResponseWriter, code, message, resource string, status int) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	encoder := xml.NewEncoder(w)
	encoder.Indent("", "  ")
	encoder.Encode(&S3Error{
		Code:     code,
		Message:  message,
		Resource: resource,
	})
}

func mapErrorToS3(err error) (string, int) {
	switch {
	case errors.Is(err, storage.ErrInvalidObjectKey):
		return "InvalidArgument", http.StatusBadRequest
	case errors.Is(err, storage.ErrInvalidObjectTag):
		return "InvalidTag", http.StatusBadRequest
	case errors.Is(err, storage.ErrInvalidRange):
		return "InvalidRange", http.StatusBadRequest
	case errors.Is(err, storage.ErrObjectNotFound):
		return "NoSuchKey", http.StatusNotFound
	case errors.Is(err, storage.ErrObjectNotMatch):
		return "PreconditionFailed", http.StatusPreconditionFailed
	case errors.Is(err, storage.ErrChecksumMismatch):
		return "BadDigest", http.StatusBadRequest
	case errors.Is(err, storage.ErrBucketExist):
		return "BucketAlreadyExists", http.StatusConflict
	case errors.Is(err, storage.ErrBucketNotFound):
		return "NoSuchBucket", http.StatusNotFound
	case errors.Is(err, storage.ErrBucketReadOnly):
		return "AccessDenied", http.StatusForbidden
	case errors.Is(err, storage.ErrTooManyRequests):
		return "SlowDown", http.StatusServiceUnavailable
	case errors.Is(err, storage.ErrPresignNotSupported):
		return "NotImplemented", http.StatusNotImplemented
	default:
		return "InternalError", http.StatusInternalServerError
	}
}

func serveLocalFile(w http.ResponseWriter, r *http.Request, filePath string, httpRange *bytesRange) {
	f, err := os.Open(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			Error(w, "NoSuchKey", "The specified key does not exist.", filePath, http.StatusNotFound)
		} else {
			Error(w, "InternalError", err.Error(), filePath, http.StatusInternalServerError)
		}
		return
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		Error(w, "InternalError", err.Error(), filePath, http.StatusInternalServerError)
		return
	}

	size := stat.Size()
	header := w.Header()

	if httpRange != nil {
		start, end := httpRange.start, httpRange.end
		if end == 0 || end > size {
			end = size
		}
		if start >= size {
			Error(w, "InvalidRange", "The requested range is not satisfiable", filePath, http.StatusRequestedRangeNotSatisfiable)
			return
		}
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			Error(w, "InternalError", err.Error(), filePath, http.StatusInternalServerError)
			return
		}
		setContentLength(header, end-start)
		setContentRange(header, httpRange.ContentRange(size))
		w.WriteHeader(http.StatusPartialContent)
		io.CopyN(w, f, end-start)
	} else {
		setContentLength(header, size)
		w.WriteHeader(http.StatusOK)
		io.Copy(w, f)
	}
}
