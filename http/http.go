package http

import (
	"encoding/xml"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/firetiger-oss/storage"
)

const (
	listObjectsMaxKeys = 1000
	s3XMLNamespace     = "http://s3.amazonaws.com/doc/2006-03-01/"
)

type ListBucketResult struct {
	XMLName        xml.Name       `xml:"ListBucketResult"`
	Xmlns          string         `xml:"xmlns,attr"`
	Name           string         `xml:"Name"`
	Prefix         string         `xml:"Prefix,omitempty"`
	Marker         string         `xml:"Marker,omitempty"`
	NextMarker     string         `xml:"NextMarker,omitempty"`
	MaxKeys        int            `xml:"MaxKeys,omitempty"`
	Delimiter      string         `xml:"Delimiter,omitempty"`
	IsTruncated    bool           `xml:"IsTruncated,omitempty"`
	Contents       []Content      `xml:"Contents,omitempty"`
	CommonPrefixes []CommonPrefix `xml:"CommonPrefixes,omitempty"`
}

type Content struct {
	Key          string     `xml:"Key"`
	LastModified string     `xml:"LastModified,omitempty"`
	ETag         string     `xml:"ETag,omitempty"`
	Size         int64      `xml:"Size,omitempty"`
	StorageClass string     `xml:"StorageClass,omitempty"`
	Owner        *OwnerInfo `xml:"Owner,omitempty"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type OwnerInfo struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type ListBucketResultV2 struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Xmlns                 string         `xml:"xmlns,attr"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	KeyCount              int            `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []Content      `xml:"Contents,omitempty"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
}

func parseObjectInfo(res *http.Response) (storage.ObjectInfo, error) {
	object := storage.ObjectInfo{
		CacheControl:    res.Header.Get("Cache-Control"),
		ContentType:     res.Header.Get("Content-Type"),
		ContentEncoding: res.Header.Get("Content-Encoding"),
		ETag:            res.Header.Get("Etag"),
	}

	object.Size, _ = parseObjectSize(res.Header)
	if object.Size < 0 {
		switch res.StatusCode {
		case http.StatusPartialContent:
			_, _, object.Size, _ = parseContentRange(res.Header)
		default:
			object.Size = res.ContentLength
		}
	}

	lastModified, err := parseLastModified(res.Header)
	if err != nil {
		return storage.ObjectInfo{}, fmt.Errorf("parsing last modified: %w", err)
	}
	object.LastModified = lastModified

	metadata, err := parseMetadata(res.Header)
	if err != nil {
		return storage.ObjectInfo{}, fmt.Errorf("parsing metadata: %w", err)
	}
	object.Metadata = metadata
	return object, nil
}

func parseContentRange(header http.Header) (first, last, total int64, err error) {
	contentRange := header.Get("Content-Range")
	if !strings.HasPrefix(contentRange, "bytes ") {
		return 0, 0, -1, fmt.Errorf("invalid Content-Range prefix: %q", contentRange)
	}
	contentRange = strings.TrimPrefix(contentRange, "bytes ")
	contentRange, objectSize, _ := strings.Cut(contentRange, "/")
	objectStart, objectEnd, ok := strings.Cut(contentRange, "-")
	if !ok {
		return 0, 0, -1, fmt.Errorf("invalid Content-Range format: %q", contentRange)
	}
	first, err = strconv.ParseInt(objectStart, 10, 64)
	if err != nil {
		return 0, 0, -1, fmt.Errorf("parsing Content-Range first index: %q: %w", contentRange, err)
	}
	last, err = strconv.ParseInt(objectEnd, 10, 64)
	if err != nil {
		return 0, 0, -1, fmt.Errorf("parsing Content-Range last index: %q: %w", contentRange, err)
	}
	total, err = strconv.ParseInt(objectSize, 10, 64)
	if err != nil {
		total = -1
	}
	return first, last, total, nil
}

func parseLastModified(header http.Header) (time.Time, error) {
	lastModified := header.Get("Last-Modified")
	if lastModified == "" {
		return time.Time{}, nil
	}
	t, err := http.ParseTime(lastModified)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing last modified: %w", err)
	}
	return t, nil
}

func parseMetadata(header http.Header) (map[string]string, error) {
	metadata := make(map[string]string)

	for key, values := range header {
		if len(values) > 0 && strings.HasPrefix(key, "X-Amz-Meta-") {
			key = strings.TrimPrefix(key, "X-Amz-Meta-")
			key = strings.ToLower(key)
			metadata[key] = values[0]
		}
	}

	return metadata, nil
}

func parseObjectSize(header http.Header) (int64, error) {
	if objectSize := header.Get("X-Amz-Object-Size"); objectSize != "" {
		size, err := strconv.ParseInt(objectSize, 10, 64)
		if err != nil {
			return -1, fmt.Errorf("parsing X-Amz-Object-Size: %w", err)
		}
		return size, nil
	}
	return -1, nil
}

func formatTime(t time.Time) string {
	return t.UTC().Format(http.TimeFormat)
}

func formatTimeS3(t time.Time) string {
	return t.UTC().Format(time.RFC3339)
}

func readListBucketResult(r io.Reader) iter.Seq2[storage.Object, error] {
	return func(yield func(storage.Object, error) bool) {
		var result ListBucketResult

		if err := xml.NewDecoder(r).Decode(&result); err != nil {
			yield(storage.Object{}, fmt.Errorf("parsing XML response: %w", err))
			return
		}

		for _, content := range result.Contents {
			lastModified, _ := time.Parse(time.RFC3339, content.LastModified)
			object := storage.Object{
				Key:          content.Key,
				Size:         content.Size,
				LastModified: lastModified,
			}
			if !yield(object, nil) {
				return
			}
		}

		for _, prefix := range result.CommonPrefixes {
			object := storage.Object{
				Key: prefix.Prefix,
			}
			if !yield(object, nil) {
				return
			}
		}
	}
}

func writeListObjectsXML(w io.Writer, bucketName string, options *storage.ListOptions, maxKeys int, objects iter.Seq2[storage.Object, error]) error {
	keyPrefix := options.KeyPrefix()
	keyDelimiter := options.KeyDelimiter()

	result := ListBucketResult{
		Xmlns:     s3XMLNamespace,
		Name:      bucketName,
		MaxKeys:   maxKeys,
		Prefix:    keyPrefix,
		Delimiter: keyDelimiter,
		Contents:  []Content{},
	}

	prefixesMap := make(map[string]bool)
	count := 0
	lastKey := ""

	for object, err := range objects {
		if err != nil {
			return err
		}

		if keyDelimiter != "" && isCommonPrefix(object.Key, keyDelimiter) {
			prefix := object.Key
			if !prefixesMap[prefix] {
				prefixesMap[prefix] = true
				result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{
					Prefix: prefix,
				})
			}
		} else {
			result.Contents = append(result.Contents, Content{
				Key:          object.Key,
				LastModified: formatTimeS3(object.LastModified),
				Size:         object.Size,
				StorageClass: "STANDARD",
			})
		}

		count++
		lastKey = object.Key
	}

	// If we received maxKeys objects and the last key isn't empty,
	// then results might be truncated
	if count >= maxKeys && lastKey != "" {
		result.IsTruncated = true
		result.NextMarker = lastKey
	}

	xmlEncoder := xml.NewEncoder(w)
	xmlEncoder.Indent("", "  ")
	return xmlEncoder.Encode(result)
}

func writeListObjectsXMLV2(w io.Writer, bucketName string, options *storage.ListOptions, maxKeys int, continuationToken string, objects iter.Seq2[storage.Object, error]) error {
	keyPrefix := options.KeyPrefix()
	keyDelimiter := options.KeyDelimiter()

	result := ListBucketResultV2{
		Xmlns:             s3XMLNamespace,
		Name:              bucketName,
		MaxKeys:           maxKeys,
		Prefix:            keyPrefix,
		Delimiter:         keyDelimiter,
		Contents:          []Content{},
		ContinuationToken: continuationToken,
		StartAfter:        options.StartAfter(),
	}

	prefixesMap := make(map[string]bool)
	count := 0
	lastKey := ""

	for object, err := range objects {
		if err != nil {
			return err
		}

		if keyDelimiter != "" && isCommonPrefix(object.Key, keyDelimiter) {
			prefix := object.Key
			if !prefixesMap[prefix] {
				prefixesMap[prefix] = true
				result.CommonPrefixes = append(result.CommonPrefixes, CommonPrefix{
					Prefix: prefix,
				})
			}
		} else {
			result.Contents = append(result.Contents, Content{
				Key:          object.Key,
				LastModified: formatTimeS3(object.LastModified),
				Size:         object.Size,
				StorageClass: "STANDARD",
			})
		}

		count++
		lastKey = object.Key
	}

	result.KeyCount = len(result.Contents) + len(result.CommonPrefixes)

	// If we received maxKeys objects and the last key isn't empty,
	// then results might be truncated
	if count >= maxKeys && lastKey != "" {
		result.IsTruncated = true
		result.NextContinuationToken = lastKey
	}

	xmlEncoder := xml.NewEncoder(w)
	xmlEncoder.Indent("", "  ")
	return xmlEncoder.Encode(result)
}

func isCommonPrefix(key, delimiter string) bool {
	return delimiter != "" && key != "" && strings.HasSuffix(key, delimiter)
}

type DeleteObjectsRequest struct {
	XMLName xml.Name       `xml:"Delete"`
	Objects []DeleteObject `xml:"Object"`
	Quiet   bool           `xml:"Quiet"`
}

type DeleteObject struct {
	Key string `xml:"Key"`
}

type DeleteObjectsResult struct {
	XMLName xml.Name      `xml:"DeleteResult"`
	Xmlns   string        `xml:"xmlns,attr"`
	Deleted []Deleted     `xml:"Deleted,omitempty"`
	Errors  []DeleteError `xml:"Error,omitempty"`
}

type Deleted struct {
	Key string `xml:"Key"`
}

type DeleteError struct {
	Key     string `xml:"Key"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type S3Error struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`
}

func parseDeleteObjectsRequest(r io.Reader) ([]string, bool, error) {
	var request DeleteObjectsRequest
	if err := xml.NewDecoder(r).Decode(&request); err != nil {
		return nil, false, fmt.Errorf("parsing DeleteObjects request: %w", err)
	}

	keys := make([]string, len(request.Objects))
	for i, obj := range request.Objects {
		keys[i] = obj.Key
	}

	return keys, request.Quiet, nil
}

func setHeaderIfNotEmpty(h http.Header, key, value string) {
	if value != "" {
		h.Set(key, value)
	}
}

// StripBucketNamePrefix is an http middleware that strips the bucket name from
// the URL path. This is useful when using path-style URLs with the S3 client,
// where the bucket name is passed as prefix in the path instead of as a
// subdomain.
func StripBucketNamePrefix(bucketName string, handler http.Handler) http.Handler {
	// Note: we can't use http.StripPrefix here because it turns requests to
	// the root "/bucketName" into an empty path instead of "/".
	prefix := "/" + bucketName
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, prefix) {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		u := *r.URL
		u.Path = r.URL.Path[len(prefix):]
		if u.Path == "" {
			u.Path = "/"
		}
		u.Opaque = ""
		u.RawPath = ""

		r = r.WithContext(r.Context()) // shallow copy
		r.URL = &u

		handler.ServeHTTP(w, r)
	})
}

func BytesRange(start, end int64) string {
	b := make([]byte, 0, 32)
	b = append(b, "bytes="...)
	b = strconv.AppendInt(b, start, 10)
	b = append(b, '-')
	if end >= 0 {
		b = strconv.AppendInt(b, end, 10)
	}
	return string(b)
}
