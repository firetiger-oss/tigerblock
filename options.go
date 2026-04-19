package storage

import (
	"crypto/sha256"
	"io"
	"iter"
	"os"
	"slices"
)

type GetOption func(*GetOptions)

type GetOptions struct {
	start     int64
	end       int64
	byteRange bool
}

func NewGetOptions(options ...GetOption) *GetOptions {
	return NewOptions(slices.Values(options))
}

func (get *GetOptions) BytesRange() (start, end int64, ok bool) {
	return get.start, get.end, get.byteRange
}

func BytesRange(start, end int64) GetOption {
	return func(options *GetOptions) {
		options.start = start
		options.end = end
		options.byteRange = true
	}
}

type PutOption func(*PutOptions)

type PutOptions struct {
	cacheControl    string
	checksumSHA256  [sha256.Size]byte
	contentEncoding string
	contentLength   *int64
	contentType     string
	ifMatch         string
	ifNoneMatch     string
	metadata        map[string]string
}

func NewPutOptions(options ...PutOption) *PutOptions {
	return NewOptions(slices.Values(options))
}

func (put *PutOptions) CacheControl() string { return put.cacheControl }

// ChecksumSHA256 returns the configured SHA-256 the body must hash to,
// and a bool indicating whether one was set. The all-zero value is the
// "unset" sentinel — collision with a real SHA-256 is 1 in 2^256.
func (put *PutOptions) ChecksumSHA256() ([sha256.Size]byte, bool) {
	var zero [sha256.Size]byte
	return put.checksumSHA256, put.checksumSHA256 != zero
}

// ContentLength returns the configured value, else tries to figure out the content length of the io.Reader.
// If it cannot figure out the length, then returns -1.
//
// The following interfaces are probed in order of precedence:
//
//  1. ContentLength() int64 — explicit 64-bit length (takes precedence over Len)
//  2. Len() int             — implemented by bytes.Buffer, bytes.Reader, strings.Reader
//  3. *os.File              — uses Stat to obtain the file size
//  4. io.Seeker             — seeks to end and back to measure the remaining bytes
func (put *PutOptions) ContentLength(r io.Reader) (int64, error) {
	if put.contentLength != nil {
		return *put.contentLength, nil
	}

	switch r := r.(type) {
	case interface{ ContentLength() int64 }:
		// Takes precedence over Len() int to avoid truncation for sizes > 2 GiB.
		return r.ContentLength(), nil

	case interface{ Len() int }:
		// Implemented by bytes.Buffer, bytes.Reader, strings.Reader
		return int64(r.Len()), nil

	case *os.File:
		if fi, err := r.Stat(); err != nil {
			return 0, err
		} else {
			return fi.Size(), nil
		}

	case io.Seeker:
		offset, err := r.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, err
		}
		contentLength, err := r.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, err
		}
		if _, err = r.Seek(offset, io.SeekStart); err != nil {
			return 0, err
		}
		return contentLength, nil

	default:
		return -1, nil
	}
}

func (put *PutOptions) ContentType() string {
	if put.contentType != "" {
		return put.contentType
	}
	return "application/octet-stream"
}

func (put *PutOptions) ContentEncoding() string { return put.contentEncoding }

func (put *PutOptions) IfMatch() string { return put.ifMatch }

func (put *PutOptions) IfNoneMatch() string { return put.ifNoneMatch }

func (put *PutOptions) Metadata() map[string]string { return put.metadata }

func CacheControl(cc string) PutOption {
	return func(put *PutOptions) { put.cacheControl = cc }
}

// ChecksumSHA256 declares the SHA-256 the body must hash to. Backends
// either let their object store verify natively (S3, S3-compatible HTTP)
// or stream-verify on the client (memory, GCS, file). On mismatch they
// return an error wrapping [ErrChecksumMismatch] and do not durably
// store the body.
func ChecksumSHA256(sum [sha256.Size]byte) PutOption {
	return func(put *PutOptions) { put.checksumSHA256 = sum }
}

func ContentLength(length int64) PutOption {
	return func(put *PutOptions) { put.contentLength = &length }
}

func ContentType(ct string) PutOption {
	return func(put *PutOptions) { put.contentType = ct }
}

func ContentEncoding(ce string) PutOption {
	return func(put *PutOptions) { put.contentEncoding = ce }
}

func IfMatch(etag string) PutOption {
	return func(put *PutOptions) {
		if put.ifNoneMatch != "" {
			panic("cannot set both If-Match and If-None-Match")
		}
		put.ifMatch = etag
	}
}

func IfNoneMatch(etag string) PutOption {
	return func(put *PutOptions) {
		if put.ifMatch != "" {
			panic("cannot set both If-Match and If-None-Match")
		}
		put.ifNoneMatch = etag
	}
}

func Metadata(key, value string) PutOption {
	return func(put *PutOptions) {
		if put.metadata == nil {
			put.metadata = map[string]string{}
		}
		put.metadata[key] = value
	}
}

type ListOption func(*ListOptions)

type ListOptions struct {
	keyDelimiter string
	keyPrefix    string
	startAfter   string
	maxKeys      int
}

func NewListOptions(options ...ListOption) *ListOptions {
	return NewOptions(slices.Values(options))
}

func (list *ListOptions) KeyDelimiter() string { return list.keyDelimiter }

func (list *ListOptions) KeyPrefix() string { return list.keyPrefix }

func (list *ListOptions) StartAfter() string { return list.startAfter }

func (list *ListOptions) MaxKeys() int { return list.maxKeys }

func KeyDelimiter(delimiter string) ListOption {
	return func(list *ListOptions) { list.keyDelimiter = delimiter }
}

func KeyPrefix(prefix string) ListOption {
	return func(list *ListOptions) { list.keyPrefix = prefix }
}

func StartAfter(key string) ListOption {
	return func(list *ListOptions) { list.startAfter = key }
}

func MaxKeys(n int) ListOption {
	return func(list *ListOptions) { list.maxKeys = n }
}

func NewOptions[Options any, Option ~func(*Options)](options iter.Seq[Option]) *Options {
	newOptions := new(Options)
	for option := range options {
		option(newOptions)
	}
	return newOptions
}
