package gs

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/firetiger-oss/tigerblock/storage"
)

// TestSignedGetOptionsDoesNotSignAcceptEncoding ensures the signed GET
// options do NOT commit the client to sending Accept-Encoding: gzip —
// that would break browsers, curl, and proxies that don't send the
// header or send a different value (signature verification fails on
// header mismatch). Presigned GETs therefore go through GCS's default
// decompressive transcoding for gzip-stored objects; opting out is
// only available on the direct GetObject path.
func TestSignedGetOptionsDoesNotSignAcceptEncoding(t *testing.T) {
	opts, err := signedGetOptions("key", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range opts.Headers {
		if strings.HasPrefix(strings.ToLower(h), "accept-encoding:") {
			t.Errorf("opts.Headers includes %q; presigned URLs must not require Accept-Encoding", h)
		}
	}
}

// TestSignedGetOptionsIncludesRange makes sure the Range header is
// signed when a BytesRange option is supplied, so presigned ranged
// GETs work as expected.
func TestSignedGetOptionsIncludesRange(t *testing.T) {
	opts, err := signedGetOptions("key", time.Hour, storage.BytesRange(100, -1))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(opts.Headers, func(h string) bool { return strings.EqualFold(h, "Range:bytes=100-") }) {
		t.Errorf("opts.Headers = %v; want to include Range:bytes=100-", opts.Headers)
	}
}
