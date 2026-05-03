package uri

import (
	"fmt"
	"iter"
	"os"
	"path/filepath"
	"strings"
)

// Clean normalizes the path by removing redundant slashes.
//
// This function differs from the standard library's path.Clean because it does
// not remove "." and ".." elements.
func Clean(path string) string {
	return join(func(yield func(string) bool) {
		hasTrailingSlash := strings.HasSuffix(path, "/")
		for elem := range walk(path) {
			if !yield(elem) {
				return
			}
		}
		if hasTrailingSlash {
			yield("")
		}
	})
}

// isLocalFilePath returns true if s looks like a local file path.
func isLocalFilePath(s string) bool {
	if s == "" {
		return false
	}
	// Absolute Unix paths
	if s[0] == '/' {
		return true
	}
	// Relative paths: ./ or ../
	if strings.HasPrefix(s, "./") || strings.HasPrefix(s, "../") || s == "." || s == ".." {
		return true
	}
	// Home directory
	if s[0] == '~' {
		return true
	}
	return false
}

// expandFilePath expands a local file path to an absolute path.
// It handles ~ for home directory and resolves relative paths.
func expandFilePath(s string) string {
	// Preserve trailing slash (also handle . and .. as directory indicators)
	base := filepath.Base(filepath.FromSlash(s))
	hasTrailingSlash := strings.HasSuffix(s, "/") || base == "." || base == ".."

	// Convert from URI slash format to native path for os operations
	nativePath := filepath.FromSlash(s)

	// Expand ~ to home directory
	if strings.HasPrefix(s, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			nativePath = filepath.Join(home, filepath.FromSlash(s[2:]))
		}
	} else if s == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			nativePath = home
		}
	}

	// Make relative paths absolute (resolve . and ..)
	if !filepath.IsAbs(nativePath) {
		if cwd, err := os.Getwd(); err == nil {
			nativePath = filepath.Join(cwd, nativePath)
		}
	}

	// Clean the path and convert back to URI slash format
	result := filepath.ToSlash(filepath.Clean(nativePath))

	// Restore trailing slash if originally present
	if hasTrailingSlash && !strings.HasSuffix(result, "/") {
		result += "/"
	}

	return result
}

// Split splits a URI into its components: scheme, location, and path.
//
// The path is always cleaned and exposed as relative to the location, even for
// file URIs where the path is represented with a leading slash.
//
// For file:// URIs, the location is always empty and the path contains the
// full path after the scheme with the leading slash trimmed.
//
// Local file paths (starting with /, ./, ../, or ~) are automatically detected
// and treated as file:// URIs with the path expanded to an absolute path.
func Split(uri string) (scheme, location, path string) {
	if len(uri) == 0 {
		return
	}
	if strings.HasPrefix(uri, ":memory:") {
		location, path = uri[:8], uri[8:]
	} else if i := strings.Index(uri, "://"); i >= 0 {
		scheme, uri = uri[:i], uri[i+3:]
		if scheme == "file" {
			// For file:// URIs, location is always empty and path is the full path
			// after the scheme with leading slash trimmed
			path = uri
		} else {
			location, path, _ = strings.Cut(uri, "/")
		}
	} else if isLocalFilePath(uri) {
		scheme = "file"
		path = expandFilePath(uri)
	} else {
		path = uri
	}
	return scheme, location, Clean(path)
}

// Join joins the scheme, location, and path into a URI.
//
// Note: for file URIs, the path is always expressed as an absolute reference.
func Join(scheme, location string, path ...string) string {
	var uri string

	if len(path) != 0 {
		var b strings.Builder
		for _, key := range path {
			for elem := range walk(key) {
				b.WriteByte('/')
				b.WriteString(elem)
			}
		}
		if strings.HasSuffix(path[len(path)-1], "/") {
			b.WriteByte('/')
		}
		uri = trimLeadingSlashes(b.String())
	}

	uri = join2(location, uri)
	switch scheme {
	case "":
	case "file":
		uri = "file:///" + uri
	default:
		uri = scheme + "://" + uri
	}
	return uri
}

// SplitPathStyle parses a path-style URI of the form
// `scheme://host/bucket/key`. The first path segment after the host
// is the bucket name; everything after is the object key (which may
// contain slashes and may be empty). All segments are returned
// verbatim — Clean is not applied.
//
// Returns an error if the input has no scheme, no `://`, or no host.
// Trailing slashes on the bucket portion are not preserved when the
// key is empty.
//
// Use this for URIs that unambiguously denote path-style multi-bucket
// addressing — it is intentionally separate from Split so that the
// general-purpose URI parser stays scheme-agnostic.
func SplitPathStyle(s string) (scheme, host, bucket, key string, err error) {
	sep := strings.Index(s, "://")
	if sep < 0 {
		return "", "", "", "", fmt.Errorf("uri: missing scheme in path-style URI %q", s)
	}
	scheme = s[:sep]
	if scheme == "" {
		return "", "", "", "", fmt.Errorf("uri: empty scheme in path-style URI %q", s)
	}
	rest := s[sep+3:]
	host, pathPart, hasPath := strings.Cut(rest, "/")
	if host == "" {
		return "", "", "", "", fmt.Errorf("uri: missing host in path-style URI %q", s)
	}
	if !hasPath {
		return scheme, host, "", "", nil
	}
	bucket, key, _ = strings.Cut(pathPart, "/")
	return scheme, host, bucket, key, nil
}

// JoinPathStyle constructs a path-style URI of the form
// `scheme://host/bucket/key`. Empty trailing segments are omitted, so
// `JoinPathStyle("http","host","","") == "http://host"` and
// `JoinPathStyle("http","host","b","") == "http://host/b"`.
func JoinPathStyle(scheme, host, bucket, key string) string {
	var b strings.Builder
	b.WriteString(scheme)
	b.WriteString("://")
	b.WriteString(host)
	switch {
	case bucket != "" && key != "":
		b.WriteByte('/')
		b.WriteString(bucket)
		b.WriteByte('/')
		b.WriteString(key)
	case bucket != "":
		b.WriteByte('/')
		b.WriteString(bucket)
	case key != "":
		b.WriteByte('/')
		b.WriteString(key)
	}
	return b.String()
}

func join(seq iter.Seq[string]) string {
	var b strings.Builder
	for elem := range seq {
		b.WriteByte('/')
		b.WriteString(elem)
	}
	return trimLeadingSlashes(b.String())
}

func join2(base, name string) string {
	switch {
	case base == "":
		return name
	case name == "":
		return base
	case strings.HasPrefix(base, ":"):
		return base + name
	default:
		return base + "/" + name
	}
}

func walk(key string) iter.Seq[string] {
	return func(yield func(string) bool) {
		for {
			if key = trimLeadingSlashes(key); key == "" {
				break
			}
			var elem string
			elem, key, _ = strings.Cut(key, "/")
			if !yield(elem) {
				break
			}
		}
	}
}

func trimLeadingSlashes(s string) string {
	for len(s) > 0 && s[0] == '/' {
		s = s[1:]
	}
	return s
}
