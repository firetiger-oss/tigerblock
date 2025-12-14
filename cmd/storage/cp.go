package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/storage"
	"github.com/firetiger-oss/storage/uri"
)

var cpCmd = &cobra.Command{
	Use:   "cp [source] [target]",
	Short: "Copy objects between storage locations",
	Long: `Copy objects between storage locations. Supports S3, GCS, file, memory, and HTTP backends.

Use "-" as source to read from stdin, or "-" as target to write to stdout.
Use -r for recursive copying of prefixes.`,
	Args: cobra.ExactArgs(2),
	RunE: runCp,
}

func init() {
	cpCmd.Flags().BoolP("recursive", "r", false, "Copy recursively (required for prefixes)")
	cpCmd.Flags().StringP("delimiter", "d", "", "Use delimiter for pseudo-directory listing (e.g., \"/\")")
	cpCmd.Flags().String("if-match", "", "Only copy if target ETag matches")
	cpCmd.Flags().String("if-none-match", "", "Only copy if target ETag does not match")
	cpCmd.Flags().String("content-type", "", "Override content type")
	cpCmd.Flags().String("cache-control", "", "Set cache control header")
	cpCmd.Flags().String("content-encoding", "", "Set content encoding")
	cpCmd.Flags().StringP("output", "o", "text", "Output format: text or json")
}

type cpOptions struct {
	ifMatch         string
	ifNoneMatch     string
	contentType     string
	cacheControl    string
	contentEncoding string
}

type cpResult struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Size   int64  `json:"size"`
}

func runCp(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cmd.SilenceUsage = true

	source := args[0]
	target := args[1]

	recursive, _ := cmd.Flags().GetBool("recursive")
	delimiter, _ := cmd.Flags().GetString("delimiter")
	output, _ := cmd.Flags().GetString("output")

	opts := cpOptions{
		ifMatch:         mustGetString(cmd, "if-match"),
		ifNoneMatch:     mustGetString(cmd, "if-none-match"),
		contentType:     mustGetString(cmd, "content-type"),
		cacheControl:    mustGetString(cmd, "cache-control"),
		contentEncoding: mustGetString(cmd, "content-encoding"),
	}

	var results []cpResult

	// Handle stdin
	if source == "-" {
		info, err := storage.PutObject(ctx, target, os.Stdin, buildPutOptions(opts)...)
		if err != nil {
			return fmt.Errorf("copy to %s: %w", target, err)
		}
		result := cpResult{Source: "-", Target: target, Size: info.Size}
		if output == "text" {
			cmd.Printf("copied %s -> %s (%s)\n", source, target, btoa(info.Size))
		} else {
			return outputJSON(cmd, []cpResult{result})
		}
		return nil
	}

	// Handle stdout
	if target == "-" {
		reader, _, err := storage.GetObject(ctx, source)
		if err != nil {
			return fmt.Errorf("copy from %s: %w", source, err)
		}
		defer reader.Close()
		if _, err := io.Copy(os.Stdout, reader); err != nil {
			return fmt.Errorf("copy to stdout: %w", err)
		}
		return nil
	}

	// Handle prefix copy (recursive)
	if isPrefix(source) {
		if !recursive {
			return fmt.Errorf("%s is a prefix, use -r for recursive copy", source)
		}

		var listOptions []storage.ListOption
		if delimiter != "" {
			listOptions = append(listOptions, storage.KeyDelimiter(delimiter))
		}

		_, _, sourcePath := uri.Split(source)
		sourcePrefix := strings.TrimSuffix(sourcePath, "/")

		for object, err := range storage.ListObjects(ctx, source, listOptions...) {
			if err != nil {
				return fmt.Errorf("listing %s: %w", source, err)
			}

			// Compute relative path from source prefix
			relPath := strings.TrimPrefix(object.Key, sourcePrefix)
			relPath = strings.TrimPrefix(relPath, "/")

			// Build full source and target URIs
			sourceScheme, sourceLocation, _ := uri.Split(source)
			fullSourceURI := uri.Join(sourceScheme, sourceLocation, object.Key)

			targetURI := buildTargetURI(target, relPath)

			result, err := copyFile(cmd, fullSourceURI, targetURI, opts)
			if err != nil {
				return err
			}

			if output == "text" {
				cmd.Printf("copied %s -> %s (%s)\n", fullSourceURI, targetURI, btoa(result.Size))
			}
			results = append(results, result)
		}

		if output == "json" {
			return outputJSON(cmd, results)
		}
		return nil
	}

	// Single file copy
	targetURI := target
	if isPrefix(target) {
		// Target is a prefix, append source basename
		targetURI = buildTargetURI(target, basename(source))
	}

	result, err := copyFile(cmd, source, targetURI, opts)
	if err != nil {
		return err
	}

	if output == "text" {
		cmd.Printf("copied %s -> %s (%s)\n", source, targetURI, btoa(result.Size))
	} else {
		return outputJSON(cmd, []cpResult{result})
	}

	return nil
}

func copyFile(cmd *cobra.Command, source, target string, opts cpOptions) (cpResult, error) {
	ctx := cmd.Context()

	// Get source
	reader, info, err := storage.GetObject(ctx, source)
	if err != nil {
		return cpResult{}, fmt.Errorf("read %s: %w", source, err)
	}
	defer reader.Close()

	// Create temp file as intermediary
	tmpFile, err := os.CreateTemp("", "storage-cp-*")
	if err != nil {
		return cpResult{}, fmt.Errorf("create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	// Copy to temp file
	if _, err := io.Copy(tmpFile, reader); err != nil {
		return cpResult{}, fmt.Errorf("copy to temp: %w", err)
	}

	// Seek back to start
	if _, err := tmpFile.Seek(0, io.SeekStart); err != nil {
		return cpResult{}, fmt.Errorf("seek temp file: %w", err)
	}

	// Build PutOptions
	putOpts := buildPutOptions(opts)

	// Inherit content-type from source if not overridden
	if opts.contentType == "" && info.ContentType != "" {
		putOpts = append(putOpts, storage.ContentType(info.ContentType))
	}

	// Put to target
	resultInfo, err := storage.PutObject(ctx, target, tmpFile, putOpts...)
	if err != nil {
		return cpResult{}, fmt.Errorf("write %s: %w", target, err)
	}

	return cpResult{Source: source, Target: target, Size: resultInfo.Size}, nil
}

func buildPutOptions(opts cpOptions) []storage.PutOption {
	var putOpts []storage.PutOption

	if opts.ifMatch != "" {
		putOpts = append(putOpts, storage.IfMatch(opts.ifMatch))
	}
	if opts.ifNoneMatch != "" {
		putOpts = append(putOpts, storage.IfNoneMatch(opts.ifNoneMatch))
	}
	if opts.contentType != "" {
		putOpts = append(putOpts, storage.ContentType(opts.contentType))
	}
	if opts.cacheControl != "" {
		putOpts = append(putOpts, storage.CacheControl(opts.cacheControl))
	}
	if opts.contentEncoding != "" {
		putOpts = append(putOpts, storage.ContentEncoding(opts.contentEncoding))
	}

	return putOpts
}

func buildTargetURI(targetPrefix, relPath string) string {
	scheme, location, path := uri.Split(targetPrefix)
	return uri.Join(scheme, location, path, relPath)
}

func mustGetString(cmd *cobra.Command, name string) string {
	val, _ := cmd.Flags().GetString(name)
	return val
}
