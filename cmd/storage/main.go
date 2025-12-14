package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/storage/uri"

	// Import storage backends for side effects
	_ "github.com/firetiger-oss/storage/file"
	_ "github.com/firetiger-oss/storage/gs"
	_ "github.com/firetiger-oss/storage/http"
	_ "github.com/firetiger-oss/storage/memory"
	_ "github.com/firetiger-oss/storage/s3"
)

var rootCmd = &cobra.Command{
	Use:   "storage",
	Short: "Storage CLI for interacting with object storage",
	Long:  "A command line interface for listing, copying, and managing objects across storage backends (S3, GCS, file, memory, HTTP).",
}

func init() {
	rootCmd.AddCommand(lsCmd)
	rootCmd.AddCommand(statCmd)
	rootCmd.AddCommand(cpCmd)
	rootCmd.AddCommand(rmCmd)
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	rootCmd.SetOut(os.Stdout)
	rootCmd.SetErr(os.Stderr)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		os.Exit(1)
	}
}

// btoa converts byte sizes to human-readable format with units
func btoa(v int64) string {
	const unit = 1024
	if v < unit {
		return strconv.FormatInt(v, 10) + " B"
	}

	div, exp := int64(unit), 0
	for n := v / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	units := []string{"KiB", "MiB", "GiB", "TiB", "PiB"}
	return fmt.Sprintf("%.1f %s", float64(v)/float64(div), units[exp])
}

// isPrefix returns true if the URI represents a prefix (ends with /)
func isPrefix(objectURI string) bool {
	_, _, path := uri.Split(objectURI)
	return strings.HasSuffix(path, "/") || path == ""
}

// basename returns the last path component of a URI
func basename(objectURI string) string {
	_, _, path := uri.Split(objectURI)
	if i := strings.LastIndex(path, "/"); i >= 0 {
		return path[i+1:]
	}
	return path
}

// outputJSON writes v as JSON to the command's output
func outputJSON(cmd *cobra.Command, v any) error {
	enc := json.NewEncoder(cmd.OutOrStdout())
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
