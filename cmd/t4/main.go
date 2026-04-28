package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/tigerblock/uri"

	// Import storage backends for side effects
	_ "github.com/firetiger-oss/tigerblock/storage/file"
	_ "github.com/firetiger-oss/tigerblock/storage/gs"
	_ "github.com/firetiger-oss/tigerblock/storage/http"
	_ "github.com/firetiger-oss/tigerblock/storage/memory"
	_ "github.com/firetiger-oss/tigerblock/storage/s3"

	basicauth "github.com/firetiger-oss/tigerblock/secret/authn/basic"
	bearerauth "github.com/firetiger-oss/tigerblock/secret/authn/bearer"
)

var rootCmd = &cobra.Command{
	Use:   "t4",
	Short: "tigerblock CLI for interacting with object storage",
	Long:  "A command line interface for listing, copying, and managing objects across storage backends (S3, GCS, file, memory, HTTP).",
}

func init() {
	rootCmd.AddCommand(lsCmd)
	rootCmd.AddCommand(statCmd)
	rootCmd.AddCommand(cpCmd)
	rootCmd.AddCommand(rmCmd)
	rootCmd.AddCommand(serveCmd)

	rootCmd.PersistentFlags().String("basic-auth", "", "HTTP basic auth credentials in username:password format")
	rootCmd.PersistentFlags().String("bearer-token", "", "HTTP bearer token for authentication")

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		basicAuth, _ := cmd.Flags().GetString("basic-auth")
		bearerToken, _ := cmd.Flags().GetString("bearer-token")
		if basicAuth != "" {
			username, password, _ := strings.Cut(basicAuth, ":")
			http.DefaultTransport = basicauth.NewTransport(http.DefaultTransport, username, password)
		} else if bearerToken != "" {
			http.DefaultTransport = bearerauth.NewTransport(http.DefaultTransport, bearerToken)
		}
		return nil
	}
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

// normalizeURI returns the canonical form of a URI
func normalizeURI(u string) string {
	scheme, location, path := uri.Split(u)
	return uri.Join(scheme, location, path)
}
