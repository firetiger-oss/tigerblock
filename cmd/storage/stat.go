package main

import (
	"fmt"
	"sort"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/storage"
)

var statCmd = &cobra.Command{
	Use:   "stat [uris...]",
	Short: "Show detailed object information",
	Long:  "Display detailed metadata for one or more objects including size, ETag, content type, and custom metadata.",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runStat,
}

func init() {
	statCmd.Flags().StringP("output", "o", "text", "Output format: text or json")
}

type statObject struct {
	URI             string            `json:"uri"`
	Size            int64             `json:"size"`
	ETag            string            `json:"etag,omitempty"`
	ContentType     string            `json:"content-type,omitempty"`
	ContentEncoding string            `json:"content-encoding,omitempty"`
	CacheControl    string            `json:"cache-control,omitempty"`
	LastModified    time.Time         `json:"last-modified,omitempty"`
	Metadata        map[string]string `json:"metadata,omitempty"`
}

func runStat(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cmd.SilenceUsage = true

	output, _ := cmd.Flags().GetString("output")

	var objects []statObject

	for _, objectURI := range args {
		info, err := storage.HeadObject(ctx, objectURI)
		if err != nil {
			return fmt.Errorf("stat %s: %w", objectURI, err)
		}

		obj := statObject{
			URI:             objectURI,
			Size:            info.Size,
			ETag:            info.ETag,
			ContentType:     info.ContentType,
			ContentEncoding: info.ContentEncoding,
			CacheControl:    info.CacheControl,
			LastModified:    info.LastModified,
			Metadata:        info.Metadata,
		}

		if output == "json" {
			objects = append(objects, obj)
		} else {
			printStatText(cmd, objectURI, info)
			if len(args) > 1 {
				cmd.Println()
			}
		}
	}

	if output == "json" {
		return outputJSON(cmd, objects)
	}

	return nil
}

func printStatText(cmd *cobra.Command, objectURI string, info storage.ObjectInfo) {
	w := tabwriter.NewWriter(cmd.OutOrStdout(), 0, 0, 2, ' ', 0)

	fmt.Fprintf(w, "URI:\t%s\n", objectURI)
	fmt.Fprintf(w, "Size:\t%s (%d bytes)\n", btoa(info.Size), info.Size)

	if info.ETag != "" {
		fmt.Fprintf(w, "ETag:\t%s\n", info.ETag)
	}
	if info.ContentType != "" {
		fmt.Fprintf(w, "Content-Type:\t%s\n", info.ContentType)
	}
	if info.ContentEncoding != "" {
		fmt.Fprintf(w, "Content-Encoding:\t%s\n", info.ContentEncoding)
	}
	if info.CacheControl != "" {
		fmt.Fprintf(w, "Cache-Control:\t%s\n", info.CacheControl)
	}
	if !info.LastModified.IsZero() {
		fmt.Fprintf(w, "Last-Modified:\t%s\n", info.LastModified.Format("Jan 2, 2006 at 3:04:05 PM MST"))
	}

	if len(info.Metadata) > 0 {
		fmt.Fprintf(w, "Metadata:\n")
		// Sort metadata keys for consistent output
		keys := make([]string, 0, len(info.Metadata))
		for k := range info.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Fprintf(w, "  %s:\t%s\n", k, info.Metadata[k])
		}
	}

	w.Flush()
}
