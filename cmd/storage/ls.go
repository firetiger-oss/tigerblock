package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/storage"
)

var lsCmd = &cobra.Command{
	Use:   "ls [uris...]",
	Short: "List objects at bucket URIs",
	Long:  "List objects at one or more bucket URIs. Supports S3, GCS, file, memory, and HTTP backends.",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runLs,
}

func init() {
	lsCmd.Flags().BoolP("long", "l", false, "Use long listing format (size, date, key)")
	lsCmd.Flags().BoolP("recursive", "r", false, "List recursively")
	lsCmd.Flags().StringP("delimiter", "d", "/", "Delimiter for directory listing")
	lsCmd.Flags().StringP("start-after", "s", "", "Start listing after this key")
	lsCmd.Flags().IntP("limit", "n", 1000, "Maximum number of objects to list (0 for unlimited)")
	lsCmd.Flags().StringP("output", "o", "text", "Output format: text or json")
}

type lsObject struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last-modified,omitempty"`
}

func runLs(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cmd.SilenceUsage = true

	longFormat, _ := cmd.Flags().GetBool("long")
	recursive, _ := cmd.Flags().GetBool("recursive")
	delimiter, _ := cmd.Flags().GetString("delimiter")
	startAfter, _ := cmd.Flags().GetString("start-after")
	limit, _ := cmd.Flags().GetInt("limit")
	output, _ := cmd.Flags().GetString("output")

	var listOptions []storage.ListOption
	// Use delimiter for non-recursive listing (default behavior)
	if !recursive && delimiter != "" {
		listOptions = append(listOptions, storage.KeyDelimiter(delimiter))
	}
	if startAfter != "" {
		listOptions = append(listOptions, storage.StartAfter(startAfter))
	}
	if limit > 0 {
		listOptions = append(listOptions, storage.MaxKeys(limit))
	}

	var objects []lsObject

	for _, objectURI := range args {
		for object, err := range storage.ListObjects(ctx, objectURI, listOptions...) {
			if err != nil {
				return fmt.Errorf("listing %s: %w", objectURI, err)
			}

			obj := lsObject{
				Key:          object.Key,
				Size:         object.Size,
				LastModified: object.LastModified,
			}

			if output == "json" {
				objects = append(objects, obj)
			} else {
				if longFormat {
					sizeStr := btoa(object.Size)
					dateStr := object.LastModified.Format("Jan _2, 2006 15:04")
					cmd.Printf("%10s  %s  %s\n", sizeStr, dateStr, object.Key)
				} else {
					cmd.Println(object.Key)
				}
			}
		}
	}

	if output == "json" {
		return outputJSON(cmd, objects)
	}

	return nil
}
