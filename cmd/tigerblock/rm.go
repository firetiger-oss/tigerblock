package main

import (
	"errors"
	"fmt"
	"iter"

	"github.com/spf13/cobra"

	"github.com/firetiger-oss/tigerblock/storage"
)

var rmCmd = &cobra.Command{
	Use:   "rm [uris...]",
	Short: "Delete objects at URIs",
	Long:  "Delete one or more objects at the specified URIs. Use -r for recursive deletion of prefixes.",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runRm,
}

func init() {
	rmCmd.Flags().BoolP("recursive", "r", false, "Delete recursively (required for prefixes)")
	rmCmd.Flags().BoolP("force", "f", false, "Ignore non-existent objects")
	rmCmd.Flags().StringP("output", "o", "text", "Output format: text or json")
}

type rmResult struct {
	URI     string `json:"uri"`
	Deleted bool   `json:"deleted"`
	Error   string `json:"error,omitempty"`
}

func runRm(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	cmd.SilenceUsage = true

	recursive, _ := cmd.Flags().GetBool("recursive")
	force, _ := cmd.Flags().GetBool("force")
	output, _ := cmd.Flags().GetString("output")

	var results []rmResult

	for _, objectURI := range args {
		if isPrefix(objectURI) {
			if !recursive {
				return fmt.Errorf("%s is a prefix, use -r for recursive deletion", objectURI)
			}
			deleteResults, err := deleteRecursive(cmd, objectURI, force, output)
			if err != nil {
				return err
			}
			results = append(results, deleteResults...)
		} else {
			normalizedURI := normalizeURI(objectURI)
			result := rmResult{URI: normalizedURI, Deleted: true}
			if err := storage.DeleteObject(ctx, objectURI); err != nil {
				if force && errors.Is(err, storage.ErrObjectNotFound) {
					result.Deleted = false
				} else {
					return fmt.Errorf("delete %s: %w", objectURI, err)
				}
			}
			if output == "text" && result.Deleted {
				cmd.Println(normalizedURI)
			}
			results = append(results, result)
		}
	}

	if output == "json" {
		return outputJSON(cmd, results)
	}

	return nil
}

func deleteRecursive(cmd *cobra.Command, prefix string, force bool, output string) ([]rmResult, error) {
	ctx := cmd.Context()
	var results []rmResult

	// Create iterator from ListObjects
	objects := func(yield func(string, error) bool) {
		for object, err := range storage.ListObjects(ctx, prefix) {
			if err != nil {
				yield("", err)
				return
			}
			if !yield(object.Key, nil) {
				return
			}
		}
	}

	// Delete using batch API
	for key, err := range storage.DeleteObjects(ctx, iter.Seq2[string, error](objects)) {
		normalizedKey := normalizeURI(key)
		result := rmResult{URI: normalizedKey, Deleted: true}
		if err != nil {
			if force && errors.Is(err, storage.ErrObjectNotFound) {
				result.Deleted = false
			} else {
				return results, fmt.Errorf("delete %s: %w", key, err)
			}
		}
		if output == "text" && result.Deleted {
			cmd.Println(normalizedKey)
		}
		results = append(results, result)
	}

	return results, nil
}
