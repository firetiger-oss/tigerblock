package storage

import (
	"context"
	"iter"
	"slices"
	"strings"
	"time"

	"github.com/firetiger-oss/tigerblock/storage/backoff"
)

// WatchObjects provides a generic implementation of watching for changes to objects
// in a bucket using ListObjects with exponential backoff. It tracks object state
// and yields objects that have been added, modified, or deleted.
//
// Objects that have been deleted are yielded with a Size of -1 as a deletion marker.
// The function uses exponential backoff to poll for changes, resetting the backoff
// delay whenever changes are detected.
//
// This implementation is similar to storage/http.Bucket.WatchObjects and can be used
// by bucket implementations that don't have native watch capabilities.
//
// Example usage:
//
//	for object, err := range storage.WatchObjects(ctx, bucket, storage.KeyPrefix("logs/")) {
//		if err != nil {
//			log.Printf("watch error: %v", err)
//			continue
//		}
//		if object.Size == -1 {
//			log.Printf("deleted: %s", object.Key)
//		} else {
//			log.Printf("added/updated: %s (size: %d)", object.Key, object.Size)
//		}
//	}
func WatchObjects(ctx context.Context, bucket Bucket, options ...ListOption) iter.Seq2[Object, error] {
	return func(yield func(Object, error) bool) {
		type listedObject struct {
			object  Object
			version int
		}

		currentObjects := make(map[string]listedObject)
		currentVersion := 0

		for {
		backoffLoop:
			for _, err := range backoff.Seq(ctx) {
				if err != nil { // context canceled
					return
				}

				var changeCount int
				for object, err := range bucket.ListObjects(ctx, options...) {
					if err != nil {
						if !yield(Object{}, err) {
							return
						}
						continue backoffLoop
					}

					// TODO: this is not resilient to consecutive updates that
					// happened below the granularity of the LastModified time.
					//
					// We could consider adding the ETag to storage.Object (tho
					// the file storage backend does not support it), or make
					// HeadObject calls to retrieve the ETag in order to check
					// for changes, or maybe introduce a different API to do
					// long polling on the storage bucket server.
					current, exists := currentObjects[object.Key]
					if !exists ||
						object.Size != current.object.Size ||
						object.LastModified.After(current.object.LastModified) {
						if !yield(object, nil) {
							return
						}
						changeCount++
					}

					currentObjects[object.Key] = listedObject{
						object:  object,
						version: currentVersion,
					}
				}

				var deletedObjects []string
				for key, object := range currentObjects {
					if object.version < currentVersion {
						deletedObjects = append(deletedObjects, key)
					}
				}

				if len(deletedObjects) > 0 {
					deletionTime := time.Now()

					slices.SortFunc(deletedObjects, func(a, b string) int {
						return -strings.Compare(a, b)
					})

					for _, key := range deletedObjects {
						if !yield(Object{
							Key:          key,
							Size:         -1, // deletion marker
							LastModified: deletionTime,
						}, nil) {
							return
						}
						changeCount++
					}

					// Clean up deleted objects from our tracking map
					for _, key := range deletedObjects {
						delete(currentObjects, key)
					}
				}

				currentVersion++
				if changeCount > 0 {
					break // continue to outer loop to reset backoff delay
				}
			}
		}
	}
}
