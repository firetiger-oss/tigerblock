package storage

import (
	"cmp"
	"context"
	"errors"
	"io"
	"iter"
	"slices"

	"github.com/achille-roussel/kway-go"
	"github.com/firetiger-oss/storage/concurrent"
)

// Merge creates a bucket that merges multiple buckets according to specific rules:
// - Location returns the location of the first bucket
// - Access/Create iterates over all buckets
// - HeadObject, GetObject iterates over all buckets, returns the first that doesn't return ErrObjectNotFound
// - PutObject puts the object in the first bucket
// - DeleteObject/DeleteObjects iterates over all buckets
// - ListObjects uses kway-go to combine the lists of all buckets
// - WatchObjects uses kway-go to combine WatchObjects on first bucket with ListObjects on others
// - Presign* methods delegate to the first bucket
func Merge(buckets ...Bucket) Bucket {
	switch len(buckets) {
	case 0:
		return EmptyBucket()
	case 1:
		return buckets[0]
	default:
		return &mergedBucket{buckets: slices.Clone(buckets)}
	}
}

type mergedBucket struct {
	buckets []Bucket
}

func (m *mergedBucket) Location() string {
	return m.buckets[0].Location()
}

func (m *mergedBucket) Access(ctx context.Context) error {
	return concurrent.RunTasks(ctx, m.buckets, func(ctx context.Context, bucket Bucket) error {
		return bucket.Access(ctx)
	})
}

func (m *mergedBucket) Create(ctx context.Context) error {
	return concurrent.RunTasks(ctx, m.buckets, func(ctx context.Context, bucket Bucket) error {
		return bucket.Create(ctx)
	})
}

func (m *mergedBucket) HeadObject(ctx context.Context, key string) (ObjectInfo, error) {
	for _, bucket := range m.buckets {
		info, err := bucket.HeadObject(ctx, key)
		if err != nil && !errors.Is(err, ErrObjectNotFound) {
			return ObjectInfo{}, err
		}
		if err == nil {
			return info, nil
		}
	}
	return ObjectInfo{}, ErrObjectNotFound
}

func (m *mergedBucket) GetObject(ctx context.Context, key string, options ...GetOption) (io.ReadCloser, ObjectInfo, error) {
	for _, bucket := range m.buckets {
		reader, info, err := bucket.GetObject(ctx, key, options...)
		if err != nil && !errors.Is(err, ErrObjectNotFound) {
			return nil, ObjectInfo{}, err
		}
		if err == nil {
			return reader, info, nil
		}
	}
	return nil, ObjectInfo{}, ErrObjectNotFound
}

func (m *mergedBucket) PutObject(ctx context.Context, key string, value io.Reader, options ...PutOption) (ObjectInfo, error) {
	return m.buckets[0].PutObject(ctx, key, value, options...)
}

func (m *mergedBucket) DeleteObject(ctx context.Context, key string) error {
	return concurrent.RunTasks(ctx, m.buckets, func(ctx context.Context, bucket Bucket) error {
		return bucket.DeleteObject(ctx, key)
	})
}

func (m *mergedBucket) DeleteObjects(ctx context.Context, keys []string) error {
	return concurrent.RunTasks(ctx, m.buckets, func(ctx context.Context, bucket Bucket) error {
		return bucket.DeleteObjects(ctx, keys)
	})
}

func (m *mergedBucket) ListObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	sequences := make([]iter.Seq2[Object, error], len(m.buckets))
	for i, bucket := range m.buckets {
		sequences[i] = bucket.ListObjects(ctx, options...)
	}
	return kway.MergeFunc(compareObjects, sequences...)
}

func (m *mergedBucket) WatchObjects(ctx context.Context, options ...ListOption) iter.Seq2[Object, error] {
	sequences := make([]iter.Seq2[Object, error], len(m.buckets))
	// First bucket uses WatchObjects
	sequences[0] = m.buckets[0].WatchObjects(ctx, options...)
	// Other buckets use ListObjects
	for i := 1; i < len(m.buckets); i++ {
		sequences[i] = m.buckets[i].ListObjects(ctx, options...)
	}
	return kway.MergeFunc(compareObjects, sequences...)
}

func (m *mergedBucket) PresignGetObject(ctx context.Context, key string, options ...GetOption) (string, error) {
	return m.buckets[0].PresignGetObject(ctx, key, options...)
}

func (m *mergedBucket) PresignPutObject(ctx context.Context, key string, options ...PutOption) (string, error) {
	return m.buckets[0].PresignPutObject(ctx, key, options...)
}

func (m *mergedBucket) PresignHeadObject(ctx context.Context, key string) (string, error) {
	return m.buckets[0].PresignHeadObject(ctx, key)
}

func (m *mergedBucket) PresignDeleteObject(ctx context.Context, key string) (string, error) {
	return m.buckets[0].PresignDeleteObject(ctx, key)
}

func compareObjects(a, b Object) int {
	return cmp.Compare(a.Key, b.Key)
}
