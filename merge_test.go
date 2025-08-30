package storage

import (
	"testing"
)

func TestMerge(t *testing.T) {
	t.Run("zero buckets returns EmptyBucket", func(t *testing.T) {
		bucket := Merge()
		if bucket.Location() != ":none:" {
			t.Errorf("expected :none:, got %s", bucket.Location())
		}
	})

	t.Run("one bucket returns bucket as-is", func(t *testing.T) {
		empty := EmptyBucket()
		bucket := Merge(empty)
		if bucket != empty {
			t.Error("expected same bucket instance")
		}
	})

	t.Run("multiple buckets returns mergedBucket", func(t *testing.T) {
		empty1 := EmptyBucket()
		empty2 := EmptyBucket()
		bucket := Merge(empty1, empty2)

		// Should be a mergedBucket, not the original bucket
		if bucket == empty1 || bucket == empty2 {
			t.Error("expected different bucket instance")
		}

		// Location should be from first bucket
		if bucket.Location() != empty1.Location() {
			t.Errorf("expected %s, got %s", empty1.Location(), bucket.Location())
		}
	})
}
