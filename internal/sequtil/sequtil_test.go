package sequtil_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/firetiger-oss/storage/internal/sequtil"
)

func TestAll(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			values := []int{1, 2, 3, 4, 5}
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		}

		result, err := sequtil.All(seq)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []int{1, 2, 3, 4, 5}
		if len(result) != len(expected) {
			t.Errorf("expected %d values, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("empty sequence", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			// Empty - no yields
		}

		result, err := sequtil.All(seq)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(result) != 0 {
			t.Errorf("expected empty result, got %d values", len(result))
		}
	})

	t.Run("error handling", func(t *testing.T) {
		expectedErr := errors.New("test error")
		seq := func(yield func(string, error) bool) {
			if !yield("first", nil) {
				return
			}
			if !yield("second", nil) {
				return
			}
			if !yield("", expectedErr) { // Error on third yield
				return
			}
			yield("fourth", nil) // Should not be processed
		}

		result, err := sequtil.All(seq)
		if err == nil {
			t.Error("expected error, got nil")
		} else if err.Error() != expectedErr.Error() {
			t.Errorf("expected error %q, got %q", expectedErr.Error(), err.Error())
		}

		expected := []string{"first", "second"}
		if len(result) != len(expected) {
			t.Errorf("expected %d values before error, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %q, expected %q", i, v, expected[i])
			}
		}
	})

	t.Run("immediate error", func(t *testing.T) {
		expectedErr := errors.New("immediate error")
		seq := func(yield func(int, error) bool) {
			yield(0, expectedErr) // Error on first yield
		}

		result, err := sequtil.All(seq)
		if err == nil {
			t.Error("expected error, got nil")
		} else if err.Error() != expectedErr.Error() {
			t.Errorf("expected error %q, got %q", expectedErr.Error(), err.Error())
		}

		if len(result) != 0 {
			t.Errorf("expected empty result on immediate error, got %d values", len(result))
		}
	})

	t.Run("large sequence", func(t *testing.T) {
		const size = 1000
		seq := func(yield func(int, error) bool) {
			for i := range size {
				if !yield(i, nil) {
					return
				}
			}
		}

		result, err := sequtil.All(seq)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(result) != size {
			t.Errorf("expected %d values, got %d", size, len(result))
		}
		for i, v := range result {
			if v != i {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, i)
			}
		}
	})
}

func TestLimit(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			for i := range 10 {
				if !yield(i, nil) {
					return
				}
			}
		}

		limited := sequtil.Limit(seq, 5)
		result, err := sequtil.All(limited)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []int{0, 1, 2, 3, 4}
		if len(result) != len(expected) {
			t.Errorf("expected %d values, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("zero limit", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			for i := range 10 {
				if !yield(i, nil) {
					return
				}
			}
		}

		limited := sequtil.Limit(seq, 0)
		result, err := sequtil.All(limited)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		if len(result) != len(expected) {
			t.Errorf("expected %d values with zero limit (no limit), got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("negative limit", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			for i := range 10 {
				if !yield(i, nil) {
					return
				}
			}
		}

		limited := sequtil.Limit(seq, -5)
		result, err := sequtil.All(limited)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
		if len(result) != len(expected) {
			t.Errorf("expected %d values with negative limit (no limit), got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("limit larger than sequence", func(t *testing.T) {
		values := []int{1, 2, 3}
		seq := func(yield func(int, error) bool) {
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		}

		limited := sequtil.Limit(seq, 10)
		result, err := sequtil.All(limited)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(result) != len(values) {
			t.Errorf("expected %d values, got %d", len(values), len(result))
		}
		for i, v := range result {
			if i >= len(values) || v != values[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, values[i])
			}
		}
	})

	t.Run("error handling", func(t *testing.T) {
		expectedErr := errors.New("test error")
		seq := func(yield func(string, error) bool) {
			if !yield("first", nil) {
				return
			}
			if !yield("", expectedErr) { // Error on second yield
				return
			}
			yield("third", nil) // Should not be processed
		}

		limited := sequtil.Limit(seq, 5)
		result, err := sequtil.All(limited)
		if err == nil {
			t.Error("expected error, got nil")
		} else if err.Error() != expectedErr.Error() {
			t.Errorf("expected error %q, got %q", expectedErr.Error(), err.Error())
		}

		expected := []string{"first"}
		if len(result) != len(expected) {
			t.Errorf("expected %d values before error, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %q, expected %q", i, v, expected[i])
			}
		}
	})

	t.Run("exact limit match", func(t *testing.T) {
		values := []int{10, 20, 30}
		seq := func(yield func(int, error) bool) {
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		}

		limited := sequtil.Limit(seq, 3)
		result, err := sequtil.All(limited)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(result) != len(values) {
			t.Errorf("expected %d values, got %d", len(values), len(result))
		}
		for i, v := range result {
			if i >= len(values) || v != values[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, values[i])
			}
		}
	})
}

func TestTransform(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			values := []int{1, 2, 3, 4, 5}
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		}

		transform := func(v int) (int, error) {
			return v * 2, nil
		}

		transformed := sequtil.Transform(seq, transform)
		result, err := sequtil.All(transformed)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []int{2, 4, 6, 8, 10}
		if len(result) != len(expected) {
			t.Errorf("expected %d values, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("transform error", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			values := []int{1, 2, 3, 4, 5}
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		}

		expectedErr := errors.New("transform error")
		transform := func(v int) (int, error) {
			if v == 3 {
				return 0, expectedErr
			}
			return v * 2, nil
		}

		transformed := sequtil.Transform(seq, transform)
		result, err := sequtil.All(transformed)
		if err == nil {
			t.Error("expected error, got nil")
		} else if err.Error() != expectedErr.Error() {
			t.Errorf("expected error %q, got %q", expectedErr.Error(), err.Error())
		}

		expected := []int{2, 4}
		if len(result) != len(expected) {
			t.Errorf("expected %d values before error, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("original sequence error", func(t *testing.T) {
		expectedErr := errors.New("sequence error")
		seq := func(yield func(int, error) bool) {
			if !yield(1, nil) {
				return
			}
			if !yield(2, nil) {
				return
			}
			yield(0, expectedErr) // Error in original sequence
		}

		transform := func(v int) (int, error) {
			return v * 2, nil
		}

		transformed := sequtil.Transform(seq, transform)
		result, err := sequtil.All(transformed)
		if err == nil {
			t.Error("expected error, got nil")
		} else if err.Error() != expectedErr.Error() {
			t.Errorf("expected error %q, got %q", expectedErr.Error(), err.Error())
		}

		expected := []int{2, 4}
		if len(result) != len(expected) {
			t.Errorf("expected %d values before error, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("empty sequence", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			// Empty - no yields
		}

		transform := func(v int) (int, error) {
			return v * 2, nil
		}

		transformed := sequtil.Transform(seq, transform)
		result, err := sequtil.All(transformed)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if len(result) != 0 {
			t.Errorf("expected empty result, got %d values", len(result))
		}
	})

	t.Run("identity transformation", func(t *testing.T) {
		seq := func(yield func(string, error) bool) {
			values := []string{"hello", "world", "test"}
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		}

		transform := func(v string) (string, error) {
			return fmt.Sprintf("prefix-%s", v), nil
		}

		transformed := sequtil.Transform(seq, transform)
		result, err := sequtil.All(transformed)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []string{"prefix-hello", "prefix-world", "prefix-test"}
		if len(result) != len(expected) {
			t.Errorf("expected %d values, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %q, expected %q", i, v, expected[i])
			}
		}
	})
}

func TestChainedOperations(t *testing.T) {
	t.Run("limit then transform", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			for i := range 10 {
				if !yield(i, nil) {
					return
				}
			}
		}

		transform := func(v int) (int, error) {
			return v * v, nil // Square the value
		}

		// First limit to 5, then transform
		limited := sequtil.Limit(seq, 5)
		transformed := sequtil.Transform(limited, transform)
		result, err := sequtil.All(transformed)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []int{0, 1, 4, 9, 16} // squares of 0, 1, 2, 3, 4
		if len(result) != len(expected) {
			t.Errorf("expected %d values, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("transform then limit", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			for i := range 10 {
				if !yield(i, nil) {
					return
				}
			}
		}

		transform := func(v int) (int, error) {
			return v * 10, nil
		}

		// First transform, then limit to 3
		transformed := sequtil.Transform(seq, transform)
		limited := sequtil.Limit(transformed, 3)
		result, err := sequtil.All(limited)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []int{0, 10, 20} // first 3 values * 10
		if len(result) != len(expected) {
			t.Errorf("expected %d values, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})

	t.Run("multiple transforms", func(t *testing.T) {
		seq := func(yield func(int, error) bool) {
			values := []int{1, 2, 3}
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		}

		double := func(v int) (int, error) { return v * 2, nil }
		addTen := func(v int) (int, error) { return v + 10, nil }

		// Chain multiple transforms: double, then add 10
		doubled := sequtil.Transform(seq, double)
		final := sequtil.Transform(doubled, addTen)
		result, err := sequtil.All(final)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		expected := []int{12, 14, 16} // (1*2)+10, (2*2)+10, (3*2)+10
		if len(result) != len(expected) {
			t.Errorf("expected %d values, got %d", len(expected), len(result))
		}
		for i, v := range result {
			if i >= len(expected) || v != expected[i] {
				t.Errorf("unexpected value at index %d: got %d, expected %d", i, v, expected[i])
			}
		}
	})
}
