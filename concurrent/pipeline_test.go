package concurrent_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/firetiger-oss/storage/concurrent"
)

func TestPipeline(t *testing.T) {
	t.Run("processes items concurrently", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 3)

		// Create input sequence
		seq := func(yield func(int, error) bool) {
			for i := 0; i < 10; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}

		// Transform that doubles the input
		transform := func(ctx context.Context, in int) (int, error) {
			time.Sleep(time.Millisecond)
			return in * 2, nil
		}

		results := make([]int, 0, 10)
		for result, err := range concurrent.Pipeline(ctx, seq, transform) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			results = append(results, result)
		}

		if len(results) != 10 {
			t.Fatalf("expected 10 results, got %d", len(results))
		}

		for i, result := range results {
			expected := i * 2
			if result != expected {
				t.Errorf("result[%d] = %d, want %d", i, result, expected)
			}
		}
	})

	t.Run("maintains ordering", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 5)

		seq := func(yield func(int, error) bool) {
			for i := 0; i < 100; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}

		transform := func(ctx context.Context, in int) (int, error) {
			// Vary sleep time to ensure ordering is maintained despite timing
			time.Sleep(time.Duration(100-in) * time.Microsecond)
			return in, nil
		}

		i := 0
		for result, err := range concurrent.Pipeline(ctx, seq, transform) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != i {
				t.Fatalf("expected result %d, got %d", i, result)
			}
			i++
		}
	})

	t.Run("propagates input errors", func(t *testing.T) {
		ctx := t.Context()
		expectedErr := errors.New("input error")

		seq := func(yield func(int, error) bool) {
			if !yield(1, nil) {
				return
			}
			if !yield(2, nil) {
				return
			}
			if !yield(0, expectedErr) {
				return
			}
			yield(3, nil)
		}

		transform := func(ctx context.Context, in int) (int, error) {
			return in, nil
		}

		count := 0
		for _, err := range concurrent.Pipeline(ctx, seq, transform) {
			count++
			if count == 3 && err != expectedErr {
				t.Fatalf("expected error %v, got %v", expectedErr, err)
			}
		}

		if count != 3 {
			t.Fatalf("expected 3 results, got %d", count)
		}
	})

	t.Run("propagates transform errors", func(t *testing.T) {
		ctx := t.Context()
		expectedErr := errors.New("transform error")

		seq := func(yield func(int, error) bool) {
			for i := 0; i < 5; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}

		transform := func(ctx context.Context, in int) (int, error) {
			if in == 2 {
				return 0, expectedErr
			}
			return in, nil
		}

		idx := 0
		for result, err := range concurrent.Pipeline(ctx, seq, transform) {
			if idx == 2 {
				if err != expectedErr {
					t.Fatalf("expected error %v at idx 2, got %v", expectedErr, err)
				}
			} else if err != nil {
				t.Fatalf("unexpected error at index %d: %v (result=%d)", idx, err, result)
			}
			idx++
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		seq := func(yield func(int, error) bool) {
			for i := 0; i < 100; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}

		transform := func(ctx context.Context, in int) (int, error) {
			time.Sleep(time.Millisecond)
			return in, nil
		}

		count := 0
		for range concurrent.Pipeline(ctx, seq, transform) {
			count++
			if count == 5 {
				cancel()
				break
			}
		}

		if count != 5 {
			t.Fatalf("expected 5 results, got %d", count)
		}
	})

	t.Run("respects concurrency limit", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 3)
		var activeCount atomic.Int32
		var maxActive atomic.Int32

		seq := func(yield func(int, error) bool) {
			for i := 0; i < 10; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}

		transform := func(ctx context.Context, in int) (int, error) {
			active := activeCount.Add(1)
			defer activeCount.Add(-1)

			// Track max concurrent executions
			for {
				max := maxActive.Load()
				if active <= max || maxActive.CompareAndSwap(max, active) {
					break
				}
			}

			time.Sleep(time.Millisecond)
			return in, nil
		}

		for range concurrent.Pipeline(ctx, seq, transform) {
		}

		max := maxActive.Load()
		if max > 3 {
			t.Fatalf("expected max concurrency of 3, got %d", max)
		}
	})
}

func TestExec(t *testing.T) {
	t.Run("executes all tasks", func(t *testing.T) {
		ctx := t.Context()
		var count atomic.Int32

		task := func(ctx context.Context) error {
			count.Add(1)
			return nil
		}

		tasks := make([]func(context.Context) error, 10)
		for i := range tasks {
			tasks[i] = task
		}

		errorCount := 0
		for err := range concurrent.Exec(ctx, tasks...) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			errorCount++
		}

		if errorCount != 10 {
			t.Fatalf("expected 10 results, got %d", errorCount)
		}

		if count.Load() != 10 {
			t.Fatalf("expected 10 task executions, got %d", count.Load())
		}
	})

	t.Run("returns errors from tasks", func(t *testing.T) {
		ctx := t.Context()
		expectedErr := errors.New("task error")

		tasks := []func(context.Context) error{
			func(ctx context.Context) error { return nil },
			func(ctx context.Context) error { return expectedErr },
			func(ctx context.Context) error { return nil },
		}

		i := 0
		for err := range concurrent.Exec(ctx, tasks...) {
			if i == 1 && err != expectedErr {
				t.Fatalf("expected error %v, got %v", expectedErr, err)
			} else if i != 1 && err != nil {
				t.Fatalf("unexpected error at index %d: %v", i, err)
			}
			i++
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		tasks := make([]func(context.Context) error, 100)
		for i := range tasks {
			tasks[i] = func(ctx context.Context) error {
				time.Sleep(time.Millisecond)
				return nil
			}
		}

		count := 0
		for range concurrent.Exec(ctx, tasks...) {
			count++
			if count == 5 {
				cancel()
				break
			}
		}

		if count != 5 {
			t.Fatalf("expected 5 results, got %d", count)
		}
	})

	t.Run("executes with concurrency limit", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 2)
		var activeCount atomic.Int32
		var maxActive atomic.Int32

		task := func(ctx context.Context) error {
			active := activeCount.Add(1)
			defer activeCount.Add(-1)

			for {
				max := maxActive.Load()
				if active <= max || maxActive.CompareAndSwap(max, active) {
					break
				}
			}

			time.Sleep(time.Millisecond)
			return nil
		}

		tasks := make([]func(context.Context) error, 10)
		for i := range tasks {
			tasks[i] = task
		}

		for range concurrent.Exec(ctx, tasks...) {
		}

		max := maxActive.Load()
		if max > 2 {
			t.Fatalf("expected max concurrency of 2, got %d", max)
		}
	})
}

func TestQuery(t *testing.T) {
	t.Run("executes all queries", func(t *testing.T) {
		ctx := t.Context()

		queries := []func(context.Context) (int, error){
			func(ctx context.Context) (int, error) { return 1, nil },
			func(ctx context.Context) (int, error) { return 2, nil },
			func(ctx context.Context) (int, error) { return 3, nil },
		}

		results := []int{}
		for result, err := range concurrent.Query(ctx, queries...) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			results = append(results, result)
		}

		expected := []int{1, 2, 3}
		if len(results) != len(expected) {
			t.Fatalf("expected %d results, got %d", len(expected), len(results))
		}

		for i, result := range results {
			if result != expected[i] {
				t.Errorf("result[%d] = %d, want %d", i, result, expected[i])
			}
		}
	})

	t.Run("returns errors from queries", func(t *testing.T) {
		ctx := t.Context()
		expectedErr := errors.New("query error")

		queries := []func(context.Context) (string, error){
			func(ctx context.Context) (string, error) { return "a", nil },
			func(ctx context.Context) (string, error) { return "", expectedErr },
			func(ctx context.Context) (string, error) { return "c", nil },
		}

		i := 0
		for result, err := range concurrent.Query(ctx, queries...) {
			if i == 1 {
				if err != expectedErr {
					t.Fatalf("expected error %v, got %v", expectedErr, err)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error at index %d: %v", i, err)
				}
				if i == 0 && result != "a" {
					t.Fatalf("expected result 'a', got '%s'", result)
				}
				if i == 2 && result != "c" {
					t.Fatalf("expected result 'c', got '%s'", result)
				}
			}
			i++
		}
	})

	t.Run("maintains query ordering", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 5)

		queries := make([]func(context.Context) (int, error), 50)
		for i := range queries {
			i := i // capture
			queries[i] = func(ctx context.Context) (int, error) {
				// Vary timing to ensure ordering is maintained
				time.Sleep(time.Duration(50-i) * time.Microsecond)
				return i, nil
			}
		}

		i := 0
		for result, err := range concurrent.Query(ctx, queries...) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != i {
				t.Fatalf("expected result %d, got %d", i, result)
			}
			i++
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		queries := make([]func(context.Context) (int, error), 100)
		for i := range queries {
			i := i
			queries[i] = func(ctx context.Context) (int, error) {
				time.Sleep(time.Millisecond)
				return i, nil
			}
		}

		count := 0
		for range concurrent.Query(ctx, queries...) {
			count++
			if count == 5 {
				cancel()
				break
			}
		}

		if count != 5 {
			t.Fatalf("expected 5 results, got %d", count)
		}
	})

	t.Run("works with different types", func(t *testing.T) {
		ctx := t.Context()

		type result struct {
			id    int
			value string
		}

		queries := []func(context.Context) (result, error){
			func(ctx context.Context) (result, error) {
				return result{1, "one"}, nil
			},
			func(ctx context.Context) (result, error) {
				return result{2, "two"}, nil
			},
		}

		results := []result{}
		for r, err := range concurrent.Query(ctx, queries...) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			results = append(results, r)
		}

		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		if results[0].id != 1 || results[0].value != "one" {
			t.Fatalf("unexpected result[0]: %+v", results[0])
		}
		if results[1].id != 2 || results[1].value != "two" {
			t.Fatalf("unexpected result[1]: %+v", results[1])
		}
	})

	t.Run("respects concurrency limit", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 3)
		var activeCount atomic.Int32
		var maxActive atomic.Int32

		queries := make([]func(context.Context) (int, error), 10)
		for i := range queries {
			i := i
			queries[i] = func(ctx context.Context) (int, error) {
				active := activeCount.Add(1)
				defer activeCount.Add(-1)

				for {
					max := maxActive.Load()
					if active <= max || maxActive.CompareAndSwap(max, active) {
						break
					}
				}

				time.Sleep(time.Millisecond)
				return i, nil
			}
		}

		for range concurrent.Query(ctx, queries...) {
		}

		max := maxActive.Load()
		if max > 3 {
			t.Fatalf("expected max concurrency of 3, got %d", max)
		}
	})
}

func BenchmarkPipeline(b *testing.B) {
	ctx := concurrent.WithLimit(b.Context(), 10)

	b.Run("small_transform", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			seq := func(yield func(int, error) bool) {
				for j := 0; j < 100; j++ {
					if !yield(j, nil) {
						return
					}
				}
			}

			transform := func(ctx context.Context, in int) (int, error) {
				return in * 2, nil
			}

			for range concurrent.Pipeline(ctx, seq, transform) {
			}
		}
	})

	b.Run("io_bound", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			seq := func(yield func(int, error) bool) {
				for j := 0; j < 50; j++ {
					if !yield(j, nil) {
						return
					}
				}
			}

			transform := func(ctx context.Context, in int) (int, error) {
				time.Sleep(time.Millisecond)
				return in, nil
			}

			for range concurrent.Pipeline(ctx, seq, transform) {
			}
		}
	})
}

func BenchmarkExec(b *testing.B) {
	ctx := concurrent.WithLimit(b.Context(), 10)

	b.Run("simple_tasks", func(b *testing.B) {
		tasks := make([]func(context.Context) error, 100)
		for i := range tasks {
			tasks[i] = func(ctx context.Context) error {
				return nil
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for range concurrent.Exec(ctx, tasks...) {
			}
		}
	})
}

func BenchmarkQuery(b *testing.B) {
	ctx := concurrent.WithLimit(b.Context(), 10)

	b.Run("simple_queries", func(b *testing.B) {
		queries := make([]func(context.Context) (int, error), 100)
		for i := range queries {
			i := i
			queries[i] = func(ctx context.Context) (int, error) {
				return i, nil
			}
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for range concurrent.Query(ctx, queries...) {
			}
		}
	})
}

// Example demonstrating Pipeline usage
func ExamplePipeline() {
	ctx := concurrent.WithLimit(context.Background(), 5)

	// Create a sequence of numbers
	numbers := func(yield func(int, error) bool) {
		for i := 1; i <= 10; i++ {
			if !yield(i, nil) {
				return
			}
		}
	}

	// Transform function that squares each number
	square := func(ctx context.Context, n int) (int, error) {
		return n * n, nil
	}

	// Process the pipeline
	for result, err := range concurrent.Pipeline(ctx, numbers, square) {
		if err != nil {
			panic(err)
		}
		_ = result // use result
	}
}

// Example demonstrating Exec usage
func ExampleExec() {
	ctx := concurrent.WithLimit(context.Background(), 3)

	task1 := func(ctx context.Context) error {
		// do work
		return nil
	}

	task2 := func(ctx context.Context) error {
		// do work
		return nil
	}

	task3 := func(ctx context.Context) error {
		// do work
		return nil
	}

	// Execute all tasks concurrently
	for err := range concurrent.Exec(ctx, task1, task2, task3) {
		if err != nil {
			// handle error
			_ = err
		}
	}
}

// Example demonstrating Query usage
func ExampleQuery() {
	ctx := concurrent.WithLimit(context.Background(), 5)

	query1 := func(ctx context.Context) (string, error) {
		return "result1", nil
	}

	query2 := func(ctx context.Context) (string, error) {
		return "result2", nil
	}

	// Execute all queries concurrently
	for result, err := range concurrent.Query(ctx, query1, query2) {
		if err != nil {
			// handle error
			continue
		}
		_ = result // use result
	}
}
