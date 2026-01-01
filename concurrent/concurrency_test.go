package concurrent_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/firetiger-oss/storage/concurrent"
)

func TestWithLimit(t *testing.T) {
	tests := []struct {
		name             string
		setupCtx         func() context.Context
		inputConcurrency int
		expectedResult   int
		description      string
	}{
		{
			name:             "adds max concurrency to context",
			setupCtx:         context.Background,
			inputConcurrency: 5,
			expectedResult:   5,
			description:      "should set max concurrency when added to empty context",
		},
		{
			name: "ignores zero max concurrency",
			setupCtx: func() context.Context {
				return concurrent.WithLimit(t.Context(), 8)
			},
			inputConcurrency: 0,
			expectedResult:   8,
			description:      "should ignore zero values and keep existing limit",
		},
		{
			name: "ignores negative max concurrency",
			setupCtx: func() context.Context {
				return concurrent.WithLimit(t.Context(), 6)
			},
			inputConcurrency: -5,
			expectedResult:   6,
			description:      "should ignore negative values and keep existing limit",
		},
		{
			name: "never allows increasing the limit",
			setupCtx: func() context.Context {
				return concurrent.WithLimit(t.Context(), 5)
			},
			inputConcurrency: 10,
			expectedResult:   5,
			description:      "should not allow increasing existing limit",
		},
		{
			name: "allows decreasing the limit",
			setupCtx: func() context.Context {
				return concurrent.WithLimit(t.Context(), 10)
			},
			inputConcurrency: 3,
			expectedResult:   3,
			description:      "should allow decreasing existing limit",
		},
		{
			name:             "works with context without max concurrency",
			setupCtx:         context.Background,
			inputConcurrency: 7,
			expectedResult:   7,
			description:      "should set limit on context that doesn't have one",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			newCtx := concurrent.WithLimit(ctx, tt.inputConcurrency)
			got := concurrent.Limit(newCtx)

			if got != tt.expectedResult {
				t.Errorf("%s: expected max concurrency %d, got %d", tt.description, tt.expectedResult, got)
			}
		})
	}

	t.Run("multiple sequential operations", func(t *testing.T) {
		sequentialTests := []struct {
			name        string
			operations  []int
			expected    int
			description string
		}{
			{
				name:        "multiple sequential decreases",
				operations:  []int{20, 15, 10},
				expected:    10,
				description: "should allow sequential decreases",
			},
			{
				name:        "mixed increase and decrease attempts",
				operations:  []int{10, 15, 5}, // 15 should be ignored (increase), 5 should be applied
				expected:    5,
				description: "should ignore increases but allow decreases",
			},
		}

		for _, tt := range sequentialTests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := t.Context()
				for _, op := range tt.operations {
					ctx = concurrent.WithLimit(ctx, op)
				}

				got := concurrent.Limit(ctx)
				if got != tt.expected {
					t.Errorf("%s: expected max concurrency %d, got %d", tt.description, tt.expected, got)
				}
			})
		}
	})

	t.Run("context inheritance", func(t *testing.T) {
		type testKey struct{}
		// Test that context values are properly inherited through multiple layers
		ctx := t.Context()
		ctx = context.WithValue(ctx, testKey{}, "test_value")

		newCtx := concurrent.WithLimit(ctx, 15)

		// Check that max concurrency is set
		got := concurrent.Limit(newCtx)
		if got != 15 {
			t.Errorf("expected max concurrency 15, got %d", got)
		}

		// Check that original context values are preserved
		value := newCtx.Value(testKey{})
		if value != "test_value" {
			t.Errorf("expected context value 'test_value', got %v", value)
		}
	})
}

func TestLimit(t *testing.T) {
	tests := []struct {
		name        string
		setupCtx    func() context.Context
		expected    int
		description string
	}{
		{
			name:        "returns default for context without max concurrency",
			setupCtx:    context.Background,
			expected:    concurrent.DefaultLimit,
			description: "should return default when no max concurrency is set",
		},
		{
			name: "returns stored value when present",
			setupCtx: func() context.Context {
				return concurrent.WithLimit(t.Context(), 8)
			},
			expected:    8,
			description: "should return stored concurrency value",
		},
		{
			name: "allows minimum limit of 1",
			setupCtx: func() context.Context {
				return concurrent.WithLimit(t.Context(), 1)
			},
			expected:    1,
			description: "should allow minimum limit of 1",
		},
		{
			name: "enforces maximum limit of 1000",
			setupCtx: func() context.Context {
				return concurrent.WithLimit(t.Context(), 2000)
			},
			expected:    1000,
			description: "should enforce maximum limit of 1000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := tt.setupCtx()
			got := concurrent.Limit(ctx)

			if got != tt.expected {
				t.Errorf("%s: expected max concurrency %d, got %d", tt.description, tt.expected, got)
			}
		})
	}

	t.Run("handles values within valid range", func(t *testing.T) {
		validValues := []int{2, 5, 10, 50, 100, 500, 1000}

		for _, maxConcurrency := range validValues {
			t.Run(fmt.Sprintf("concurrency_%d", maxConcurrency), func(t *testing.T) {
				ctx := concurrent.WithLimit(t.Context(), maxConcurrency)
				got := concurrent.Limit(ctx)

				if got != maxConcurrency {
					t.Errorf("expected max concurrency %d, got %d", maxConcurrency, got)
				}
			})
		}
	})

	t.Run("boundary value testing", func(t *testing.T) {
		boundaryTests := []struct {
			input    int
			expected int
			name     string
		}{
			{0, concurrent.DefaultLimit, "zero_input_ignored_gets_default"},
			{1, 1, "one_input_gets_min"},
			{2, 2, "two_boundary"},
			{3, 3, "just_above_min"},
			{999, 999, "just_below_max"},
			{1000, 1000, "max_boundary"},
			{1001, 1000, "just_above_max_gets_max"},
			{2000, 1000, "large_input_gets_max"},
			{-1, concurrent.DefaultLimit, "negative_input_ignored_gets_default"},
		}

		for _, tt := range boundaryTests {
			t.Run(tt.name, func(t *testing.T) {
				ctx := concurrent.WithLimit(t.Context(), tt.input)
				got := concurrent.Limit(ctx)

				if got != tt.expected {
					t.Errorf("input %d: expected max concurrency %d, got %d", tt.input, tt.expected, got)
				}
			})
		}
	})
}

func TestDefaultLimit(t *testing.T) {
	tests := []struct {
		name        string
		testFunc    func() int
		expected    int
		description string
	}{
		{
			name:        "default constant has expected value",
			testFunc:    func() int { return concurrent.DefaultLimit },
			expected:    10,
			description: "DefaultLimit constant should be 10",
		},
		{
			name:        "default is used when no context value set",
			testFunc:    func() int { return concurrent.Limit(t.Context()) },
			expected:    concurrent.DefaultLimit,
			description: "should use default when no context value is set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.testFunc()
			if got != tt.expected {
				t.Errorf("%s: expected %d, got %d", tt.description, tt.expected, got)
			}
		})
	}
}

func TestConcurrencyContextIntegration(t *testing.T) {
	integrationTests := []struct {
		name        string
		testFunc    func(t *testing.T)
		description string
	}{
		{
			name: "realistic workflow",
			testFunc: func(t *testing.T) {
				// Define workflow steps
				workflowSteps := []struct {
					operation string
					input     int
					expected  int
				}{
					{"initial", 0, concurrent.DefaultLimit},
					{"app_level", 50, 50},
					{"operation_level", 20, 20},
					{"sub_operation_increase_attempt", 30, 20}, // Should be ignored
					{"final_restriction", 5, 5},
				}

				ctx := t.Context()

				for _, step := range workflowSteps {
					switch step.operation {
					case "initial":
						// Just check the initial value
						got := concurrent.Limit(ctx)
						if got != step.expected {
							t.Errorf("step %s: expected %d, got %d", step.operation, step.expected, got)
						}
					case "app_level", "operation_level", "sub_operation_increase_attempt", "final_restriction":
						ctx = concurrent.WithLimit(ctx, step.input)
						got := concurrent.Limit(ctx)
						if got != step.expected {
							t.Errorf("step %s: expected %d, got %d", step.operation, step.expected, got)
						}
					}
				}
			},
			description: "should handle realistic progressive limiting workflow",
		},
		{
			name: "context cancellation preserves max concurrency",
			testFunc: func(t *testing.T) {
				ctx := t.Context()
				concurrencyCtx := concurrent.WithLimit(ctx, 25)

				cancelCtx, cancel := context.WithCancel(concurrencyCtx)
				defer cancel()

				got := concurrent.Limit(cancelCtx)
				expected := 25
				if got != expected {
					t.Errorf("expected max concurrency %d after cancellation context, got %d", expected, got)
				}
			},
			description: "should preserve max concurrency through context cancellation",
		},
	}

	for _, tt := range integrationTests {
		t.Run(tt.name, func(t *testing.T) {
			tt.testFunc(t)
		})
	}
}

func TestProcess(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		ctx := t.Context()
		queue := concurrent.NewQueue[string]()

		// Add some jobs to the queue
		queue.Push(func(ctx context.Context, yield func(string, error) bool) {
			yield("job1", nil)
			yield("job2", nil)
		})
		queue.Push(func(ctx context.Context, yield func(string, error) bool) {
			yield("job3", nil)
		})
		queue.Done()

		var results []string
		for result, err := range concurrent.Process(ctx, queue) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		sort.Strings(results)
		expected := []string{"job1", "job2", "job3"}
		if len(results) != len(expected) {
			t.Errorf("expected %d results, got %d", len(expected), len(results))
		}
		for i, result := range results {
			if i >= len(expected) || result != expected[i] {
				t.Errorf("unexpected result at index %d: got %q, expected %q", i, result, expected[i])
			}
		}
	})

	t.Run("error propagation", func(t *testing.T) {
		ctx := t.Context()
		queue := concurrent.NewQueue[string]()

		expectedErr := errors.New("test error")
		queue.Push(func(ctx context.Context, yield func(string, error) bool) {
			yield("", expectedErr)
		})
		queue.Done()

		var gotErr error
		for _, err := range concurrent.Process(ctx, queue) {
			if err != nil {
				gotErr = err
				break
			}
		}

		if gotErr == nil {
			t.Error("expected error, got nil")
		} else if gotErr.Error() != expectedErr.Error() {
			t.Errorf("expected error %q, got %q", expectedErr.Error(), gotErr.Error())
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		queue := concurrent.NewQueue[int]()
		queue.Push(func(ctx context.Context, yield func(int, error) bool) {
			for i := range 1000 {
				select {
				case <-ctx.Done():
				case <-time.After(time.Millisecond):
				}
				if !yield(i, context.Cause(ctx)) {
					return
				}
			}
		})
		queue.Done()

		count := 0
		start := time.Now()
		for range concurrent.Process(ctx, queue) {
			count++
			if count == 5 {
				cancel()
			}
		}
		duration := time.Since(start)

		if count > 10 {
			t.Errorf("expected cancellation to stop iteration early, but got %d results", count)
		}
		if duration > 50*time.Millisecond {
			t.Errorf("expected cancellation to be fast, but took %v", duration)
		}
	})

	t.Run("concurrency control", func(t *testing.T) {
		const maxConcurrency = 3
		ctx := concurrent.WithLimit(t.Context(), maxConcurrency)
		queue := concurrent.NewQueue[string]()

		var activeWorkers int64
		var maxActiveWorkers int64
		var mu sync.Mutex

		// Add jobs that track concurrent execution
		for i := range 10 {
			job := fmt.Sprintf("job%d", i)
			queue.Push(func(ctx context.Context, yield func(string, error) bool) {
				current := atomic.AddInt64(&activeWorkers, 1)
				defer atomic.AddInt64(&activeWorkers, -1)

				mu.Lock()
				if current > maxActiveWorkers {
					maxActiveWorkers = current
				}
				mu.Unlock()

				select {
				case <-ctx.Done():
				case <-time.After(time.Millisecond):
				}
				yield(job, nil)
			})
		}
		queue.Done()

		var results []string
		for result, err := range concurrent.Process(ctx, queue) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != 10 {
			t.Errorf("expected 10 results, got %d", len(results))
		}

		mu.Lock()
		actualMaxConcurrency := maxActiveWorkers
		mu.Unlock()

		if actualMaxConcurrency > int64(maxConcurrency) {
			t.Errorf("expected max concurrent workers %d, but saw %d", maxConcurrency, actualMaxConcurrency)
		}
		if actualMaxConcurrency == 0 {
			t.Error("expected some concurrent execution, but maxActiveWorkers was 0")
		}
	})

	t.Run("empty queue", func(t *testing.T) {
		ctx := t.Context()
		queue := concurrent.NewQueue[string]()
		queue.Done()

		var results []string
		for result, err := range concurrent.Process(ctx, queue) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != 0 {
			t.Errorf("expected no results from empty queue, got %d", len(results))
		}
	})

	t.Run("mixed success and error jobs", func(t *testing.T) {
		ctx := t.Context()
		queue := concurrent.NewQueue[int]()

		// Add successful jobs
		queue.Push(func(ctx context.Context, yield func(int, error) bool) {
			yield(1, nil)
			yield(2, nil)
		})

		// Add failing job
		queue.Push(func(ctx context.Context, yield func(int, error) bool) {
			yield(0, errors.New("failure"))
		})

		// Add more successful jobs
		queue.Push(func(ctx context.Context, yield func(int, error) bool) {
			yield(3, nil)
		})
		queue.Done()

		var successes []int
		var errs []error
		for result, err := range concurrent.Process(ctx, queue) {
			if err != nil {
				errs = append(errs, err)
			} else {
				successes = append(successes, result)
			}
		}

		if len(errs) != 1 {
			t.Errorf("expected 1 error, got %d", len(errs))
		}
		if len(successes) != 3 {
			t.Errorf("expected 3 successful results, got %d", len(successes))
		}
		sort.Ints(successes)
		expectedSuccesses := []int{1, 2, 3}
		for i, success := range successes {
			if i >= len(expectedSuccesses) || success != expectedSuccesses[i] {
				t.Errorf("unexpected success at index %d: got %d, expected %d", i, success, expectedSuccesses[i])
			}
		}
	})

	t.Run("large number of jobs", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 5)
		queue := concurrent.NewQueue[int]()

		const numJobs = 100
		for i := range numJobs {
			value := i
			queue.Push(func(ctx context.Context, yield func(int, error) bool) {
				yield(value, nil)
			})
		}
		queue.Done()

		var results []int
		for result, err := range concurrent.Process(ctx, queue) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != numJobs {
			t.Errorf("expected %d results, got %d", numJobs, len(results))
		}

		sort.Ints(results)
		for i, result := range results {
			if result != i {
				t.Errorf("unexpected result at index %d: got %d, expected %d", i, result, i)
			}
		}
	})

	t.Run("jobs with multiple yields", func(t *testing.T) {
		ctx := t.Context()
		queue := concurrent.NewQueue[string]()

		queue.Push(func(ctx context.Context, yield func(string, error) bool) {
			values := []string{"a", "b", "c"}
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		})
		queue.Push(func(ctx context.Context, yield func(string, error) bool) {
			values := []string{"x", "y", "z"}
			for _, v := range values {
				if !yield(v, nil) {
					return
				}
			}
		})
		queue.Done()

		var results []string
		for result, err := range concurrent.Process(ctx, queue) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		sort.Strings(results)
		expected := []string{"a", "b", "c", "x", "y", "z"}
		if len(results) != len(expected) {
			t.Errorf("expected %d results, got %d", len(expected), len(results))
		}
		for i, result := range results {
			if i >= len(expected) || result != expected[i] {
				t.Errorf("unexpected result at index %d: got %q, expected %q", i, result, expected[i])
			}
		}
	})
}

func TestRun(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		ctx := t.Context()
		jobs := []int{1, 2, 3, 4, 5}

		process := func(ctx context.Context, job int) (string, error) {
			return fmt.Sprintf("result-%d", job*2), nil
		}

		var results []string
		for result, err := range concurrent.Run(ctx, jobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		sort.Strings(results)
		expected := []string{"result-10", "result-2", "result-4", "result-6", "result-8"}
		if len(results) != len(expected) {
			t.Errorf("expected %d results, got %d", len(expected), len(results))
		}
		for i, result := range results {
			if i >= len(expected) || result != expected[i] {
				t.Errorf("unexpected result at index %d: got %q, expected %q", i, result, expected[i])
			}
		}
	})

	t.Run("error handling", func(t *testing.T) {
		ctx := t.Context()
		jobs := []int{1, 2, 3}

		process := func(ctx context.Context, job int) (string, error) {
			if job == 2 {
				return "", errors.New("job 2 failed")
			}
			return fmt.Sprintf("result-%d", job), nil
		}

		var results []string
		var errs []error
		for result, err := range concurrent.Run(ctx, jobs, process) {
			if err != nil {
				errs = append(errs, err)
			} else {
				results = append(results, result)
			}
		}

		if len(errs) != 1 {
			t.Errorf("expected 1 error, got %d", len(errs))
		}
		if len(results) != 2 {
			t.Errorf("expected 2 successful results, got %d", len(results))
		}
	})

	t.Run("empty input", func(t *testing.T) {
		ctx := t.Context()
		var jobs []int

		process := func(ctx context.Context, job int) (string, error) {
			return fmt.Sprintf("result-%d", job), nil
		}

		var results []string
		for result, err := range concurrent.Run(ctx, jobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != 0 {
			t.Errorf("expected no results for empty input, got %d", len(results))
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		jobs := make([]int, 100)
		for i := range jobs {
			jobs[i] = i
		}

		process := func(ctx context.Context, job int) (int, error) {
			select {
			case <-time.After(time.Millisecond):
				return job, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}

		count := 0
		start := time.Now()
		for _, err := range concurrent.Run(ctx, jobs, process) {
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			if err == nil {
				count++
				if count == 5 {
					cancel()
				}
			}
		}
		duration := time.Since(start)

		if count > 10 {
			t.Errorf("expected cancellation to stop processing early, but got %d results", count)
		}
		if duration > 50*time.Millisecond {
			t.Errorf("expected cancellation to be fast, but took %v", duration)
		}
	})

	t.Run("concurrency control", func(t *testing.T) {
		const maxConcurrency = 3
		ctx := concurrent.WithLimit(t.Context(), maxConcurrency)

		jobs := make([]int, 10)
		for i := range jobs {
			jobs[i] = i
		}

		var activeWorkers int64
		var maxActiveWorkers int64
		var mu sync.Mutex

		process := func(ctx context.Context, job int) (int, error) {
			current := atomic.AddInt64(&activeWorkers, 1)
			defer atomic.AddInt64(&activeWorkers, -1)

			mu.Lock()
			if current > maxActiveWorkers {
				maxActiveWorkers = current
			}
			mu.Unlock()

			select {
			case <-ctx.Done():
			case <-time.After(time.Millisecond):
			}
			return job * 2, nil
		}

		var results []int
		for result, err := range concurrent.Run(ctx, jobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != 10 {
			t.Errorf("expected 10 results, got %d", len(results))
		}

		mu.Lock()
		actualMaxConcurrency := maxActiveWorkers
		mu.Unlock()

		if actualMaxConcurrency > int64(maxConcurrency) {
			t.Errorf("expected max concurrent workers %d, but saw %d", maxConcurrency, actualMaxConcurrency)
		}
		if actualMaxConcurrency == 0 {
			t.Error("expected some concurrent execution, but maxActiveWorkers was 0")
		}
	})
}

func TestRun2(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		ctx := t.Context()
		jobs := map[string]int{
			"a": 1,
			"b": 2,
			"c": 3,
		}

		process := func(ctx context.Context, key string, value int) (string, error) {
			return fmt.Sprintf("%s:%d", key, value*2), nil
		}

		var results []string
		for result, err := range concurrent.Run2(ctx, jobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		sort.Strings(results)
		expected := []string{"a:2", "b:4", "c:6"}
		if len(results) != len(expected) {
			t.Errorf("expected %d results, got %d", len(expected), len(results))
		}
		for i, result := range results {
			if i >= len(expected) || result != expected[i] {
				t.Errorf("unexpected result at index %d: got %q, expected %q", i, result, expected[i])
			}
		}
	})

	t.Run("error handling", func(t *testing.T) {
		ctx := t.Context()
		jobs := map[string]int{
			"good": 1,
			"bad":  2,
			"ok":   3,
		}

		process := func(ctx context.Context, key string, value int) (string, error) {
			if key == "bad" {
				return "", errors.New("bad key")
			}
			return fmt.Sprintf("%s:%d", key, value), nil
		}

		var results []string
		var errs []error
		for result, err := range concurrent.Run2(ctx, jobs, process) {
			if err != nil {
				errs = append(errs, err)
			} else {
				results = append(results, result)
			}
		}

		if len(errs) != 1 {
			t.Errorf("expected 1 error, got %d", len(errs))
		}
		if len(results) != 2 {
			t.Errorf("expected 2 successful results, got %d", len(results))
		}
	})

	t.Run("empty input", func(t *testing.T) {
		ctx := t.Context()
		jobs := map[string]int{}

		process := func(ctx context.Context, key string, value int) (string, error) {
			return fmt.Sprintf("%s:%d", key, value), nil
		}

		var results []string
		for result, err := range concurrent.Run2(ctx, jobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != 0 {
			t.Errorf("expected no results for empty input, got %d", len(results))
		}
	})

	t.Run("nil map", func(t *testing.T) {
		ctx := t.Context()
		var jobs map[string]int

		process := func(ctx context.Context, key string, value int) (string, error) {
			return fmt.Sprintf("%s:%d", key, value), nil
		}

		var results []string
		for result, err := range concurrent.Run2(ctx, jobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != 0 {
			t.Errorf("expected no results for nil map, got %d", len(results))
		}
	})

	t.Run("large map", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 5)

		jobs := make(map[int]string)
		for i := range 50 {
			jobs[i] = fmt.Sprintf("value%d", i)
		}

		process := func(ctx context.Context, key int, value string) (string, error) {
			return fmt.Sprintf("%d-%s", key, value), nil
		}

		var results []string
		for result, err := range concurrent.Run2(ctx, jobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != 50 {
			t.Errorf("expected 50 results, got %d", len(results))
		}
	})
}

func TestRunTasks(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		ctx := t.Context()
		tasks := []string{"task1", "task2", "task3"}

		var executed []string
		var mu sync.Mutex

		process := func(ctx context.Context, task string) error {
			mu.Lock()
			executed = append(executed, task)
			mu.Unlock()
			return nil
		}

		err := concurrent.RunTasks(ctx, tasks, process)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		mu.Lock()
		sort.Strings(executed)
		mu.Unlock()

		expected := []string{"task1", "task2", "task3"}
		if len(executed) != len(expected) {
			t.Errorf("expected %d executed tasks, got %d", len(expected), len(executed))
		}
		for i, task := range executed {
			if i >= len(expected) || task != expected[i] {
				t.Errorf("unexpected executed task at index %d: got %q, expected %q", i, task, expected[i])
			}
		}
	})

	t.Run("error handling - fail fast", func(t *testing.T) {
		ctx := t.Context()
		tasks := []string{"good1", "bad", "good2"}

		var executed []string
		var mu sync.Mutex
		expectedErr := errors.New("task failed")

		process := func(ctx context.Context, task string) error {
			mu.Lock()
			executed = append(executed, task)
			mu.Unlock()

			if task == "bad" {
				return expectedErr
			}
			return nil
		}

		err := concurrent.RunTasks(ctx, tasks, process)
		if err == nil {
			t.Error("expected error, got nil")
		} else if err.Error() != expectedErr.Error() {
			t.Errorf("expected error %q, got %q", expectedErr.Error(), err.Error())
		}

		// Some tasks should have been executed before the error occurred
		mu.Lock()
		numExecuted := len(executed)
		mu.Unlock()

		if numExecuted == 0 {
			t.Error("expected at least one task to be executed")
		}
	})

	t.Run("empty tasks", func(t *testing.T) {
		ctx := t.Context()
		var tasks []string

		process := func(ctx context.Context, task string) error {
			return nil
		}

		err := concurrent.RunTasks(ctx, tasks, process)
		if err != nil {
			t.Errorf("unexpected error for empty tasks: %v", err)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		tasks := make([]int, 100)
		for i := range tasks {
			tasks[i] = i
		}

		var executed int64

		process := func(ctx context.Context, task int) error {
			current := atomic.AddInt64(&executed, 1)
			if current == 5 {
				cancel()
			}
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-time.After(100 * time.Second):
				return nil
			}
		}

		start := time.Now()
		err := concurrent.RunTasks(ctx, tasks, process)
		duration := time.Since(start)

		if err == nil {
			t.Error("expected context cancellation error")
		} else if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled error, got %v", err)
		}

		if duration > 50*time.Millisecond {
			t.Errorf("expected cancellation to be fast, but took %v", duration)
		}
	})

	t.Run("concurrency control", func(t *testing.T) {
		const maxConcurrency = 3
		ctx := concurrent.WithLimit(t.Context(), maxConcurrency)

		tasks := make([]int, 15)
		for i := range tasks {
			tasks[i] = i
		}

		var activeWorkers int64
		var maxActiveWorkers int64
		var mu sync.Mutex

		process := func(ctx context.Context, task int) error {
			current := atomic.AddInt64(&activeWorkers, 1)
			defer atomic.AddInt64(&activeWorkers, -1)

			mu.Lock()
			if current > maxActiveWorkers {
				maxActiveWorkers = current
			}
			mu.Unlock()

			select {
			case <-ctx.Done():
			case <-time.After(time.Millisecond):
			}
			return nil
		}

		err := concurrent.RunTasks(ctx, tasks, process)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		mu.Lock()
		actualMaxConcurrency := maxActiveWorkers
		mu.Unlock()

		if actualMaxConcurrency > int64(maxConcurrency) {
			t.Errorf("expected max concurrent workers %d, but saw %d", maxConcurrency, actualMaxConcurrency)
		}
		if actualMaxConcurrency == 0 {
			t.Error("expected some concurrent execution, but maxActiveWorkers was 0")
		}
	})
}

func TestRunTasks2(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		ctx := t.Context()
		tasks := map[string]int{
			"task1": 1,
			"task2": 2,
			"task3": 3,
		}

		var executed []string
		var mu sync.Mutex

		process := func(ctx context.Context, key string, value int) error {
			mu.Lock()
			executed = append(executed, fmt.Sprintf("%s:%d", key, value))
			mu.Unlock()
			return nil
		}

		err := concurrent.RunTasks2(ctx, tasks, process)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		mu.Lock()
		sort.Strings(executed)
		mu.Unlock()

		expected := []string{"task1:1", "task2:2", "task3:3"}
		if len(executed) != len(expected) {
			t.Errorf("expected %d executed tasks, got %d", len(expected), len(executed))
		}
		for i, task := range executed {
			if i >= len(expected) || task != expected[i] {
				t.Errorf("unexpected executed task at index %d: got %q, expected %q", i, task, expected[i])
			}
		}
	})

	t.Run("error handling - fail fast", func(t *testing.T) {
		ctx := t.Context()
		tasks := map[string]int{
			"good": 1,
			"bad":  2,
			"ok":   3,
		}

		var executed []string
		var mu sync.Mutex
		expectedErr := errors.New("task failed")

		process := func(ctx context.Context, key string, value int) error {
			mu.Lock()
			executed = append(executed, key)
			mu.Unlock()

			if key == "bad" {
				return expectedErr
			}
			return nil
		}

		err := concurrent.RunTasks2(ctx, tasks, process)
		if err == nil {
			t.Error("expected error, got nil")
		} else if err.Error() != expectedErr.Error() {
			t.Errorf("expected error %q, got %q", expectedErr.Error(), err.Error())
		}

		// Some tasks should have been executed before the error occurred
		mu.Lock()
		numExecuted := len(executed)
		mu.Unlock()

		if numExecuted == 0 {
			t.Error("expected at least one task to be executed")
		}
	})

	t.Run("empty tasks", func(t *testing.T) {
		ctx := t.Context()
		tasks := map[string]int{}

		process := func(ctx context.Context, key string, value int) error {
			return nil
		}

		err := concurrent.RunTasks2(ctx, tasks, process)
		if err != nil {
			t.Errorf("unexpected error for empty tasks: %v", err)
		}
	})

	t.Run("nil map", func(t *testing.T) {
		ctx := t.Context()
		var tasks map[string]int

		process := func(ctx context.Context, key string, value int) error {
			return nil
		}

		err := concurrent.RunTasks2(ctx, tasks, process)
		if err != nil {
			t.Errorf("unexpected error for nil map: %v", err)
		}
	})

	t.Run("large map with different types", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 4)

		tasks := make(map[int]string)
		for i := range 20 {
			tasks[i] = fmt.Sprintf("value%d", i)
		}

		var executed int64

		process := func(ctx context.Context, key int, value string) error {
			atomic.AddInt64(&executed, 1)
			select {
			case <-ctx.Done():
			case <-time.After(time.Millisecond):
			}
			return nil
		}

		err := concurrent.RunTasks2(ctx, tasks, process)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if executed != 20 {
			t.Errorf("expected 20 executed tasks, got %d", executed)
		}
	})
}

func TestRunVsRunTasks(t *testing.T) {
	t.Run("Run and RunTasks equivalent behavior", func(t *testing.T) {
		ctx := concurrent.WithLimit(t.Context(), 3)
		tasks := []int{1, 2, 3, 4, 5}

		// Test with Run - should collect all results
		var runResults []string
		for result, err := range concurrent.Run(ctx, tasks, func(ctx context.Context, task int) (string, error) {
			return fmt.Sprintf("result-%d", task), nil
		}) {
			if err != nil {
				t.Errorf("Run: unexpected error: %v", err)
				continue
			}
			runResults = append(runResults, result)
		}

		// Test with RunTasks - should execute all tasks successfully
		var runTasksExecuted []string
		var mu sync.Mutex
		err := concurrent.RunTasks(ctx, tasks, func(ctx context.Context, task int) error {
			result := fmt.Sprintf("result-%d", task)
			mu.Lock()
			runTasksExecuted = append(runTasksExecuted, result)
			mu.Unlock()
			return nil
		})

		if err != nil {
			t.Errorf("RunTasks: unexpected error: %v", err)
		}

		sort.Strings(runResults)
		mu.Lock()
		sort.Strings(runTasksExecuted)
		mu.Unlock()

		if len(runResults) != len(runTasksExecuted) {
			t.Errorf("expected same number of results: Run got %d, RunTasks got %d", len(runResults), len(runTasksExecuted))
		}

		for i, result := range runResults {
			if i >= len(runTasksExecuted) || result != runTasksExecuted[i] {
				t.Errorf("result mismatch at index %d: Run got %q, RunTasks got %q", i, result, runTasksExecuted[i])
			}
		}
	})
}

// TestNestedConcurrency verifies that nested concurrent calls share the same
// concurrency pool, preventing multiplicative concurrency.
func TestNestedConcurrency(t *testing.T) {
	t.Run("nested Run calls share concurrency limit", func(t *testing.T) {
		const maxConcurrency = 5
		ctx := concurrent.WithLimit(t.Context(), maxConcurrency)

		var activeWorkers int64
		var maxActiveWorkers int64

		outerJobs := make([]int, 10)
		for i := range outerJobs {
			outerJobs[i] = i
		}

		innerJobs := []int{1, 2, 3}

		process := func(ctx context.Context, outerJob int) (int, error) {
			// Each outer job spawns inner jobs
			innerSum := 0
			for result, err := range concurrent.Run(ctx, innerJobs, func(ctx context.Context, innerJob int) (int, error) {
				current := atomic.AddInt64(&activeWorkers, 1)
				defer atomic.AddInt64(&activeWorkers, -1)

				// Track max concurrent workers
				for {
					max := atomic.LoadInt64(&maxActiveWorkers)
					if current <= max || atomic.CompareAndSwapInt64(&maxActiveWorkers, max, current) {
						break
					}
				}

				time.Sleep(time.Millisecond)
				return innerJob * outerJob, nil
			}) {
				if err != nil {
					return 0, err
				}
				innerSum += result
			}
			return innerSum, nil
		}

		var results []int
		for result, err := range concurrent.Run(ctx, outerJobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		// Verify we got all results
		if len(results) != len(outerJobs) {
			t.Errorf("expected %d results, got %d", len(outerJobs), len(results))
		}

		// Verify concurrency was limited - with nested calls sharing the pool,
		// we should never exceed maxConcurrency
		max := atomic.LoadInt64(&maxActiveWorkers)
		if max > int64(maxConcurrency) {
			t.Errorf("expected max concurrency of %d, but observed %d concurrent workers", maxConcurrency, max)
		}
	})

	t.Run("nested Pipeline calls share concurrency limit", func(t *testing.T) {
		const maxConcurrency = 4
		ctx := concurrent.WithLimit(t.Context(), maxConcurrency)

		var activeWorkers int64
		var maxActiveWorkers int64

		// Create input sequence
		inputSeq := func(yield func(int, error) bool) {
			for i := 0; i < 8; i++ {
				if !yield(i, nil) {
					return
				}
			}
		}

		// Outer pipeline that spawns inner pipelines
		outerTransform := func(ctx context.Context, outer int) (int, error) {
			innerSeq := func(yield func(int, error) bool) {
				for j := 0; j < 3; j++ {
					if !yield(j, nil) {
						return
					}
				}
			}

			sum := 0
			for result, err := range concurrent.Pipeline(ctx, innerSeq, func(ctx context.Context, inner int) (int, error) {
				current := atomic.AddInt64(&activeWorkers, 1)
				defer atomic.AddInt64(&activeWorkers, -1)

				for {
					max := atomic.LoadInt64(&maxActiveWorkers)
					if current <= max || atomic.CompareAndSwapInt64(&maxActiveWorkers, max, current) {
						break
					}
				}

				time.Sleep(time.Millisecond)
				return outer * inner, nil
			}) {
				if err != nil {
					return 0, err
				}
				sum += result
			}
			return sum, nil
		}

		var results []int
		for result, err := range concurrent.Pipeline(ctx, inputSeq, outerTransform) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				continue
			}
			results = append(results, result)
		}

		if len(results) != 8 {
			t.Errorf("expected 8 results, got %d", len(results))
		}

		max := atomic.LoadInt64(&maxActiveWorkers)
		if max > int64(maxConcurrency) {
			t.Errorf("expected max concurrency of %d, but observed %d concurrent workers", maxConcurrency, max)
		}
	})

	t.Run("sub-limit respected within shared pool", func(t *testing.T) {
		const outerLimit = 10
		const innerLimit = 3
		ctx := concurrent.WithLimit(t.Context(), outerLimit)

		// Track workers per-inner-Run to verify each Run respects its own limit
		type runStats struct {
			maxWorkers int64
		}
		var statsMu sync.Mutex
		var allRunStats []*runStats

		outerJobs := make([]int, 5)
		for i := range outerJobs {
			outerJobs[i] = i
		}

		innerJobs := make([]int, 10)
		for i := range innerJobs {
			innerJobs[i] = i
		}

		process := func(ctx context.Context, outerJob int) (int, error) {
			// Reduce the limit for inner operations
			innerCtx := concurrent.WithLimit(ctx, innerLimit)

			// Track workers for this specific inner Run
			stats := &runStats{}
			statsMu.Lock()
			allRunStats = append(allRunStats, stats)
			statsMu.Unlock()

			var activeWorkers int64

			sum := 0
			for result, err := range concurrent.Run(innerCtx, innerJobs, func(ctx context.Context, innerJob int) (int, error) {
				current := atomic.AddInt64(&activeWorkers, 1)
				defer atomic.AddInt64(&activeWorkers, -1)

				// Track max for this specific Run
				for {
					max := atomic.LoadInt64(&stats.maxWorkers)
					if current <= max || atomic.CompareAndSwapInt64(&stats.maxWorkers, max, current) {
						break
					}
				}

				time.Sleep(time.Millisecond)
				return innerJob, nil
			}) {
				if err != nil {
					return 0, err
				}
				sum += result
			}
			return sum, nil
		}

		for _, err := range concurrent.Run(ctx, outerJobs, process) {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}

		// Each individual inner Run should respect its own innerLimit
		statsMu.Lock()
		defer statsMu.Unlock()
		for i, stats := range allRunStats {
			if stats.maxWorkers > int64(innerLimit) {
				t.Errorf("inner Run %d exceeded limit: expected max %d, observed %d", i, innerLimit, stats.maxWorkers)
			}
		}
	})
}
