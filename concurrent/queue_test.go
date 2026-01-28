package concurrent

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	q := NewQueue[int]()
	if q == nil {
		t.Fatal("NewQueue returned nil")
	}
	if q.cap != DefaultQueueCapacity {
		t.Errorf("expected capacity %d, got %d", DefaultQueueCapacity, q.cap)
	}
}

func TestNewQueueWithCapacity(t *testing.T) {
	capacity := 50
	q := NewQueueWithCapacity[string](capacity)
	if q == nil {
		t.Fatal("NewQueueWithCapacity returned nil")
	}
	if q.cap != capacity {
		t.Errorf("expected capacity %d, got %d", capacity, q.cap)
	}
}

func TestQueuePushAndPull(t *testing.T) {
	q := NewQueueWithCapacity[int](3)

	// Test pushing jobs
	var results []int
	var mu sync.Mutex

	job1 := func(ctx context.Context, yield func(int, error) bool) {
		mu.Lock()
		results = append(results, 1)
		mu.Unlock()
		yield(1, nil)
	}

	job2 := func(ctx context.Context, yield func(int, error) bool) {
		mu.Lock()
		results = append(results, 2)
		mu.Unlock()
		yield(2, nil)
	}

	q.Push(job1)
	q.Push(job2)

	// Test pulling jobs
	var pulled []Job[int]
	for job := range q.Pull() {
		pulled = append(pulled, job)
		if len(pulled) == 2 {
			break
		}
	}

	if len(pulled) != 2 {
		t.Errorf("expected 2 jobs pulled, got %d", len(pulled))
	}

	// Execute the pulled jobs
	for _, job := range pulled {
		job(t.Context(), func(val int, err error) bool {
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			return true
		})
	}

	mu.Lock()
	if len(results) != 2 {
		t.Errorf("expected 2 results, got %d", len(results))
	}
	mu.Unlock()
}

func TestQueueCapacityLimit(t *testing.T) {
	q := NewQueueWithCapacity[int](2)

	// Add jobs up to capacity
	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(1, nil)
	})
	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(2, nil)
	})

	// This should replace the job with an error-emitting one since len(queue) == cap
	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(3, nil)
	})

	// Pull and execute jobs - the third one should be an error job
	var errorReceived bool
	var jobCount int
	for job := range q.Pull() {
		job(t.Context(), func(val int, err error) bool {
			if errors.Is(err, ErrFull) {
				errorReceived = true
			}
			return true
		})
		jobCount++
		if jobCount == 3 {
			break
		}
	}

	if !errorReceived {
		t.Error("expected to receive ErrFull when capacity is exceeded")
	}
}

func TestQueuePushAfterDone(t *testing.T) {
	q := NewQueueWithCapacity[int](2)

	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(1, nil)
	})

	q.Done()

	// Pushing after Done should panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when pushing after Done")
		}
	}()

	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(2, nil)
	})
}

func TestQueueWait(t *testing.T) {
	q := NewQueueWithCapacity[int](5)

	var completed int32

	// Add jobs
	for range 3 {
		q.Push(func(ctx context.Context, yield func(int, error) bool) {
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&completed, 1)
			yield(1, nil)
		})
	}

	// Start processing jobs in goroutine
	go func() {
		jobCount := 0
		for job := range q.Pull() {
			job(context.Background(), func(val int, err error) bool {
				return true
			})
			jobCount++
			if jobCount == 3 {
				break
			}
		}
	}()

	// Wait should block until all jobs are done (marked done by Pull)
	q.Wait()

	if atomic.LoadInt32(&completed) != 3 {
		t.Errorf("expected 3 completed jobs, got %d", atomic.LoadInt32(&completed))
	}
}

func TestQueueFlush(t *testing.T) {
	q := NewQueueWithCapacity[int](5)

	// Add some jobs
	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(1, nil)
	})
	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(2, nil)
	})

	// Flush should clear the queue and mark jobs as done
	q.Flush()

	// Queue should be empty
	if len(q.jobs) != 0 {
		t.Errorf("expected empty queue after flush, got %d jobs", len(q.jobs))
	}

	// Wait should not block since jobs were marked as done
	done := make(chan struct{})
	go func() {
		q.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - Wait completed immediately
	case <-time.After(100 * time.Millisecond):
		t.Error("Wait did not complete after flush")
	}
}

func TestQueueDone(t *testing.T) {
	q := NewQueueWithCapacity[int](5)

	// Add a job
	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(1, nil)
	})

	// Start pulling in goroutine
	var jobCount int
	done := make(chan struct{})
	go func() {
		for range q.Pull() {
			jobCount++
		}
		close(done)
	}()

	// Give the goroutine time to start
	time.Sleep(10 * time.Millisecond)

	// Call Done
	q.Done()

	// Pull should terminate
	select {
	case <-done:
		// Success
	case <-time.After(100 * time.Millisecond):
		t.Error("Pull did not terminate after Done")
	}

	if jobCount != 1 {
		t.Errorf("expected 1 job to be pulled, got %d", jobCount)
	}
}

func TestProcess(t *testing.T) {
	q := NewQueueWithCapacity[int](10)

	// Add jobs that produce values
	for i := 1; i <= 5; i++ {
		value := i
		q.Push(func(ctx context.Context, yield func(int, error) bool) {
			yield(value, nil)
		})
	}

	// Add job that produces an error
	q.Push(func(ctx context.Context, yield func(int, error) bool) {
		yield(0, errors.New("test error"))
	})

	var values []int
	var errs []error

	for val, err := range Process(t.Context(), q) {
		if err != nil {
			errs = append(errs, err)
		} else {
			values = append(values, val)
		}
	}

	if len(values) != 5 {
		t.Errorf("expected 5 values, got %d", len(values))
	}

	if len(errs) != 1 {
		t.Errorf("expected 1 error, got %d", len(errs))
	}

	if len(errs) > 0 && errs[0].Error() != "test error" {
		t.Errorf("expected 'test error', got %q", errs[0].Error())
	}
}

func TestProcessWithContextCancellation(t *testing.T) {
	q := NewQueueWithCapacity[int](5)

	// Add long-running jobs
	for i := 1; i <= 3; i++ {
		value := i
		q.Push(func(ctx context.Context, yield func(int, error) bool) {
			select {
			case <-time.After(100 * time.Millisecond):
				yield(value, nil)
			case <-ctx.Done():
				yield(0, ctx.Err())
			}
		})
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var count atomic.Int32
	go func() {
		for range Process(ctx, q) {
			if count.Add(1) == 1 {
				cancel() // Cancel after processing first item
			}
		}
	}()

	// Give it time to process
	time.Sleep(200 * time.Millisecond)

	// Should have processed at least one item before cancellation
	if count.Load() == 0 {
		t.Error("expected at least one item to be processed before cancellation")
	}
}

func TestProcessWithConcurrencyLimit(t *testing.T) {
	q := NewQueueWithCapacity[int](10)

	var running int32
	var maxRunning int32

	// Add jobs that track concurrency
	for i := 1; i <= 10; i++ {
		q.Push(func(ctx context.Context, yield func(int, error) bool) {
			current := atomic.AddInt32(&running, 1)
			defer atomic.AddInt32(&running, -1)

			// Update max running
			for {
				max := atomic.LoadInt32(&maxRunning)
				if current <= max || atomic.CompareAndSwapInt32(&maxRunning, max, current) {
					break
				}
			}

			time.Sleep(10 * time.Millisecond)
			yield(1, nil)
		})
	}

	ctx := WithLimit(t.Context(), 3)
	for range Process(ctx, q) {
		// Just consume all results
	}

	// Check that concurrency was limited
	max := atomic.LoadInt32(&maxRunning)
	if max > 3 {
		t.Errorf("expected max concurrency of 3, got %d", max)
	}
}

func TestTaskQueue(t *testing.T) {
	tq := NewTaskQueue()
	if tq == nil {
		t.Fatal("NewTaskQueue returned nil")
	}

	queue := tq.Queue()
	if queue == nil {
		t.Fatal("TaskQueue.Queue() returned nil")
	}
}

func TestTaskQueueWithCapacity(t *testing.T) {
	capacity := 25
	tq := NewTaskQueueWithCapacity(capacity)
	if tq == nil {
		t.Fatal("NewTaskQueueWithCapacity returned nil")
	}

	queue := tq.Queue()
	if queue.cap != capacity {
		t.Errorf("expected capacity %d, got %d", capacity, queue.cap)
	}
}

func TestProcessTasks(t *testing.T) {
	tq := NewTaskQueue()

	var results []int
	var mu sync.Mutex

	// Add tasks
	for i := 1; i <= 5; i++ {
		value := i
		tq.Push(func(ctx context.Context) error {
			mu.Lock()
			results = append(results, value)
			mu.Unlock()
			return nil
		})
	}

	if err := ProcessTasks(t.Context(), tq); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mu.Lock()
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}
	mu.Unlock()
}

func TestProcessTasksWithError(t *testing.T) {
	tq := NewTaskQueue()

	var executed bool

	// Add a successful task
	tq.Push(func(ctx context.Context) error {
		executed = true
		return nil
	})

	// Add a failing task
	expectedErr := errors.New("task failed")
	tq.Push(func(ctx context.Context) error {
		return expectedErr
	})

	ctx := t.Context()
	err := ProcessTasks(ctx, tq)

	if !errors.Is(err, expectedErr) {
		t.Errorf("expected error %v, got %v", expectedErr, err)
	}

	// The successful task should have been executed
	if !executed {
		t.Error("expected successful task to be executed")
	}
}

func TestRunTasks(t *testing.T) {
	tasks := []int{1, 2, 3, 4, 5}
	var results []int
	var mu sync.Mutex

	process := func(ctx context.Context, value int) error {
		mu.Lock()
		results = append(results, value*2)
		mu.Unlock()
		return nil
	}

	ctx := t.Context()
	err := RunTasks(ctx, tasks, process)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	mu.Lock()
	if len(results) != 5 {
		t.Errorf("expected 5 results, got %d", len(results))
	}

	// Check that all values were processed (order may vary due to concurrency)
	resultMap := make(map[int]bool)
	for _, r := range results {
		resultMap[r] = true
	}

	expected := []int{2, 4, 6, 8, 10}
	for _, exp := range expected {
		if !resultMap[exp] {
			t.Errorf("expected result %d not found", exp)
		}
	}
	mu.Unlock()
}

func TestRunTasksWithError(t *testing.T) {
	tasks := []int{1, 2, 3}

	process := func(ctx context.Context, value int) error {
		if value == 2 {
			return errors.New("process error")
		}
		return nil
	}

	ctx := t.Context()
	err := RunTasks(ctx, tasks, process)

	if err == nil {
		t.Error("expected error from RunTasks")
	}

	if err.Error() != "process error" {
		t.Errorf("expected 'process error', got %q", err.Error())
	}
}

func TestRunTasksEmpty(t *testing.T) {
	var tasks []int

	process := func(ctx context.Context, value int) error {
		t.Error("process should not be called for empty tasks")
		return nil
	}

	ctx := t.Context()
	err := RunTasks(ctx, tasks, process)
	if err != nil {
		t.Errorf("unexpected error for empty tasks: %v", err)
	}
}

func TestProcessPropagatesPanic(t *testing.T) {
	t.Run("propagates job panic to caller", func(t *testing.T) {
		q := NewQueueWithCapacity[int](10)

		q.Push(func(ctx context.Context, yield func(int, error) bool) {
			panic("job panic")
		})

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic to propagate to caller")
			}
			if r != "job panic" {
				t.Errorf("expected panic value 'job panic', got %v", r)
			}
		}()

		for range Process(t.Context(), q) {
		}
		t.Fatal("should not reach here")
	})

	t.Run("propagates non-string panic value", func(t *testing.T) {
		q := NewQueueWithCapacity[int](10)

		q.Push(func(ctx context.Context, yield func(int, error) bool) {
			panic(42)
		})

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic to propagate")
			}
			if r != 42 {
				t.Errorf("expected panic value 42, got %v", r)
			}
		}()

		for range Process(t.Context(), q) {
		}
		t.Fatal("should not reach here")
	})
}

func TestRunTasksPropagatesPanic(t *testing.T) {
	t.Run("propagates task panic to caller", func(t *testing.T) {
		tasks := []int{1, 2, 3}

		process := func(ctx context.Context, value int) error {
			if value == 2 {
				panic("task panic")
			}
			return nil
		}

		defer func() {
			r := recover()
			if r == nil {
				t.Fatal("expected panic to propagate to caller")
			}
			if r != "task panic" {
				t.Errorf("expected panic value 'task panic', got %v", r)
			}
		}()

		_ = RunTasks(t.Context(), tasks, process)
		t.Fatal("should not reach here")
	})
}

func TestJobType(t *testing.T) {
	// Test that Job type works with different types
	var intJob Job[int] = func(ctx context.Context, yield func(int, error) bool) {
		yield(42, nil)
	}

	var stringJob Job[string] = func(ctx context.Context, yield func(string, error) bool) {
		yield("hello", nil)
	}

	ctx := t.Context()

	// Test int job
	intJob(ctx, func(val int, err error) bool {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if val != 42 {
			t.Errorf("expected 42, got %d", val)
		}
		return true
	})

	// Test string job
	stringJob(ctx, func(val string, err error) bool {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if val != "hello" {
			t.Errorf("expected 'hello', got %q", val)
		}
		return true
	})
}

func TestConcurrentAccess(t *testing.T) {
	q := NewQueueWithCapacity[int](1000)

	var wg sync.WaitGroup
	const numProducers = 10
	const numJobsPerProducer = 100

	// Start producers
	for i := range numProducers {
		wg.Add(1)
		go func(producer int) {
			defer wg.Done()
			for j := range numJobsPerProducer {
				q.Push(func(ctx context.Context, yield func(int, error) bool) {
					yield(producer*1000+j, nil)
				})
			}
		}(i)
	}

	// Start consumer
	results := make(map[int]bool)
	var resultMu sync.Mutex
	wg.Add(1)
	go func() {
		defer wg.Done()
		count := 0
		for job := range q.Pull() {
			job(context.Background(), func(val int, err error) bool {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				resultMu.Lock()
				results[val] = true
				resultMu.Unlock()
				return true
			})
			count++
			if count == numProducers*numJobsPerProducer {
				break
			}
		}
	}()

	wg.Wait()

	// Verify all jobs were processed
	resultMu.Lock()
	if len(results) != numProducers*numJobsPerProducer {
		t.Errorf("expected %d results, got %d", numProducers*numJobsPerProducer, len(results))
	}
	resultMu.Unlock()
}

// Example demonstrates basic queue usage for processing jobs concurrently.
func ExampleQueue() {
	// Create a queue with capacity for 100 jobs
	queue := NewQueueWithCapacity[string](100)

	// Add some jobs to the queue
	queue.Push(func(ctx context.Context, yield func(string, error) bool) {
		yield("processed job 1", nil)
	})
	queue.Push(func(ctx context.Context, yield func(string, error) bool) {
		yield("processed job 2", nil)
	})

	// Signal that no more jobs will be added
	queue.Done()

	// Process jobs using the Pull iterator
	var results []string
	for job := range queue.Pull() {
		job(context.Background(), func(result string, err error) bool {
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				results = append(results, result)
			}
			return true
		})
	}

	// Sort for deterministic output since job order may vary
	sort.Strings(results)
	for _, result := range results {
		fmt.Println(result)
	}

	// Output:
	// processed job 1
	// processed job 2
}

// Example demonstrates using Process to handle jobs with concurrency control.
func ExampleProcess() {
	ctx := WithLimit(context.Background(), 2) // Limit to 2 concurrent workers
	queue := NewQueue[int]()

	// Add jobs that simulate work
	for i := 1; i <= 3; i++ {
		value := i
		queue.Push(func(ctx context.Context, yield func(int, error) bool) {
			// Simulate some work
			time.Sleep(10 * time.Millisecond)
			yield(value*10, nil)
		})
	}

	// Process all jobs and collect results
	var results []int
	for result, err := range Process(ctx, queue) {
		if err != nil {
			fmt.Printf("Error: %v\n", err)
		} else {
			results = append(results, result)
		}
	}

	// Sort for deterministic output
	sort.Ints(results)
	fmt.Printf("Results: %v\n", results)

	// Output:
	// Results: [10 20 30]
}

// Example demonstrates TaskQueue for simple success/failure operations.
func ExampleTaskQueue() {
	taskQueue := NewTaskQueue()

	// Collect task completions for deterministic output
	var completed []string
	var mu sync.Mutex

	// Add tasks that perform operations
	taskQueue.Push(func(ctx context.Context) error {
		mu.Lock()
		completed = append(completed, "Task 1 completed")
		mu.Unlock()
		return nil
	})
	taskQueue.Push(func(ctx context.Context) error {
		mu.Lock()
		completed = append(completed, "Task 2 completed")
		mu.Unlock()
		return nil
	})

	// Process all tasks
	if err := ProcessTasks(context.Background(), taskQueue); err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		// Sort for deterministic output since task order may vary
		sort.Strings(completed)
		for _, msg := range completed {
			fmt.Println(msg)
		}
		fmt.Println("All tasks completed successfully")
	}

	// Output:
	// Task 1 completed
	// Task 2 completed
	// All tasks completed successfully
}

// Example demonstrates RunTasks for processing a slice of data.
func ExampleRunTasks() {
	data := []int{1, 2, 3, 4, 5}

	// Collect results for deterministic output
	var results []string
	var mu sync.Mutex

	// Process each item, doubling the values
	err := RunTasks(context.Background(), data, func(ctx context.Context, item int) error {
		result := item * 2
		mu.Lock()
		results = append(results, fmt.Sprintf("Processed %d -> %d", item, result))
		mu.Unlock()
		return nil
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		// Sort for deterministic output since task order may vary
		sort.Strings(results)
		for _, result := range results {
			fmt.Println(result)
		}
	}

	// Output:
	// Processed 1 -> 2
	// Processed 2 -> 4
	// Processed 3 -> 6
	// Processed 4 -> 8
	// Processed 5 -> 10
}

// Example demonstrates queue capacity limits and error handling.
func ExampleQueue_capacityLimit() {
	// Create a queue with very small capacity
	queue := NewQueueWithCapacity[string](1)

	// Add jobs up to and beyond capacity
	queue.Push(func(ctx context.Context, yield func(string, error) bool) {
		yield("job 1", nil)
	})
	queue.Push(func(ctx context.Context, yield func(string, error) bool) {
		yield("job 2", nil) // This will be replaced with an error job
	})

	queue.Done()

	// Process jobs and handle capacity errors
	for job := range queue.Pull() {
		job(context.Background(), func(result string, err error) bool {
			if errors.Is(err, ErrFull) {
				fmt.Println("Queue was full")
			} else if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Printf("Result: %s\n", result)
			}
			return true
		})
	}

	// Output:
	// Result: job 1
	// Queue was full
}

// Example demonstrates graceful shutdown with queue coordination.
func ExampleQueue_shutdown() {
	queue := NewQueue[string]()

	// Collect results for deterministic output
	var results []string
	var mu sync.Mutex

	// Start a producer goroutine
	go func() {
		for i := 1; i <= 3; i++ {
			value := i // Capture loop variable
			queue.Push(func(ctx context.Context, yield func(string, error) bool) {
				yield(fmt.Sprintf("work item %d", value), nil)
			})
			time.Sleep(10 * time.Millisecond)
		}
		queue.Done() // Signal no more work
	}()

	// Process work as it arrives
	for job := range queue.Pull() {
		job(context.Background(), func(result string, err error) bool {
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				mu.Lock()
				results = append(results, result)
				mu.Unlock()
			}
			return true
		})
	}

	// Wait ensures all jobs are finished before proceeding
	queue.Wait()

	// Sort for deterministic output since job completion order may vary
	sort.Strings(results)
	for _, result := range results {
		fmt.Printf("Completed: %s\n", result)
	}
	fmt.Println("All work completed")

	// Output:
	// Completed: work item 1
	// Completed: work item 2
	// Completed: work item 3
	// All work completed
}
