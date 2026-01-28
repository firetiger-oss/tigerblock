// Package concurrent provides thread-safe job queue implementations for concurrent processing.
// It supports generic job types and provides mechanisms for controlling concurrency,
// capacity limits, and graceful shutdown.
package concurrent

import (
	"context"
	"errors"
	"iter"
	"sync"
)

// DefaultQueueCapacity is the default maximum number of jobs that can be queued
// when using NewQueue. This provides a reasonable default for most use cases
// while preventing unbounded memory growth.
const DefaultQueueCapacity = 100_000

// ErrFull is returned when a job queue has reached its capacity limit.
// When the queue is full, additional jobs will be replaced with error-emitting
// jobs that yield this error when processed.
var ErrFull = errors.New("job queue is full")

// Job represents a generic job function that processes data of type T.
// The job receives a context for cancellation and a yield function to emit
// results. The yield function returns true if the caller wants more results,
// or false to stop processing.
//
// Jobs can yield multiple values by calling the yield function multiple times.
// They should respect context cancellation and stop processing when the
// context is done.
type Job[T any] func(context.Context, func(T, error) bool)

// Queue is a thread-safe, bounded queue for processing jobs concurrently.
// It supports generic job types and provides capacity limits, graceful shutdown,
// and synchronization primitives for coordinating between producers and consumers.
//
// The queue uses a condition variable to efficiently block consumers when empty
// and wake them when jobs become available. It maintains a wait group to track
// outstanding jobs for synchronization.
//
// When the queue reaches its capacity limit, additional jobs are dropped and
// replaced with error-emitting jobs that yield ErrFull when processed.
//
// Type parameter T represents the type of values that jobs will produce.
type Queue[T any] struct {
	group sync.WaitGroup // tracks outstanding jobs for Wait()
	mutex sync.Mutex     // protects access to jobs and cap
	cond  sync.Cond      // signals when jobs become available
	jobs  []Job[T]       // slice of pending jobs
	cap   int            // maximum queue capacity, negative means closed
}

// NewQueue creates a new job queue with the default capacity.
// The queue will accept up to DefaultQueueCapacity jobs before dropping
// additional jobs and replacing them with error-emitting jobs that yield ErrFull.
//
// Type parameter T specifies the type of values that jobs will produce.
func NewQueue[T any]() *Queue[T] {
	return NewQueueWithCapacity[T](DefaultQueueCapacity)
}

// NewQueueWithCapacity creates a new job queue with the specified capacity.
// The capacity determines the maximum number of jobs that can be queued
// before additional jobs are dropped.
//
// When the queue reaches capacity, additional jobs are dropped and replaced
// with error-emitting jobs that yield ErrFull when processed.
//
// Type parameter T specifies the type of values that jobs will produce.
func NewQueueWithCapacity[T any](capacity int) *Queue[T] {
	q := &Queue[T]{cap: capacity}
	q.cond.L = &q.mutex
	return q
}

// Push adds a job to the queue for processing.
//
// If the queue has reached its capacity, the job will be dropped and replaced
// with an error-emitting job that yields ErrFull when processed. This prevents
// unbounded memory growth without blocking the caller.
//
// Push panics if called on a queue that has been marked as done via Done().
// This prevents adding jobs to a queue that consumers have stopped processing.
//
// Push is safe for concurrent use and will wake up blocked consumers.
func (q *Queue[T]) Push(job Job[T]) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.cap < 0 {
		panic("push called on queue that is not accepting any more jobs")
	}
	if len(q.jobs) > q.cap {
		return
	}
	if len(q.jobs) == q.cap {
		// replace job with one that will emit the error
		job = func(ctx context.Context, yield func(T, error) bool) {
			yield(*new(T), ErrFull)
		}
	}

	q.jobs = append(q.jobs, job)
	q.cond.Signal()
	q.group.Add(1)
}

// Pull returns an iterator that yields jobs from the queue in FIFO order.
//
// The iterator blocks when the queue is empty and waits for new jobs to be
// pushed. It terminates when Done() is called and all remaining jobs have
// been consumed.
//
// Each job yielded by the iterator is automatically marked as done in the
// wait group when the iterator advances, enabling Wait() to track completion.
//
// Pull is safe for concurrent use by multiple consumers. Jobs are distributed
// among consumers in a round-robin fashion.
//
// The iterator can be stopped early by returning false from the yield function.
func (q *Queue[T]) Pull() iter.Seq[Job[T]] {
	return func(yield func(Job[T]) bool) {
		for {
			q.mutex.Lock()
			for len(q.jobs) == 0 && q.cap >= 0 {
				q.cond.Wait()
			}
			if len(q.jobs) == 0 {
				q.mutex.Unlock()
				return
			}
			job := q.jobs[0]
			q.jobs[0] = nil
			q.jobs = q.jobs[1:]
			q.mutex.Unlock()

			ack := yield(job)
			q.group.Done()

			if !ack {
				return
			}
		}
	}
}

// Wait blocks until all jobs that have been pushed to the queue have been
// processed by consumers via Pull().
//
// Wait tracks job completion through an internal wait group that is incremented
// when jobs are pushed and decremented when jobs are consumed from Pull().
//
// This method is useful for ensuring all work has completed before proceeding
// or shutting down the application.
func (q *Queue[T]) Wait() {
	q.group.Wait()
}

// Flush removes all pending jobs from the queue without processing them.
//
// All flushed jobs are marked as done in the wait group, allowing Wait()
// to proceed without waiting for the flushed jobs to be processed.
//
// This method is useful for graceful shutdown scenarios where you want to
// stop processing new work but still wait for already-started jobs to complete.
//
// Flush is safe for concurrent use.
func (q *Queue[T]) Flush() {
	q.mutex.Lock()
	for range len(q.jobs) {
		q.group.Done()
	}
	q.jobs = nil
	q.mutex.Unlock()
}

// Done marks the queue as closed for new jobs and signals all blocked consumers
// to stop pulling jobs once the queue is empty.
//
// After Done() is called:
// - Push() will panic if called
// - Pull() will terminate once all remaining jobs are consumed
// - New Pull() calls will return immediately if the queue is empty
//
// Done is safe for concurrent use and is typically called by producers to
// signal that no more work will be submitted.
func (q *Queue[T]) Done() {
	q.mutex.Lock()
	q.cap = -1
	q.cond.Broadcast()
	q.mutex.Unlock()
}

// Process executes jobs from the queue concurrently and returns an iterator
// of their results.
//
// The function creates a pool of worker goroutines (controlled by the context's
// concurrency limit) that pull jobs from the queue and execute them. Results
// are collected and yielded through the returned iterator.
//
// The concurrency level is determined by Limit(ctx). Workers respect context
// cancellation and will stop processing when the context is cancelled.
//
// The iterator terminates when:
// - All jobs in the queue have been processed
// - The context is cancelled
// - The consumer stops the iterator by returning false
//
// Process automatically calls queue.Done() when all jobs have been consumed,
// preventing new jobs from being added and allowing graceful shutdown.
//
// Type parameter T represents the type of values that jobs produce.
func Process[T any](ctx context.Context, queue *Queue[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		type item struct {
			val   T
			err   error
			panic any
		}

		items := make(chan item)
		group := new(sync.WaitGroup)
		defer group.Wait()

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		for range Limit(ctx) {
			group.Add(1)
			go func() {
				defer group.Done()

				for job := range queue.Pull() {
					func() {
						defer recoverAndRethrow(func(v any) {
							select {
							case items <- item{panic: v}:
							case <-ctx.Done():
							}
						})

						job(ctx, func(val T, err error) bool {
							select {
							case items <- item{val: val, err: err}:
								return true
							case <-ctx.Done():
								return false
							}
						})
					}()
				}
			}()
		}

		go func() {
			queue.Wait()
			queue.Done()
			group.Wait()
			close(items)
		}()

		for item := range items {
			if item.panic != nil {
				panic(item.panic)
			}
			if !yield(item.val, item.err) {
				return
			}
		}
	}
}

// Run is a convenience function that creates a queue, adds jobs for each
// input item, and processes them concurrently.
// For each item in the jobs slice, the process function is called to create
// a job. All jobs are then executed concurrently.
// This is equivalent to:
//
//	queue := NewQueueWithCapacity(len(jobs))
//	for _, job := range jobs {
//	    queue.Push(func(ctx context.Context, yield func(R, error) bool) {
//	        return yield(process(ctx, job))
//	    })
//	}
//	return Process(ctx, queue)
func Run[R, T any](ctx context.Context, jobs []T, process func(context.Context, T) (R, error)) iter.Seq2[R, error] {
	queue := NewQueueWithCapacity[R](len(jobs))
	for _, job := range jobs {
		queue.Push(func(ctx context.Context, yield func(R, error) bool) {
			yield(process(ctx, job))
		})
	}
	return Process(ctx, queue)
}

// Run2 is like Run but it takes its input jobs as a map.
// For each key-value pair in the jobs map, the process function is called to create
// a job. All jobs are then executed concurrently.
// This is equivalent to:
//
//	queue := NewQueueWithCapacity[R](len(jobs))
//	for key, value := range jobs {
//	    queue.Push(func(ctx context.Context, yield func(R, error) bool) {
//	        return yield(process(ctx, key, value))
//	    })
//	}
//	return Process(ctx, queue)
func Run2[R any, K comparable, V any](ctx context.Context, jobs map[K]V, process func(context.Context, K, V) (R, error)) iter.Seq2[R, error] {
	queue := NewQueueWithCapacity[R](len(jobs))
	for key, value := range jobs {
		queue.Push(func(ctx context.Context, yield func(R, error) bool) {
			yield(process(ctx, key, value))
		})
	}
	return Process(ctx, queue)
}

// Task represents a simple function that performs work and may return an error.
// Tasks are used with TaskQueue for scenarios where you only care about
// success/failure rather than producing specific result values.
//
// Tasks should respect context cancellation and return promptly when the
// context is cancelled.
type Task func(context.Context) error

// TaskQueue is a specialized queue for processing Task functions.
// It's a convenience wrapper around Queue[struct{}] for scenarios where
// you only need to track success/failure of operations rather than
// collecting specific result values.
type TaskQueue Queue[struct{}]

// Queue returns the underlying generic queue that backs this TaskQueue.
// This allows access to the full Queue API when needed.
func (q *TaskQueue) Queue() *Queue[struct{}] {
	return (*Queue[struct{}])(q)
}

// Push adds a task to the queue for processing.
// The task will be executed by workers created via ProcessTasks().
//
// Tasks are converted to jobs that yield empty struct{} values on success
// or error values on failure.
func (q *TaskQueue) Push(task Task) {
	q.Queue().Push(func(ctx context.Context, yield func(struct{}, error) bool) {
		yield(struct{}{}, task(ctx))
	})
}

// NewTaskQueue creates a new task queue with the default capacity.
// Task queues are optimized for scenarios where you only need to track
// success/failure of operations.
func NewTaskQueue() *TaskQueue {
	return (*TaskQueue)(NewQueue[struct{}]())
}

// NewTaskQueueWithCapacity creates a new task queue with the specified capacity.
// The capacity determines how many tasks can be queued before additional
// tasks are dropped and ErrFull is emitted.
func NewTaskQueueWithCapacity(capacity int) *TaskQueue {
	return (*TaskQueue)(NewQueueWithCapacity[struct{}](capacity))
}

// ProcessTasks executes all tasks in the queue concurrently and returns the
// first error encountered, if any.
//
// Tasks are processed by a pool of workers (controlled by the context's
// concurrency limit). If any task returns an error, processing stops and
// that error is returned.
//
// This function is useful for scenarios where you want fail-fast behavior
// and don't need to collect result values from successful tasks.
func ProcessTasks(ctx context.Context, tasks *TaskQueue) error {
	for _, err := range Process(ctx, tasks.Queue()) {
		if err != nil {
			return err
		}
	}
	return nil
}

// RunTasks is a convenience function that creates a task queue, adds tasks
// for each input item, and processes them concurrently.
//
// For each item in the tasks slice, the process function is called to create
// a task. All tasks are then executed concurrently with fail-fast behavior.
//
// This is equivalent to:
//
//	queue := NewTaskQueueWithCapacity(len(tasks))
//	for _, task := range tasks {
//	    queue.Push(func(ctx context.Context) error {
//	        return process(ctx, task)
//	    })
//	}
//	return ProcessTasks(ctx, queue)
//
// Type parameter T represents the type of input items to process.
func RunTasks[T any](ctx context.Context, tasks []T, process func(context.Context, T) error) error {
	queue := NewTaskQueueWithCapacity(len(tasks))
	for _, task := range tasks {
		queue.Push(func(ctx context.Context) error {
			return process(ctx, task)
		})
	}
	return ProcessTasks(ctx, queue)
}

// RunTasks2 is like RunTasks but it takes its input tasks as a map.
func RunTasks2[K comparable, V any](ctx context.Context, tasks map[K]V, process func(context.Context, K, V) error) error {
	queue := NewTaskQueueWithCapacity(len(tasks))
	for key, value := range tasks {
		queue.Push(func(ctx context.Context) error {
			return process(ctx, key, value)
		})
	}
	return ProcessTasks(ctx, queue)
}
