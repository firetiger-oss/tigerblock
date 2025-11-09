package concurrent

import (
	"context"
	"iter"
	"sync"
)

// Pipeline provides concurrent processing of iterator sequences with a transform function.
//
// This function takes an input iterator sequence and applies a transformation function
// to each element concurrently, respecting the context's concurrency limits. It maintains
// ordering and provides proper error handling and cancellation support.
//
// Type Parameters:
//   - Out: The output type after transformation
//   - In: The input type before transformation
//
// Parameters:
//   - ctx: Context that controls concurrency limits and cancellation
//   - seq: Input iterator sequence of In and error pairs
//   - transform: Function that transforms input items to output items
//
// Returns:
//
//	An iterator sequence that yields Out and error pairs, maintaining the
//	order of the input sequence while processing items concurrently.
//
// The function:
//   - Respects context concurrency limits from concurrent.Limit(ctx)
//   - Maintains proper ordering of results
//   - Handles context cancellation gracefully
//   - Uses goroutines for concurrent processing
//   - Propagates errors from both input sequence and transform function
func Pipeline[Out, In any](
	ctx context.Context,
	seq iter.Seq2[In, error],
	transform func(context.Context, In) (Out, error),
) iter.Seq2[Out, error] {
	return func(yield func(Out, error) bool) {
		type result struct {
			out Out
			err error
		}

		type promise chan result
		promises := make(chan promise)
		semaphore := make(chan struct{}, Limit(ctx))

		ctx, cancel := context.WithCancel(ctx)
		go func() {
			waitGroup := new(sync.WaitGroup)
			defer close(promises)
			defer waitGroup.Wait()

			for in, err := range seq {
				resolve := make(promise, 1)
				promises <- resolve

				if err != nil {
					resolve <- result{err: err}
					return
				}

				select {
				case semaphore <- struct{}{}:
				case <-ctx.Done():
					resolve <- result{err: context.Cause(ctx)}
					return
				}

				waitGroup.Add(1)
				go func() {
					defer waitGroup.Done()
					defer func() { <-semaphore }()

					out, err := transform(ctx, in)
					resolve <- result{out: out, err: err}
				}()
			}
		}()

		defer func() {
			cancel()
			for range promises {
			}
		}()

		for p := range promises {
			r := <-p
			if !yield(r.out, r.err) {
				return
			}
		}
	}
}

// Exec executes multiple tasks concurrently and returns an iterator of errors.
//
// This is a special case of Pipeline where each task is a simple function that
// returns only an error. All tasks are executed concurrently up to the context's
// concurrency limit.
//
// Parameters:
//   - ctx: Context that controls concurrency limits and cancellation
//   - tasks: Variable number of functions to execute concurrently
//
// Returns:
//
//	An iterator sequence that yields error values, one for each task.
//	The order of errors corresponds to the order of tasks provided.
//
// Example:
//
//	for err := range Exec(ctx, task1, task2, task3) {
//	    if err != nil {
//	        // handle error
//	    }
//	}
func Exec(ctx context.Context, tasks ...func(context.Context) error) iter.Seq[error] {
	return func(yield func(error) bool) {
		for _, err := range Pipeline(ctx,
			func(yield func(func(context.Context) error, error) bool) {
				for _, task := range tasks {
					if !yield(task, nil) {
						return
					}
				}
			},
			func(ctx context.Context, task func(context.Context) error) (struct{}, error) {
				return struct{}{}, task(ctx)
			},
		) {
			if !yield(err) {
				return
			}
		}
	}
}

// Query executes multiple query tasks concurrently and returns an iterator of results and errors.
//
// This is a special case of Pipeline where each task is a function that returns
// a result and an error. All tasks are executed concurrently up to the context's
// concurrency limit.
//
// Type Parameters:
//   - R: The result type returned by each task
//
// Parameters:
//   - ctx: Context that controls concurrency limits and cancellation
//   - tasks: Variable number of query functions to execute concurrently
//
// Returns:
//
//	An iterator sequence that yields result and error pairs, one for each task.
//	The order of results corresponds to the order of tasks provided.
//
// Example:
//
//	for result, err := range Query(ctx, query1, query2, query3) {
//	    if err != nil {
//	        // handle error
//	    } else {
//	        // use result
//	    }
//	}
func Query[R any](ctx context.Context, tasks ...func(context.Context) (R, error)) iter.Seq2[R, error] {
	return Pipeline(ctx,
		func(yield func(func(context.Context) (R, error), error) bool) {
			for _, task := range tasks {
				if !yield(task, nil) {
					return
				}
			}
		},
		func(ctx context.Context, task func(context.Context) (R, error)) (R, error) {
			return task(ctx)
		},
	)
}
