// Package sequtil provides utility functions for working with sequences.
// It exposes APIs similar to those in the standard slices package,
// but for iterators with two type parameters, with the second being an error,
// i.e., iter.Seq2[T, error].
package sequtil

import (
	"iter"
	"sync"
)

// Collect consumes an iterator of [T, error], producing a []T, error. It
// stops reading on the first error it encounters.
func Collect[T any](seq iter.Seq2[T, error]) ([]T, error) {
	var slice []T
	for elem, err := range seq {
		if err != nil {
			return slice, err
		}
		slice = append(slice, elem)
	}
	return slice, nil
}

func Limit[T any](seq iter.Seq2[T, error], limit int) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		remain := limit
		for v, err := range seq {
			if err != nil {
				yield(v, err)
				return
			}
			if !yield(v, nil) {
				return
			}
			if remain--; remain == 0 {
				return
			}
		}
	}
}

func Transform[T any](seq iter.Seq2[T, error], transform func(T) (T, error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for v, err := range seq {
			if err == nil {
				v, err = transform(v)
			}
			if !yield(v, err) {
				return
			}
		}
	}
}

// Values creates an iterator from a slice, yielding each element with nil error.
func Values[T any](slice []T) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		for _, v := range slice {
			if !yield(v, nil) {
				return
			}
		}
	}
}

// Chunk groups elements from the input sequence into chunks of the specified size.
// It yields slices of elements until the input is exhausted or an error occurs.
func Chunk[T any](seq iter.Seq2[T, error], chunkSize int) iter.Seq2[[]T, error] {
	return func(yield func([]T, error) bool) {
		chunk := make([]T, 0, chunkSize)
		for v, err := range seq {
			if err != nil {
				if len(chunk) > 0 {
					if !yield(chunk, nil) {
						return
					}
				}
				yield(nil, err)
				return
			}
			chunk = append(chunk, v)
			if len(chunk) >= chunkSize {
				if !yield(chunk, nil) {
					return
				}
				chunk = make([]T, 0, chunkSize)
			}
		}
		if len(chunk) > 0 {
			yield(chunk, nil)
		}
	}
}

// Merge combines multiple sequences into a single sequence.
// Elements are yielded as they arrive from any of the input sequences.
// This function consumes all sequences concurrently and merges their outputs.
func Merge[T any](seqs ...iter.Seq2[T, error]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		if len(seqs) == 0 {
			return
		}

		if len(seqs) == 1 {
			seqs[0](yield)
			return
		}

		done := make(chan struct{})
		resch := make(chan T)
		errch := make(chan error)
		group := new(sync.WaitGroup)
		group.Add(len(seqs))

		for _, seq := range seqs {
			go func() {
				defer group.Done()
				for res, err := range seq {
					if err != nil {
						select {
						case errch <- err:
						case <-done:
							return
						}
					} else {
						select {
						case resch <- res:
						case <-done:
							return
						}
					}
				}
			}()
		}

		go func() {
			group.Wait()
			close(resch)
			close(errch)
		}()

		defer func() {
			close(done)
			for range resch {
			}
			for range errch {
			}
		}()

		for {
			var res T
			var err error
			var ok bool
			select {
			case res, ok = <-resch:
			case err, ok = <-errch:
			}
			if !ok {
				return
			}
			if !yield(res, err) {
				return
			}
		}
	}
}
