package oteltrace

import (
	"context"
	"errors"
	"io"
	"iter"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func Tracer() trace.Tracer { return otel.Tracer("github.com/firetiger-oss/tigerblock/storage") }

func Start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx, span := Tracer().Start(ctx, name, trace.WithSpanKind(trace.SpanKindClient))
	span.SetAttributes(attrs...)
	return ctx, span
}

func RecordError(span trace.Span, err error) {
	if err != nil && err != io.EOF {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func RecordSeq[T any](span trace.Span, name string, items iter.Seq2[T, error]) iter.Seq2[T, error] {
	return recordSeq(span, name, items, func(T) int64 { return 1 })
}

func RecordSeqSlice[T any](span trace.Span, name string, chunks iter.Seq2[[]T, error]) iter.Seq2[[]T, error] {
	return recordSeq(span, name, chunks, func(chunk []T) int64 { return int64(len(chunk)) })
}

func recordSeq[T any](span trace.Span, name string, items iter.Seq2[T, error], count func(T) int64) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var total int64

		defer func() {
			span.SetAttributes(attribute.Int64(name, total))
		}()

		for item, err := range items {
			total += count(item)
			RecordError(span, err)
			if !yield(item, err) {
				return
			}
		}
	}
}

// Create span-closing wrappers to properly end the span once iterating/reading is done

type readCloser struct {
	reader io.ReadCloser
	span   trace.Span
	once   sync.Once
	count  string
	bytes  int64
}

// Wraps a reader so that span.End() is called once done reading
func ReadCloser(r io.ReadCloser, s trace.Span, count string) io.ReadCloser {
	return &readCloser{reader: r, span: s, count: count}
}

func (tr *readCloser) Read(p []byte) (int, error) {
	n, err := tr.reader.Read(p)
	tr.bytes += int64(n)
	if err != nil && !errors.Is(err, io.EOF) {
		RecordError(tr.span, err)
	}
	return n, err
}

func (tr *readCloser) Close() error {
	defer tr.once.Do(func() {
		tr.span.SetAttributes(attribute.Int64(tr.count, tr.bytes))
		tr.span.End()
	})
	return tr.reader.Close()
}
