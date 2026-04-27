package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"
)

type expectedErr struct{}

func (expectedErr) Error() string  { return "expected" }
func (expectedErr) Expected() bool { return true }

type temporaryErr struct{}

func (temporaryErr) Error() string   { return "temporary" }
func (temporaryErr) Temporary() bool { return true }

func TestLogLevelOf(t *testing.T) {
	cancelCause := errors.New("caller canceled")
	timeoutCause := errors.New("caller timeout")

	cancelCauseCtx, cancelWithCause := context.WithCancelCause(context.Background())
	cancelWithCause(cancelCause)

	timeoutCtx, cancelTimeoutCtx := context.WithTimeout(context.Background(), time.Nanosecond)
	t.Cleanup(cancelTimeoutCtx)
	<-timeoutCtx.Done()

	timeoutCauseCtx, cancelTimeout := context.WithTimeoutCause(context.Background(), time.Nanosecond, timeoutCause)
	t.Cleanup(cancelTimeout)
	<-timeoutCauseCtx.Done()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want slog.Level
	}{
		{"context.Canceled", context.Background(), context.Canceled, slog.LevelDebug},
		{"wrapped context.Canceled", context.Background(), fmt.Errorf("get object: %w", context.Canceled), slog.LevelDebug},
		{"context.Cause from WithCancelCause", cancelCauseCtx, cancelCause, slog.LevelDebug},
		{"wrapped context.Cause from WithCancelCause", cancelCauseCtx, fmt.Errorf("get object: %w", cancelCause), slog.LevelDebug},
		{"context.Cause from WithTimeoutCause", timeoutCauseCtx, timeoutCause, slog.LevelDebug},
		// context.DeadlineExceeded implements Temporary(), so it's classified
		// as a warning by the temporary branch — not demoted to debug. This
		// leaves server-timeout firings visible while hiding caller-driven
		// cancellations.
		{"context.DeadlineExceeded not demoted", context.Background(), context.DeadlineExceeded, slog.LevelWarn},
		{"context.DeadlineExceeded cause without explicit timeout cause", timeoutCtx, context.DeadlineExceeded, slog.LevelWarn},
		{"Expected error", context.Background(), expectedErr{}, slog.LevelDebug},
		{"Temporary error", context.Background(), temporaryErr{}, slog.LevelWarn},
		{"unknown error", context.Background(), errors.New("boom"), slog.LevelError},
		{"io.EOF", context.Background(), io.EOF, slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logLevelOf(tt.ctx, tt.err); got != tt.want {
				t.Errorf("logLevelOf(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
