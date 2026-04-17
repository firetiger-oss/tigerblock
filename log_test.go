package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
)

type expectedErr struct{}

func (expectedErr) Error() string  { return "expected" }
func (expectedErr) Expected() bool { return true }

type temporaryErr struct{}

func (temporaryErr) Error() string   { return "temporary" }
func (temporaryErr) Temporary() bool { return true }

func TestLogLevelOf(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want slog.Level
	}{
		{"context.Canceled", context.Canceled, slog.LevelDebug},
		{"wrapped context.Canceled", fmt.Errorf("get object: %w", context.Canceled), slog.LevelDebug},
		// context.DeadlineExceeded implements Temporary(), so it's classified
		// as a warning by the temporary branch — not demoted to debug. This
		// leaves server-timeout firings visible while hiding caller-driven
		// cancellations.
		{"context.DeadlineExceeded not demoted", context.DeadlineExceeded, slog.LevelWarn},
		{"Expected error", expectedErr{}, slog.LevelDebug},
		{"Temporary error", temporaryErr{}, slog.LevelWarn},
		{"unknown error", errors.New("boom"), slog.LevelError},
		{"io.EOF", io.EOF, slog.LevelError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := logLevelOf(tt.err); got != tt.want {
				t.Errorf("logLevelOf(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}
