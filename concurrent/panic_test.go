package concurrent

import (
	"testing"
)

func TestRecoverAndRethrow(t *testing.T) {
	t.Run("captures panic value", func(t *testing.T) {
		var captured any

		func() {
			defer recoverAndRethrow(func(v any) {
				captured = v
			})
			panic("test panic")
		}()

		if captured == nil {
			t.Fatal("expected panic to be captured")
		}
		if captured != "test panic" {
			t.Errorf("captured value = %v, want %q", captured, "test panic")
		}
	})

	t.Run("captures non-string panic value", func(t *testing.T) {
		var captured any

		func() {
			defer recoverAndRethrow(func(v any) {
				captured = v
			})
			panic(42)
		}()

		if captured == nil {
			t.Fatal("expected panic to be captured")
		}
		if captured != 42 {
			t.Errorf("captured value = %v, want 42", captured)
		}
	})

	t.Run("does not call handler when no panic", func(t *testing.T) {
		called := false

		func() {
			defer recoverAndRethrow(func(v any) {
				called = true
			})
			// No panic here
		}()

		if called {
			t.Error("handler should not be called when no panic occurs")
		}
	})
}
