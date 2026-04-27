package backoff

import (
	"context"
	"testing"
	"time"
)

func TestContextWithMinDelay(t *testing.T) {
	minDelay := 500 * time.Millisecond

	ctxWithDelay := ContextWithMinDelay(t.Context(), minDelay)
	result := MinDelayFromContext(ctxWithDelay)

	if result != minDelay {
		t.Errorf("expected MinDelay %v, got %v", minDelay, result)
	}
}

func TestContextWithMaxDelay(t *testing.T) {
	maxDelay := 15 * time.Second

	ctxWithDelay := ContextWithMaxDelay(t.Context(), maxDelay)
	result := MaxDelayFromContext(ctxWithDelay)

	if result != maxDelay {
		t.Errorf("expected MaxDelay %v, got %v", maxDelay, result)
	}
}

func TestContextWithStrategy(t *testing.T) {
	strategy := Exponential()

	ctxWithStrategy := ContextWithStrategy(t.Context(), strategy)
	result := StrategyFromContext(ctxWithStrategy)

	// Test that the strategy works correctly
	minDelay := time.Second
	maxDelay := 10 * time.Second

	originalDelay := strategy.Backoff(1, minDelay, maxDelay)
	resultDelay := result.Backoff(1, minDelay, maxDelay)

	if originalDelay != resultDelay {
		t.Errorf("expected strategy to return %v, got %v", originalDelay, resultDelay)
	}
}

func TestMinDelayFromContextDefaults(t *testing.T) {
	defaultMinDelay := 250 * time.Millisecond

	result := MinDelayFromContext(t.Context())

	if result != defaultMinDelay {
		t.Errorf("expected default MinDelay %v, got %v", defaultMinDelay, result)
	}
}

func TestMaxDelayFromContextDefaults(t *testing.T) {
	defaultMaxDelay := 8 * time.Second

	result := MaxDelayFromContext(t.Context())

	if result != defaultMaxDelay {
		t.Errorf("expected default MaxDelay %v, got %v", defaultMaxDelay, result)
	}
}

func TestStrategyFromContextDefaults(t *testing.T) {
	result := StrategyFromContext(t.Context())

	// The default should be FullJitter(Exponential())
	// We can test that it returns a valid strategy by checking it returns reasonable delays
	minDelay := time.Second
	maxDelay := 10 * time.Second

	delay := result.Backoff(1, minDelay, maxDelay)

	if delay < 0 || delay > maxDelay {
		t.Errorf("default strategy returned invalid delay: %v", delay)
	}
}

func TestContextChaining(t *testing.T) {
	minDelay := 100 * time.Millisecond
	maxDelay := 5 * time.Second
	strategy := Exponential()

	// Chain all context modifications
	ctxWithAll := ContextWithStrategy(
		ContextWithMaxDelay(
			ContextWithMinDelay(t.Context(), minDelay),
			maxDelay,
		),
		strategy,
	)

	// Verify all values are preserved
	if MinDelayFromContext(ctxWithAll) != minDelay {
		t.Errorf("MinDelay not preserved in chain")
	}

	if MaxDelayFromContext(ctxWithAll) != maxDelay {
		t.Errorf("MaxDelay not preserved in chain")
	}

	resultStrategy := StrategyFromContext(ctxWithAll)
	originalDelay := strategy.Backoff(2, minDelay, maxDelay)
	resultDelay := resultStrategy.Backoff(2, minDelay, maxDelay)

	if originalDelay != resultDelay {
		t.Errorf("Strategy not preserved in chain: expected %v, got %v", originalDelay, resultDelay)
	}
}

func TestContextOverride(t *testing.T) {
	// Set initial values
	ctx1 := ContextWithMinDelay(t.Context(), time.Second)
	ctx2 := ContextWithMinDelay(ctx1, 2*time.Second)

	// The second value should override the first
	result := MinDelayFromContext(ctx2)
	expected := 2 * time.Second

	if result != expected {
		t.Errorf("expected override to work: expected %v, got %v", expected, result)
	}
}

func TestContextWithWrongType(t *testing.T) {
	// Test that functions return defaults when context values have wrong types
	// Since we're in the same package, we can use the actual context keys
	ctx := context.WithValue(t.Context(), contextKeyMinDelay{}, "not a duration")
	ctx = context.WithValue(ctx, contextKeyMaxDelay{}, 123) // int instead of duration
	ctx = context.WithValue(ctx, contextKeyStrategy{}, "not a strategy")

	// Should return default values when types don't match
	minDelay := MinDelayFromContext(ctx)
	maxDelay := MaxDelayFromContext(ctx)
	strategy := StrategyFromContext(ctx)

	expectedMin := DefaultMinDelay
	expectedMax := DefaultMaxDelay

	if minDelay != expectedMin {
		t.Errorf("expected default MinDelay %v when type is wrong, got %v", expectedMin, minDelay)
	}

	if maxDelay != expectedMax {
		t.Errorf("expected default MaxDelay %v when type is wrong, got %v", expectedMax, maxDelay)
	}

	if strategy == nil {
		t.Error("expected default strategy when type is wrong, got nil")
	}

	// Verify the default strategy works
	delay := strategy.Backoff(1, expectedMin, expectedMax)
	if delay < 0 || delay > expectedMax {
		t.Errorf("default strategy returned invalid delay: %v", delay)
	}
}
