package concurrent

// recoverAndRethrow captures a panic and stores its value via the handler.
// The panic value can then be re-panicked on the caller's goroutine.
// Usage: defer recoverAndRethrow(func(v any) { panicValue = v })
func recoverAndRethrow(handler func(any)) {
	if r := recover(); r != nil {
		handler(r)
	}
}
