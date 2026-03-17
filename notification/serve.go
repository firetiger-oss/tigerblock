package notification

import (
	"cmp"
	"net/http"
	"os"
)

// DefaultServeOptions contains options registered by subpackages via init().
// Import notification/aws, notification/gcp, or notification/cloudflare to
// register their respective handlers.
var DefaultServeOptions []ServeOption

// Serve starts a notification handler using registered options.
//
// Environment detection for Lambda mode requires importing notification/aws.
// HTTP endpoints are registered via DefaultServeOptions by importing subpackages.
func Serve(handler ObjectHandler, options ...ServeOption) error {
	opts := defaultServeOptions()
	for _, opt := range DefaultServeOptions {
		opt(&opts)
	}
	for _, opt := range options {
		opt(&opts)
	}

	// Check if serve logic was overridden (e.g., Lambda mode)
	if opts.serve != nil {
		opts.serve(handler)
		return nil
	}

	// HTTP mode
	mux := http.NewServeMux()
	for _, h := range opts.handlers {
		mux.Handle(h.route, h.handler(handler))
	}

	if opts.healthPath != "" {
		mux.HandleFunc("GET "+opts.healthPath, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
	}

	return http.ListenAndServe(":"+opts.port, mux)
}

type serveOptions struct {
	healthPath string
	port       string
	serve      func(ObjectHandler)
	handlers   []routeHandler
}

type routeHandler struct {
	route   string
	handler func(ObjectHandler) http.Handler
}

func defaultServeOptions() serveOptions {
	return serveOptions{
		healthPath: "/health",
		port:       cmp.Or(os.Getenv("PORT"), "8080"),
	}
}

// ServeOption is a functional option for configuring Serve.
type ServeOption func(*serveOptions)

// WithHealthPath sets the health check endpoint path (default: "/health", empty to disable)
func WithHealthPath(path string) ServeOption {
	return func(o *serveOptions) { o.healthPath = path }
}

// WithPort sets the HTTP port (default: PORT env or "8080")
func WithPort(port string) ServeOption {
	return func(o *serveOptions) { o.port = port }
}

// WithServe overrides the default HTTP serve logic (used by aws package for Lambda)
func WithServe(serve func(ObjectHandler)) ServeOption {
	return func(o *serveOptions) { o.serve = serve }
}

// WithHandler registers an HTTP handler for a route (used by aws, gcp, cloudflare packages)
func WithHandler(route string, handler func(ObjectHandler) http.Handler) ServeOption {
	return func(o *serveOptions) {
		o.handlers = append(o.handlers, routeHandler{route: route, handler: handler})
	}
}
