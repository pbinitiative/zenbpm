package middleware

import "net/http"

// RequestBodyLimit bounds how many request body bytes any downstream
// middleware or handler is able to read.
//
// It has to be registered before every middleware that buffers request bodies
// (Logger with body capture, the OpenAPI validator, …). Those layers copy
// whatever they read into memory, so without an upstream cap a client
// streaming a chunked body could make them allocate without bound.
//
// The middleware only wraps the body; producing the 413 response is left to
// the layer that actually reads it (see OpenAPIValidator). That keeps the
// rejection inside the logged part of the chain instead of short-circuiting
// before the request logger runs.
func RequestBodyLimit(maxRequestBodyBytes int64) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if maxRequestBodyBytes <= 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Body != nil && r.Body != http.NoBody {
				r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
			}
			next.ServeHTTP(w, r)
		})
	}
}
