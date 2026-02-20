package middleware

import (
	"net/http"
	"net/url"
)

// StripEmptyQueryParams removes query parameters with empty string values.
func StripEmptyQueryParams() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			q := r.URL.Query()
			filtered := make(url.Values, len(q))
			for k, vs := range q {
				var kept []string
				for _, v := range vs {
					if v != "" {
						kept = append(kept, v)
					}
				}
				if len(kept) > 0 {
					filtered[k] = kept
				}
			}
			r.URL.RawQuery = filtered.Encode()
			next.ServeHTTP(w, r)
		})
	}
}
