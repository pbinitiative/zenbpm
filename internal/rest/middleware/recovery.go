package middleware

import (
	"encoding/json"
	"net/http"
	"runtime/debug"

	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

const recoveryErrorMessage = "An unexpected error occurred while processing the request"

func Recovery() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				recovery := recover()
				if recovery == nil {
					return
				}
				// http.ErrAbortHandler is a sentinel used to abort a handler;
				// it must not be recovered per net/http docs.
				if recovery == http.ErrAbortHandler {
					panic(recovery)
				}
				log.Errorf(r.Context(),
					"panic recovered in HTTP handler %s %s: %v\n%s",
					r.Method, r.URL.Path, recovery, debug.Stack(),
				)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusInternalServerError)
				_ = json.NewEncoder(w).Encode(public.Error{
					Code:    "TECHNICAL_ERROR",
					Message: recoveryErrorMessage,
				})
			}()
			next.ServeHTTP(w, r)
		})
	}
}
