package e2e

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/pbinitiative/zenbpm/internal/rest/middleware"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestServerRecovery(t *testing.T) {

	router := chi.NewRouter()
	router.Use(middleware.Recovery())
	router.Get("/panic", func(w http.ResponseWriter, r *http.Request) {
		panic("panic")
	})
	router.Get("/ok", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	server := httptest.NewServer(router)
	defer server.Close()

	t.Run("panicking endpoint returns 500 without killing the server", func(t *testing.T) {
		responseFromPanicEndpoint, err := http.Get(server.URL + "/panic")
		require.NoError(t, err)

		defer responseFromPanicEndpoint.Body.Close()

		var body public.Error
		require.NoError(t, json.NewDecoder(responseFromPanicEndpoint.Body).Decode(&body))

		assert.Equal(t, http.StatusInternalServerError, responseFromPanicEndpoint.StatusCode)
		assert.Equal(t, "application/json", responseFromPanicEndpoint.Header.Get("Content-Type"))
		assert.Equal(t, public.Error{
			Code:    "TECHNICAL_ERROR",
			Message: "An unexpected error occurred while processing the request",
		}, body)

		assertOkEndpoint(t, server)
	})
}

func assertOkEndpoint(t *testing.T, server *httptest.Server) {

	responseFromOkEndpoint, err := http.Get(server.URL + "/ok")
	require.NoError(t, err)

	defer responseFromOkEndpoint.Body.Close()

	body, _ := io.ReadAll(responseFromOkEndpoint.Body)
	assert.Equal(t, http.StatusOK, responseFromOkEndpoint.StatusCode)
	assert.Equal(t, "ok", string(body))
}
