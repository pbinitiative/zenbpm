package rest

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteHealthResponseHealthy(t *testing.T) {
	rec := httptest.NewRecorder()

	writeHealthResponse(rec, slog.Default(), true, nil)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	body := decodeHealthResponse(t, rec)
	assert.Equal(t, "UP", body.Status)
	assert.Empty(t, body.Reasons)
	// nil reasons must serialize as an empty array, not null
	assert.Contains(t, rec.Body.String(), `"reasons":[]`)
}

func TestWriteHealthResponseUnhealthy(t *testing.T) {
	rec := httptest.NewRecorder()
	reasons := []string{"no cluster leader elected", "partition 2 has no leader"}

	writeHealthResponse(rec, slog.Default(), false, reasons)

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	body := decodeHealthResponse(t, rec)
	assert.Equal(t, "DOWN", body.Status)
	assert.Equal(t, reasons, body.Reasons)
}

type healthResponse struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons"`
}

func decodeHealthResponse(t *testing.T, rec *httptest.ResponseRecorder) healthResponse {
	t.Helper()
	var body healthResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	return body
}
