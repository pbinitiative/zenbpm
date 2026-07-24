package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/pbinitiative/zenbpm/internal/config"
	"github.com/pbinitiative/zenbpm/internal/otel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSystemInfo(t *testing.T) {
	openTelemetry, err := otel.SetupOtel(config.Tracing{Name: "rest-test"})
	require.NoError(t, err)
	t.Cleanup(func() { openTelemetry.Stop(t.Context()) })

	api, err := openapi3.NewLoader().LoadFromFile("../../openapi/api.yaml")
	require.NoError(t, err)

	server := NewServer(nil, config.Config{}, "7af392e")
	request := httptest.NewRequest(http.MethodGet, "/system/info", nil)
	response := httptest.NewRecorder()

	server.server.Handler.ServeHTTP(response, request)

	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"version":"`+api.Info.Version+`","commit":"7af392e"}`, response.Body.String())
}
