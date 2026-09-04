package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
)

func TestRespWriterWrapperUsesSafeDefaultContentType(t *testing.T) {
	recorder := httptest.NewRecorder()
	wrapper := &respWriterWrapper{
		ResponseWriter: recorder,
		record:         func(int64) {},
		props:          propagation.NewCompositeTextMapPropagator(),
	}

	_, err := wrapper.Write([]byte("<script>alert('xss')</script>"))

	require.NoError(t, err)
	require.Equal(t, "text/plain; charset=utf-8", recorder.Header().Get("Content-Type"))
	require.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"))
}

func TestRespWriterWrapperPreservesExplicitContentType(t *testing.T) {
	recorder := httptest.NewRecorder()
	recorder.Header().Set("Content-Type", "application/octet-stream")
	wrapper := &respWriterWrapper{
		ResponseWriter: recorder,
		record:         func(int64) {},
		props:          propagation.NewCompositeTextMapPropagator(),
	}

	wrapper.WriteHeader(http.StatusCreated)

	require.Equal(t, "application/octet-stream", recorder.Header().Get("Content-Type"))
	require.Equal(t, "nosniff", recorder.Header().Get("X-Content-Type-Options"))
}
