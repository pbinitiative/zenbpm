package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
)

func TestRespWriterWrapper(t *testing.T) {
	t.Run("implicit status commits safe default headers and preserves body", func(t *testing.T) {
		wrapper, recorder := newTestRespWriterWrapper(t)
		const body = "<script>alert('xss')</script>"

		_, err := wrapper.Write([]byte(body))

		require.NoError(t, err)
		response := recorder.Result()
		t.Cleanup(func() { require.NoError(t, response.Body.Close()) })
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, "text/plain; charset=utf-8", response.Header.Get("Content-Type"))
		require.Equal(t, "nosniff", response.Header.Get("X-Content-Type-Options"))
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(data))
	})

	t.Run("explicit status commits safe default headers before body", func(t *testing.T) {
		wrapper, recorder := newTestRespWriterWrapper(t)
		const body = "<script>alert('xss')</script>"

		wrapper.WriteHeader(http.StatusBadRequest)
		_, err := wrapper.Write([]byte(body))

		require.NoError(t, err)
		response := recorder.Result()
		t.Cleanup(func() { require.NoError(t, response.Body.Close()) })
		require.Equal(t, http.StatusBadRequest, response.StatusCode)
		require.Equal(t, "text/plain; charset=utf-8", response.Header.Get("Content-Type"))
		require.Equal(t, "nosniff", response.Header.Get("X-Content-Type-Options"))
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(data))
	})

	t.Run("implicit status preserves explicit JSON content type and body", func(t *testing.T) {
		wrapper, recorder := newTestRespWriterWrapper(t)
		wrapper.Header().Set("Content-Type", "application/json")
		const body = `{"ok":true}`

		_, err := wrapper.Write([]byte(body))

		require.NoError(t, err)
		response := recorder.Result()
		t.Cleanup(func() { require.NoError(t, response.Body.Close()) })
		require.Equal(t, http.StatusOK, response.StatusCode)
		require.Equal(t, "application/json", response.Header.Get("Content-Type"))
		require.Equal(t, "nosniff", response.Header.Get("X-Content-Type-Options"))
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, body, string(data))
	})

	t.Run("explicit status preserves explicit binary content type and body", func(t *testing.T) {
		wrapper, recorder := newTestRespWriterWrapper(t)
		wrapper.Header().Set("Content-Type", "application/octet-stream")
		body := []byte{0, 1, 2, 255}

		wrapper.WriteHeader(http.StatusCreated)
		_, err := wrapper.Write(body)

		require.NoError(t, err)
		response := recorder.Result()
		t.Cleanup(func() { require.NoError(t, response.Body.Close()) })
		require.Equal(t, http.StatusCreated, response.StatusCode)
		require.Equal(t, "application/octet-stream", response.Header.Get("Content-Type"))
		require.Equal(t, "nosniff", response.Header.Get("X-Content-Type-Options"))
		data, err := io.ReadAll(response.Body)
		require.NoError(t, err)
		require.Equal(t, body, data)
	})
}

func newTestRespWriterWrapper(t *testing.T) (*respWriterWrapper, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	wrapper := &respWriterWrapper{
		ResponseWriter: recorder,
		record:         func(int64) {},
		ctx:            t.Context(),
		props:          propagation.NewCompositeTextMapPropagator(),
	}
	return wrapper, recorder
}
