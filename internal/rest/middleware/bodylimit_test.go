package middleware

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestBodyLimitAllowsBodyWithinLimit(t *testing.T) {
	body := "0123456789"
	reader := &bodyReadRecorder{}
	handler := RequestBodyLimit(int64(len(body)))(reader)

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(body))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	require.NoError(t, reader.err)
	assert.Equal(t, body, string(reader.read))
}

func TestRequestBodyLimitStopsReadingOversizedChunkedBody(t *testing.T) {
	limit := int64(8)
	reader := &bodyReadRecorder{}
	handler := RequestBodyLimit(limit)(reader)

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(strings.Repeat("a", 1024)))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	handler.ServeHTTP(httptest.NewRecorder(), req)

	var maxBytesErr *http.MaxBytesError
	require.ErrorAs(t, reader.err, &maxBytesErr, "downstream reads must fail once the limit is exceeded")
	assert.Equal(t, limit, maxBytesErr.Limit)
	assert.LessOrEqual(t, int64(len(reader.read)), limit,
		"no more than the configured limit may be buffered downstream")
}

func TestRequestBodyLimitIsAppliedBeforeBodyLoggingCapture(t *testing.T) {
	limit := int64(8)
	reader := &bodyReadRecorder{}
	var captured strings.Builder
	// mirrors internal/rest/server.go: the limit is registered before the
	// logger so that its body capture cannot buffer more than the limit.
	handler := RequestBodyLimit(limit)(teeBody(&captured, reader))

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(strings.Repeat("a", 1024)))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	handler.ServeHTTP(httptest.NewRecorder(), req)

	assert.LessOrEqual(t, int64(captured.Len()), limit+1,
		"the capturing middleware must not buffer beyond the limit")
}

func TestRequestBodyLimitIsNoopForNonPositiveLimit(t *testing.T) {
	body := strings.Repeat("a", 1024)
	reader := &bodyReadRecorder{}
	handler := RequestBodyLimit(0)(reader)

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(body))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	require.NoError(t, reader.err)
	assert.Equal(t, body, string(reader.read))
}

// bodyReadRecorder drains the request body and records what it managed to read.
type bodyReadRecorder struct {
	read []byte
	err  error
}

func (b *bodyReadRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.read, b.err = io.ReadAll(r.Body)
	if errors.Is(b.err, io.EOF) {
		b.err = nil
	}
	w.WriteHeader(http.StatusOK)
}

// captured stands in for the in-memory buffer that httplog's request body
// capture tees the body into. teeBody mimics that capture.
func teeBody(captured *strings.Builder, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = io.NopCloser(io.TeeReader(r.Body, captured))
		next.ServeHTTP(w, r)
	})
}
