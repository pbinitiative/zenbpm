package middleware

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAPIValidatorPassesValidRequest(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances",
		strings.NewReader(`{"processDefinitionKey": 4503599627370498, "variables": {"a": 1}}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, next.called, "next handler should be called for a valid request")
}

func TestOpenAPIValidatorPreservesBodyForNextHandler(t *testing.T) {
	body := `{"processDefinitionKey": 4503599627370498}`
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	require.True(t, next.called)
	assert.JSONEq(t, body, next.body, "request body must remain readable after validation")
}

func TestOpenAPIValidatorRejectsInvalidBodyType(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances",
		strings.NewReader(`{"processDefinitionKey": "not-an-integer"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.False(t, next.called, "next handler must not be called for an invalid request")
	assertErrorPayload(t, rec, "BAD_REQUEST")
}

func TestOpenAPIValidatorRejectsMissingRequiredBody(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", nil)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.False(t, next.called)
	assertErrorPayload(t, rec, "BAD_REQUEST")
}

func TestOpenAPIValidatorRejectsUnknownRoute(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/does-not-exist", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.False(t, next.called)
	assertErrorPayload(t, rec, "NOT_FOUND")
}

func TestOpenAPIValidatorRejectsInvalidQueryParam(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/process-instances?page=abc", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.False(t, next.called)
	assertErrorPayload(t, rec, "BAD_REQUEST")
}

func TestOpenAPIValidatorPassesXmlBody(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/dmn-resource-definitions",
		strings.NewReader(`<?xml version="1.0" encoding="UTF-8"?><definitions></definitions>`))
	req.Header.Set("Content-Type", "application/xml")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, next.called, "next handler should be called for a valid XML request")
	assert.Contains(t, next.body, "<definitions>", "XML body must remain readable after validation")
}

func TestOpenAPIValidatorRejectsUnsupportedMethod(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/v1/dmn-resource-definitions", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.False(t, next.called)
	assert.Equal(t, "GET, POST", rec.Header().Get("Allow"))
	assertErrorPayload(t, rec, "METHOD_NOT_ALLOWED")
}

func TestOpenAPIValidatorSetsAllowHeaderForTemplatedPath(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodDelete, "/v1/process-instances/4503599627370498", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.False(t, next.called)
	assert.Contains(t, rec.Header().Get("Allow"), http.MethodGet,
		"Allow must be derived from the templated spec path")
}

func TestOpenAPIValidatorOmitsAllowHeaderForUnknownRoute(t *testing.T) {
	handler, _ := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodGet, "/v1/does-not-exist", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Empty(t, rec.Header().Get("Allow"))
}

func TestOpenAPIValidatorRejectsUnsupportedMediaType(t *testing.T) {
	handler, next := newValidatedHandler(t)

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
	assert.False(t, next.called)
	assertErrorPayload(t, rec, "UNSUPPORTED_MEDIA_TYPE")
}

func TestOpenAPIValidatorAllowsBodyAtLimit(t *testing.T) {
	body := `{"processDefinitionKey": 4503599627370498}`
	handler, next := newValidatedHandlerWithLimit(t, int64(len(body)))

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, next.called)
}

func TestOpenAPIValidatorRejectsOversizedBodyWithContentLength(t *testing.T) {
	body := `{"processDefinitionKey": 4503599627370498}`
	handler, next := newValidatedHandlerWithLimit(t, int64(len(body)-1))

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.False(t, next.called)
	assertErrorPayload(t, rec, "PAYLOAD_TOO_LARGE")
}

func TestOpenAPIValidatorRejectsOversizedChunkedBody(t *testing.T) {
	body := `{"processDefinitionKey": 4503599627370498}`
	handler, next := newValidatedHandlerWithLimit(t, int64(len(body)-1))

	req := httptest.NewRequest(http.MethodPost, "/v1/process-instances", strings.NewReader(body))
	req.ContentLength = -1
	req.TransferEncoding = []string{"chunked"}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
	assert.False(t, next.called)
	assertErrorPayload(t, rec, "PAYLOAD_TOO_LARGE")
}

func TestErrorCodeForStatus(t *testing.T) {
	assert.Equal(t, "BAD_REQUEST", errorCodeForStatus(http.StatusBadRequest))
	assert.Equal(t, "NOT_FOUND", errorCodeForStatus(http.StatusNotFound))
	assert.Equal(t, "METHOD_NOT_ALLOWED", errorCodeForStatus(http.StatusMethodNotAllowed))
	assert.Equal(t, "UNSUPPORTED_MEDIA_TYPE", errorCodeForStatus(http.StatusUnsupportedMediaType))
	assert.Equal(t, "PAYLOAD_TOO_LARGE", errorCodeForStatus(http.StatusRequestEntityTooLarge))
	assert.Equal(t, "TECHNICAL_ERROR", errorCodeForStatus(http.StatusTeapot))
}

func TestAllowedMethodsIndexPrefersLiteralOverTemplatedPath(t *testing.T) {
	// "/jobs/search" and "/jobs/{jobKey}" both match the request path
	// "/jobs/search"; the literal path must win regardless of the map
	// iteration order of spec.Paths.
	spec := &openapi3.T{Paths: openapi3.NewPaths(
		openapi3.WithPath("/jobs/{jobKey}", &openapi3.PathItem{
			Get: &openapi3.Operation{},
		}),
		openapi3.WithPath("/jobs/search", &openapi3.PathItem{
			Post: &openapi3.Operation{},
		}),
	)}

	index := newAllowedMethodsIndex(spec, "/v1")

	assert.Equal(t, "POST", index.find("/v1/jobs/search"),
		"literal spec path must take precedence over the templated one")
	assert.Equal(t, "GET", index.find("/v1/jobs/4503599627370498"))
}

// spyHandler records whether it was called and captures the request body.
type spyHandler struct {
	called bool
	body   string
}

func (s *spyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.called = true
	if r.Body != nil {
		data, _ := io.ReadAll(r.Body)
		s.body = string(data)
	}
	w.WriteHeader(http.StatusOK)
}

// newValidatedHandler builds the validator middleware around a spy handler
// using the embedded OpenAPI spec mounted under /v1, mirroring production.
func newValidatedHandler(t *testing.T) (http.Handler, *spyHandler) {
	t.Helper()
	return newValidatedHandlerWithLimit(t, 1024*1024)
}

// newValidatedHandlerWithLimit chains RequestBodyLimit and OpenAPIValidator
// the same way the production router does: the limit middleware owns the
// body cap, the validator turns the resulting MaxBytesError into a 413.
func newValidatedHandlerWithLimit(t *testing.T, maxRequestBodyBytes int64) (http.Handler, *spyHandler) {
	t.Helper()
	spec, err := public.GetSpec()
	require.NoError(t, err)
	next := &spyHandler{}
	return RequestBodyLimit(maxRequestBodyBytes)(OpenAPIValidator(spec, "/v1")(next)), next
}

// assertErrorPayload verifies the response carries the shared public.Error
// JSON contract with the expected code and a non-empty message.
func assertErrorPayload(t *testing.T, rec *httptest.ResponseRecorder, expectedCode string) {
	t.Helper()
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	var e public.Error
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &e))
	assert.Equal(t, expectedCode, e.Code)
	assert.NotEmpty(t, e.Message)
}
