package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	nethttpmiddleware "github.com/oapi-codegen/nethttp-middleware"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

func init() {
	// kin-openapi has no built-in decoder for application/xml, so without this
	// registration every XML request body (e.g. DMN resource deployment) is
	// rejected by the validator. The spec models XML bodies as plain strings
	// (type: string, format: xml), so the raw payload is returned as-is and
	// structural XML validation stays with the handlers/engine.
	openapi3filter.RegisterBodyDecoder("application/xml", xmlBodyDecoder)
}

// xmlBodyDecoder decodes an application/xml request body into a raw string so
// it can be validated against the `type: string` schema declared in the spec.
func xmlBodyDecoder(body io.Reader, _ http.Header, _ *openapi3.SchemaRef, _ openapi3filter.EncodingFn) (any, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, &openapi3filter.ParseError{Kind: openapi3filter.KindInvalidFormat, Cause: err}
	}
	return string(data), nil
}

// OpenApiValidator returns a middleware that validates incoming requests
// (path, query, headers and body) against the provided OpenAPI 3 spec.
//
// pathPrefix is the router mount point of the API (e.g. "/v1"); it is
// stripped from the request path before matching against the spec paths.
//
// Validation failures are answered with the shared public.Error JSON shape
// and never reach the handlers. Request bodies are limited before the
// validator buffers them. Server (Host) validation is disabled because the
// spec `servers` entry only documents a sample deployment URL.
func OpenApiValidator(spec *openapi3.T, pathPrefix string, maxRequestBodyBytes int64) func(next http.Handler) http.Handler {
	if maxRequestBodyBytes <= 0 {
		panic("maxRequestBodyBytes must be greater than zero")
	}
	validator := nethttpmiddleware.OapiRequestValidatorWithOptions(spec, &nethttpmiddleware.Options{
		DoNotValidateServers: true,
		Prefix:               pathPrefix,
		ErrorHandlerWithOpts: func(_ context.Context, err error, w http.ResponseWriter, _ *http.Request, opts nethttpmiddleware.ErrorHandlerOpts) {
			writeValidationError(w, validationStatusCode(err, opts.StatusCode), err)
		},
	})
	return func(next http.Handler) http.Handler {
		validated := validator(next)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.ContentLength > maxRequestBodyBytes {
				writeValidationError(w, http.StatusRequestEntityTooLarge, &http.MaxBytesError{Limit: maxRequestBodyBytes})
				return
			}
			if r.Body != nil {
				r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
			}
			validated.ServeHTTP(w, r)
		})
	}
}

func validationStatusCode(err error, suggestedStatus int) int {
	var maxBytesErr *http.MaxBytesError
	if errors.As(err, &maxBytesErr) {
		return http.StatusRequestEntityTooLarge
	}
	if errors.Is(err, routers.ErrMethodNotAllowed) {
		return http.StatusMethodNotAllowed
	}
	if isUnsupportedMediaType(err) {
		return http.StatusUnsupportedMediaType
	}
	return suggestedStatus
}

func isUnsupportedMediaType(err error) bool {
	var requestErr *openapi3filter.RequestError
	if !errors.As(err, &requestErr) || requestErr.Err != nil || requestErr.RequestBody == nil ||
		requestErr.Input == nil || requestErr.Input.Request == nil {
		return false
	}
	return requestErr.RequestBody.Content.Get(requestErr.Input.Request.Header.Get("Content-Type")) == nil
}

// writeValidationError writes the validation failure using the same JSON
// error contract (public.Error) as the rest of the API.
func writeValidationError(w http.ResponseWriter, status int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, mErr := json.Marshal(public.Error{
		Code:    errorCodeForStatus(status),
		Message: err.Error(),
	})
	if mErr != nil {
		log.Error("failed to marshal validation error: %s", mErr)
		return
	}
	if _, wErr := w.Write(body); wErr != nil {
		log.Error("failed to write validation error: %s", wErr)
	}
}

func errorCodeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "BAD_REQUEST"
	case http.StatusUnauthorized:
		return "UNAUTHORIZED"
	case http.StatusForbidden:
		return "FORBIDDEN"
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusMethodNotAllowed:
		return "METHOD_NOT_ALLOWED"
	case http.StatusUnsupportedMediaType:
		return "UNSUPPORTED_MEDIA_TYPE"
	case http.StatusRequestEntityTooLarge:
		return "PAYLOAD_TOO_LARGE"
	default:
		return "ERROR"
	}
}
