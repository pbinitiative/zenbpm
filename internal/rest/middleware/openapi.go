package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers"
	nethttpmiddleware "github.com/oapi-codegen/nethttp-middleware"
	"github.com/pbinitiative/zenbpm/internal/cluster/zenerr"
	"github.com/pbinitiative/zenbpm/internal/log"
	"github.com/pbinitiative/zenbpm/internal/rest/public"
)

// registerXMLBodyDecoder installs the application/xml decoder into
// kin-openapi's global decoder registry exactly once.
//
// kin-openapi has no built-in decoder for application/xml, so without this
// registration every XML request body (e.g. DMN resource deployment) is
// rejected by the validator. The spec models XML bodies as plain strings
// (type: string, format: xml), so the raw payload is returned as-is and
// structural XML validation stays with the handlers/engine.
var registerXMLBodyDecoder = sync.OnceFunc(func() {
	openapi3filter.RegisterBodyDecoder("application/xml", xmlBodyDecoder)
})

// xmlBodyDecoder decodes an application/xml request body into a raw string so
// it can be validated against the `type: string` schema declared in the spec.
func xmlBodyDecoder(body io.Reader, _ http.Header, _ *openapi3.SchemaRef, _ openapi3filter.EncodingFn) (any, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return nil, &openapi3filter.ParseError{Kind: openapi3filter.KindInvalidFormat, Cause: err}
	}
	return string(data), nil
}

// OpenAPIValidator returns a middleware that validates incoming requests
// (path, query, headers and body) against the provided OpenAPI 3 spec.
//
// pathPrefix is the router mount point of the API (e.g. "/v1"); it is
// stripped from the request path before matching against the spec paths.
//
// Validation failures are answered with the shared public.Error JSON shape
// and never reach the handlers. Bounding the request body size is owned by
// RequestBodyLimit, which must be registered upstream; when the validator
// hits that limit while buffering the body it answers with 413. Server
// (Host) validation is disabled because the spec `servers` entry only
// documents a sample deployment URL.
func OpenAPIValidator(spec *openapi3.T, pathPrefix string) func(next http.Handler) http.Handler {
	registerXMLBodyDecoder()
	// built before the validator: OapiRequestValidatorWithOptions clears
	// spec.Servers, but spec.Paths (the only part read here) is left intact.
	allowedMethods := newAllowedMethodsIndex(spec, pathPrefix)
	return nethttpmiddleware.OapiRequestValidatorWithOptions(spec, &nethttpmiddleware.Options{
		DoNotValidateServers: true,
		Prefix:               pathPrefix,
		ErrorHandlerWithOpts: func(_ context.Context, err error, w http.ResponseWriter, r *http.Request, opts nethttpmiddleware.ErrorHandlerOpts) {
			status := validationStatusCode(err, opts.StatusCode)
			// RFC 9110 §15.5.6 makes the Allow header mandatory on 405.
			// opts.MatchedRoute is nil for routers.ErrMethodNotAllowed, so the
			// methods are resolved from the spec paths instead.
			if status == http.StatusMethodNotAllowed {
				if allow := allowedMethods.find(r.URL.Path); allow != "" {
					w.Header().Set("Allow", allow)
				}
			}
			writeValidationError(w, status, err)
		},
	})
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
		return zenerr.BadRequestCode.ToString()
	case http.StatusNotFound:
		return zenerr.NotFoundCode.ToString()
	case http.StatusMethodNotAllowed:
		return zenerr.MethodNotAllowedCode.ToString()
	case http.StatusUnsupportedMediaType:
		return zenerr.UnsupportedMediaTypeCode.ToString()
	case http.StatusRequestEntityTooLarge:
		return zenerr.PayloadTooLargeCode.ToString()
	default:
		return zenerr.TechnicalErrorCode.ToString()
	}
}

// pathTemplateParam matches an OpenAPI path template parameter, e.g. the
// "{processInstanceKey}" in "/process-instances/{processInstanceKey}/jobs".
var pathTemplateParam = regexp.MustCompile(`\{[^/{}]*}`)

// allowedMethodsIndex resolves the HTTP methods a spec path declares, so that
// 405 responses can carry the Allow header required by RFC 9110.
type allowedMethodsIndex struct {
	prefix  string
	entries []allowedMethodsEntry
}

type allowedMethodsEntry struct {
	pattern *regexp.Regexp
	// templated reports whether the spec path contains parameters. Literal
	// paths are matched first so that e.g. "/jobs/search" wins over
	// "/jobs/{jobKey}".
	templated bool
	// allow is the pre-rendered Allow header value, e.g. "GET, POST".
	allow string
}

func newAllowedMethodsIndex(spec *openapi3.T, pathPrefix string) *allowedMethodsIndex {
	index := &allowedMethodsIndex{prefix: pathPrefix}
	if spec == nil || spec.Paths == nil {
		return index
	}
	for specPath, pathItem := range spec.Paths.Map() {
		if pathItem == nil {
			continue
		}
		operations := pathItem.Operations()
		if len(operations) == 0 {
			continue
		}
		methods := make([]string, 0, len(operations))
		for method := range operations {
			methods = append(methods, method)
		}
		sort.Strings(methods)
		pattern, err := compilePathTemplate(specPath)
		if err != nil {
			// a spec path that cannot be compiled only costs us the Allow
			// header for that path; the 405 itself is still returned.
			log.Error("failed to compile OpenAPI path template %q: %s", specPath, err)
			continue
		}
		index.entries = append(index.entries, allowedMethodsEntry{
			pattern:   pattern,
			templated: pathTemplateParam.MatchString(specPath),
			allow:     strings.Join(methods, ", "),
		})
	}
	sort.SliceStable(index.entries, func(i, j int) bool {
		return !index.entries[i].templated && index.entries[j].templated
	})
	return index
}

// find returns the Allow header value for the given request path, or an empty
// string when the path is not part of the spec.
func (i *allowedMethodsIndex) find(requestPath string) string {
	specPath := strings.TrimPrefix(requestPath, i.prefix)
	if specPath == "" {
		specPath = "/"
	}
	for _, entry := range i.entries {
		if entry.pattern.MatchString(specPath) {
			return entry.allow
		}
	}
	return ""
}

// compilePathTemplate turns an OpenAPI path template into an anchored regexp
// where every parameter matches a single non-empty path segment.
func compilePathTemplate(specPath string) (*regexp.Regexp, error) {
	var sb strings.Builder
	sb.WriteString("^")
	last := 0
	for _, loc := range pathTemplateParam.FindAllStringIndex(specPath, -1) {
		sb.WriteString(regexp.QuoteMeta(specPath[last:loc[0]]))
		sb.WriteString("[^/]+")
		last = loc[1]
	}
	sb.WriteString(regexp.QuoteMeta(specPath[last:]))
	sb.WriteString("$")
	return regexp.Compile(sb.String())
}
