package middleware

import (
	"compress/flate"
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"unicode/utf8"

	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/httplog/v3"
	"github.com/pbinitiative/zenbpm/internal/config"
)

const defaultLoggedBodyBytes = 64 * 1024

// LogMode controls which requests get logged. The string values are the single
// source of truth in internal/config, so this stays in sync with the parsed
// httpServer.logMode configuration.
type LogMode string

const (
	// LogModeErrors logs failed requests only (status >= 400). This is the
	// default when LoggingOpts.Mode is left empty.
	LogModeErrors LogMode = config.LogModeErrors
	// LogModeAll logs every request.
	LogModeAll LogMode = config.LogModeAll
	// LogModeOff disables request logging entirely.
	LogModeOff LogMode = config.LogModeOff
)

// LoggingOpts contains the middleware configuration.
type LoggingOpts struct {
	// Mode controls which requests are logged. Defaults to LogModeErrors.
	Mode LogMode

	// WithReferer enables logging the "Referer" HTTP header value.
	WithReferer bool

	// WithUserAgent enables logging the "User-Agent" HTTP header value.
	WithUserAgent bool

	// MaxBodyBytes limits request and response body bytes included in a log entry.
	// Defaults to 64 KiB when unset.
	MaxBodyBytes int

	// LogRequestBody enables capturing of request bodies. Capturing
	// buffers the whole body of every request in memory, even those that
	// end up not being logged.
	LogRequestBody bool

	// LogResponseBody enables capturing of response bodies. Capturing
	// buffers the whole body of every response in memory, even those that
	// end up not being logged; keep this off on busy servers.
	LogResponseBody bool

	// IgnorePaths lists exact request paths that are never logged,
	// regardless of Mode (e.g. metrics scrape endpoints).
	IgnorePaths []string
}

// Logger returns a logger middleware for chi, that implements the http.Handler interface.
// By default only failed requests (status >= 400) are logged: 5xx at Error,
// 4xx at Warn (429 at Info, an httplog exception). Panics in downstream
// handlers are recovered, logged with a stack trace and answered with HTTP 500.
func Logger(logger *slog.Logger, opts *LoggingOpts) func(next http.Handler) http.Handler {
	if opts == nil {
		opts = &LoggingOpts{}
	}
	if logger == nil || opts.Mode == LogModeOff {
		return func(next http.Handler) http.Handler { return next }
	}
	maxBodyBytes := opts.MaxBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = defaultLoggedBodyBytes
	}

	// httplog logs User-Agent and Referer unconditionally; blanking the schema
	// key drops the attribute, which keeps the LoggingOpts contract.
	schema := *httplog.SchemaECS
	if !opts.WithUserAgent {
		schema.RequestUserAgent = ""
	}
	if !opts.WithReferer {
		schema.RequestReferer = ""
	}

	ignoredPaths := make(map[string]struct{}, len(opts.IgnorePaths))
	for _, path := range opts.IgnorePaths {
		ignoredPaths[path] = struct{}{}
	}
	ignored := func(req *http.Request) bool {
		_, ok := ignoredPaths[req.URL.Path]
		return ok
	}

	var skip func(req *http.Request, respStatus int) bool
	if opts.Mode != LogModeAll || len(ignoredPaths) > 0 {
		skip = func(req *http.Request, respStatus int) bool {
			if ignored(req) {
				return true
			}
			return opts.Mode != LogModeAll && respStatus < http.StatusBadRequest
		}
	}

	// Body capture predicates must stay nil when capture is off: httplog tees
	// bodies whenever a predicate is set, regardless of its result.
	var logRequestBody, logResponseBody func(req *http.Request) bool
	if opts.LogRequestBody {
		logRequestBody = func(req *http.Request) bool { return !ignored(req) }
	}
	if opts.LogResponseBody {
		logResponseBody = func(req *http.Request) bool { return !ignored(req) }
	}

	reqLogger := slog.New(reqIDHandler{Handler: logger.Handler(), maxBodyBytes: maxBodyBytes})

	return httplog.RequestLogger(reqLogger, &httplog.Options{
		Level:         slog.LevelInfo,
		Schema:        &schema,
		RecoverPanics: true,
		Skip:          skip,
		// Content-Encoding is passed through so that reqIDHandler can
		// decompress compressed bodies before logging them.
		LogRequestHeaders:  []string{"Content-Type", "Content-Encoding"},
		LogResponseHeaders: []string{"Content-Type", "Content-Encoding"},
		LogRequestBody:     logRequestBody,
		LogResponseBody:    logResponseBody,
		LogBodyMaxLen:      maxBodyBytes,
	})
}

// reqIDHandler adds the chi request ID to log records and cleans up body
// attributes. It reads the ID from the record context instead of httplog's
// LogExtraAttrs, because setting LogExtraAttrs forces httplog to buffer
// every request body.
type reqIDHandler struct {
	slog.Handler
	maxBodyBytes int
}

func (h reqIDHandler) Handle(ctx context.Context, record slog.Record) error {
	reqEncoding := headerFromRecord(record, httplog.SchemaECS.RequestHeaders, "Content-Encoding")
	respEncoding := headerFromRecord(record, httplog.SchemaECS.ResponseHeaders, "Content-Encoding")

	cleaned := slog.NewRecord(record.Time, record.Level, record.Message, record.PC)
	record.Attrs(func(attr slog.Attr) bool {
		switch attr.Key {
		case httplog.SchemaECS.RequestBody:
			attr = h.cleanBodyAttr(attr, reqEncoding)
		case httplog.SchemaECS.ResponseBody:
			attr = h.cleanBodyAttr(attr, respEncoding)
		}
		if !attr.Equal(slog.Attr{}) {
			cleaned.AddAttrs(attr)
		}
		return true
	})
	if reqID := chimiddleware.GetReqID(ctx); reqID != "" {
		cleaned.AddAttrs(slog.String("reqId", reqID))
	}
	return h.Handler.Handle(ctx, cleaned)
}

// cleanBodyAttr drops empty body attributes and decompresses compressed body
// content. httplog whitelists bodies by Content-Type only, so e.g.
// gzip-compressed text/plain responses would otherwise be logged as raw
// bytes. Content-Encoding is the primary signal; the UTF-8 check is a
// fallback for binary payloads served with a missing or lying header.
func (h reqIDHandler) cleanBodyAttr(attr slog.Attr, contentEncoding string) slog.Attr {
	body := attr.Value.String()
	if body == "" {
		return slog.Attr{}
	}
	if contentEncoding != "" && contentEncoding != "identity" {
		decoded, ok := decodeBody(body, contentEncoding, h.maxBodyBytes)
		if !ok {
			return slog.String(attr.Key, "["+contentEncoding+" body omitted]")
		}
		body = decoded
	}
	if !utf8.ValidString(body) {
		return slog.String(attr.Key, "[binary body omitted]")
	}
	return slog.String(attr.Key, body)
}

// decodeBody decompresses a captured body so the log shows the actual
// content. Unsupported encodings report !ok; a body cut off at httplog's
// capture limit decodes into its readable prefix.
func decodeBody(body string, contentEncoding string, maxBodyBytes int) (string, bool) {
	var reader io.ReadCloser
	switch contentEncoding {
	case "gzip":
		gzipReader, err := gzip.NewReader(strings.NewReader(body))
		if err != nil {
			return "", false
		}
		reader = gzipReader
	case "deflate":
		reader = flate.NewReader(strings.NewReader(body))
	default:
		return "", false
	}
	decoded, err := io.ReadAll(io.LimitReader(reader, int64(maxBodyBytes)+1))
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil && len(decoded) == 0 {
		return "", false
	}
	if err != nil || len(decoded) > maxBodyBytes {
		if len(decoded) > maxBodyBytes {
			decoded = decoded[:maxBodyBytes]
		}
		return string(decoded) + "... [trimmed]", true
	}
	return string(decoded), true
}

// headerFromRecord reads a single header value from the header group
// attribute that httplog adds to the record.
func headerFromRecord(record slog.Record, groupKey, header string) string {
	var value string
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key != groupKey || attr.Value.Kind() != slog.KindGroup {
			return true
		}
		for _, headerAttr := range attr.Value.Group() {
			if headerAttr.Key == header {
				value = headerAttr.Value.String()
				return false
			}
		}
		return false
	})
	return value
}

func (h reqIDHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return reqIDHandler{Handler: h.Handler.WithAttrs(attrs), maxBodyBytes: h.maxBodyBytes}
}

func (h reqIDHandler) WithGroup(name string) slog.Handler {
	return reqIDHandler{Handler: h.Handler.WithGroup(name), maxBodyBytes: h.maxBodyBytes}
}
