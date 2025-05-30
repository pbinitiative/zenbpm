package middleware

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/pbinitiative/zenbpm/internal/config"
	otelint "github.com/pbinitiative/zenbpm/internal/otel"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconvV4 "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
)

var TracerKey = "opentelemetry-tracer"

// bodyWrapper wraps a http.Request.Body (an io.ReadCloser) to track the number
// of bytes read and the last error
type bodyWrapper struct {
	io.ReadCloser
	record func(n int64) // must not be nil

	read int64
	err  error
}

func (w *bodyWrapper) Read(b []byte) (int, error) {
	n, err := w.ReadCloser.Read(b)
	n1 := int64(n)
	w.read += n1
	w.err = err
	w.record(n1)
	return n, err
}
func (w *bodyWrapper) Close() error {
	return w.ReadCloser.Close()
}

type respWriterWrapper struct {
	http.ResponseWriter
	record func(n int64) // must not be nil

	// used to inject the header
	ctx context.Context

	props propagation.TextMapPropagator

	written     int64
	statusCode  int
	err         error
	wroteHeader bool
}

func (w *respWriterWrapper) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *respWriterWrapper) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	n1 := int64(n)
	w.record(n1)
	w.written += n1
	w.err = err
	return n, err
}

func (w *respWriterWrapper) WriteHeader(statusCode int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	w.statusCode = statusCode
	w.props.Inject(w.ctx, propagation.HeaderCarrier(w.Header()))
	w.ResponseWriter.WriteHeader(statusCode)
}

// Opentelemetry returns middleware that will trace and meter incoming requests.
func Opentelemetry(conf config.Config) func(next http.Handler) http.Handler {
	tracer := otel.GetTracerProvider().Tracer("http-request-middleware")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			opts := []trace.SpanStartOption{
				trace.WithLinks(trace.LinkFromContext(r.Context())),
				trace.WithAttributes(semconvV4.NetAttributesFromHTTPRequest("tcp", r)...),
				trace.WithAttributes(semconvV4.EndUserAttributesFromHTTPRequest(r)...),
				trace.WithAttributes(getTransferHeaderAttributes(r, conf.Tracing.TransferHeaders)...),
				trace.WithSpanKind(trace.SpanKindServer),
			}
			ctx = getTransferHeadersCtx(ctx, r, conf.Tracing.TransferHeaders)
			ctx, span := tracer.Start(ctx, "request", opts...)

			// pass the span through the request context
			r = r.WithContext(ctx)

			var bw bodyWrapper
			// if request body is nil we don't want to mutate the body as it will affect
			// the identity of it in a unforeseeable way because we assert ReadCloser
			// fullfills a certain interface and it is indeed nil.
			if r.Body != nil {
				bw.ReadCloser = r.Body
				bw.record = func(n int64) {
					span.AddEvent("read", trace.WithAttributes(otelhttp.ReadBytesKey.Int64(n)))
				}
				r.Body = &bw
			}
			writeRecordFunc := func(n int64) {
				span.AddEvent("write", trace.WithAttributes(otelhttp.WroteBytesKey.Int64(n)))
			}
			rww := &respWriterWrapper{ResponseWriter: w, record: writeRecordFunc, ctx: ctx, props: otel.GetTextMapPropagator()}

			defer span.End()

			startTime := time.Now()
			// serve the request to the next middleware and get route pattern
			next.ServeHTTP(rww, r)

			routePattern := chi.RouteContext(r.Context()).RoutePattern()
			span.SetName(routePattern)
			span.SetAttributes(
				semconvV4.HTTPServerAttributesFromHTTPRequest(conf.Tracing.Name, routePattern, r)...,
			)

			setAfterServeTracing(span, bw.read, rww.written, rww.statusCode, bw.err, rww.err)
			setAfterServeMetrics(routePattern, r, rww, startTime, conf)
		})
	}
}

func setAfterServeMetrics(routePattern string, r *http.Request, rww *respWriterWrapper, startTime time.Time, conf config.Config) {
	tags := []attribute.KeyValue{
		attribute.String("path", routePattern),
		attribute.String("method", r.Method),
		attribute.Int("status", rww.statusCode),
	}
	otelint.RequestTotal.Add(r.Context(), 1)
	otelint.RequestUriTotal.Add(r.Context(), 1, metric.WithAttributes(tags...))
	if r.ContentLength >= 0 {
		otelint.RequestBodySize.Add(r.Context(), float64(r.ContentLength), metric.WithAttributes(tags...))
	}
	if rww.written > 0 {
		otelint.ResponseBodySize.Add(r.Context(), float64(rww.written), metric.WithAttributes(tags...))
	}
	latency := time.Since(startTime)
	otelint.RequestDuration.Record(r.Context(), latency.Seconds()*1000, metric.WithAttributes(tags...))
}

func setAfterServeTracing(span trace.Span, read, wrote int64, statusCode int, rerr, werr error) {
	attributes := []attribute.KeyValue{}

	if read > 0 {
		attributes = append(attributes, otelint.ReadBytesKey.Int64(read))
	}
	if rerr != nil && rerr != io.EOF {
		attributes = append(attributes, otelint.ReadErrorKey.String(rerr.Error()))
	}
	if wrote > 0 {
		attributes = append(attributes, otelint.WroteBytesKey.Int64(wrote))
	}
	if statusCode > 0 {
		attributes = append(attributes, semconvV4.HTTPAttributesFromHTTPStatusCode(statusCode)...)
		span.SetStatus(semconvV4.SpanStatusFromHTTPStatusCode(statusCode))
		span.RecordError(werr)
	}
	if werr != nil && werr != io.EOF {
		attributes = append(attributes, otelint.WriteErrorKey.String(werr.Error()))
	}
	span.SetAttributes(attributes...)
}

func getTransferHeadersCtx(ctx context.Context, r *http.Request, transferHeaders []string) context.Context {
	for _, header := range transferHeaders {
		hVal := r.Header.Get(header)
		ctx = context.WithValue(ctx, otelint.TransferHeaderKey(header), hVal)
	}
	return ctx
}

func getTransferHeaderAttributes(r *http.Request, transferHeaders []string) []attribute.KeyValue {
	attributes := make([]attribute.KeyValue, len(transferHeaders))
	for i, header := range transferHeaders {
		hVal := r.Header.Get(header)
		attributes[i] = attribute.String(header, hVal)
	}
	return attributes
}
