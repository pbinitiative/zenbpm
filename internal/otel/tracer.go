package otel

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/pbinitiative/zenbpm/internal/config"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// validateSamplerRatio checks that the configured trace sampler ratio is a
// finite number within [0, 1]. It is validated regardless of whether tracing
// is enabled so that a latent misconfiguration fails fast at startup instead
// of on the day tracing gets switched on.
func validateSamplerRatio(ratio float64) error {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
		return fmt.Errorf("tracing sampler ratio must be between 0 and 1, got %v", ratio)
	}
	return nil
}

func setupTraceProvider(conf config.Tracing) (*trace.TracerProvider, error) {
	// sample new traces at the configured ratio; child spans follow their parent decision.
	// An explicit 0 disables root sampling; out-of-range values are a configuration error.
	// Validated before any exporter/resource setup to avoid avoidable work on bad config.
	if err := validateSamplerRatio(conf.SamplerRatio); err != nil {
		return nil, err
	}
	endpoint := conf.Endpoint
	endpoint = strings.TrimPrefix(endpoint, "https://")
	endpoint = strings.TrimPrefix(endpoint, "http://")
	ctx := context.Background()
	exporter, err := otlptrace.New(
		ctx,
		otlptracehttp.NewClient(
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithInsecure(),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("creating new exporter: %w", err)
	}
	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(conf.Name),
		),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithProcess(),
		resource.WithOS(),
		resource.WithContainer(),
		resource.WithHost(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create new tracing resource: %w", err)
	}

	tracerprovider := trace.NewTracerProvider(
		trace.WithSampler(trace.ParentBased(trace.TraceIDRatioBased(conf.SamplerRatio))),
		trace.WithBatcher(
			exporter,
			trace.WithMaxExportBatchSize(trace.DefaultMaxExportBatchSize),
			trace.WithBatchTimeout(5*time.Second),
		),
		trace.WithResource(res),
	)

	return tracerprovider, nil
}
