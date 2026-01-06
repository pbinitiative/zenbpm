package otel

import (
	"context"
	"errors"
	"fmt"

	"github.com/pbinitiative/zenbpm/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	metrics "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
)

var (
	RequestTotal     metrics.Int64Counter
	RequestUriTotal  metrics.Int64Counter
	RequestBodySize  metrics.Float64Counter
	ResponseBodySize metrics.Float64Counter
	RequestDuration  metrics.Float64Histogram

	requestMeter string = "request-meter"
)

type Otel struct {
	meterProvider  *metric.MeterProvider
	tracerprovider *trace.TracerProvider
}

func SetupOtel(conf config.Tracing) (*Otel, error) {
	o := Otel{}
	var err error

	o.meterProvider, err = setupMeterProvider(conf.Name)
	if err != nil {
		return nil, err
	}
	otel.SetMeterProvider(o.meterProvider)
	if conf.Enabled {
		o.tracerprovider, err = setupTraceProvider(conf)
		otel.SetTracerProvider(o.tracerprovider)
		if err != nil {
			return nil, fmt.Errorf("failed to set up tracer: %w", err)
		}
	}

	return &o, nil
}

func (o *Otel) Stop(ctx context.Context) {
	if o.meterProvider != nil {
		_ = o.meterProvider.Shutdown(ctx)
		o.meterProvider = nil
	}
	if o.tracerprovider != nil {
		_ = o.tracerprovider.Shutdown(ctx)
		o.tracerprovider = nil
	}
}

func setupMeterProvider(appName string) (*metric.MeterProvider, error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, fmt.Errorf("failed to set up prometheus exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(appName),
		attribute.String("library.language", "go"),
	))
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(exporter),
		metric.WithResource(res),
	)

	var errJoin error
	RequestTotal, err = otel.Meter(requestMeter).Int64Counter("request_total", metrics.WithDescription("Total requests to the server"))
	errJoin = errors.Join(errJoin, err)
	RequestUriTotal, err = otel.Meter(requestMeter).Int64Counter("request_uri_total", metrics.WithDescription("Total request per uri"))
	errJoin = errors.Join(errJoin, err)
	RequestBodySize, err = otel.Meter(requestMeter).Float64Counter("request_body_size", metrics.WithUnit("By"), metrics.WithDescription("Server received request body size, bytes"))
	errJoin = errors.Join(errJoin, err)
	ResponseBodySize, err = otel.Meter(requestMeter).Float64Counter("response_body_size", metrics.WithUnit("By"), metrics.WithDescription("Server send response body size, bytes"))
	errJoin = errors.Join(errJoin, err)
	RequestDuration, err = otel.Meter(requestMeter).Float64Histogram("request_duration", metrics.WithUnit("ms"), metrics.WithDescription("Time the server took to handle the request, milliseconds"))
	errJoin = errors.Join(errJoin, err)
	if errJoin != nil {
		return nil, fmt.Errorf("failed to create otel instruments: %w", err)
	}
	return meterProvider, nil
}
