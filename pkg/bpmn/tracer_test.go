// Copyright 2021-present ZenBPM Contributors
// (based on git commit history).
//
// ZenBPM project is available under two licenses:
//  - SPDX-License-Identifier: AGPL-3.0-or-later (See LICENSE-AGPL.md)
//  - Enterprise License (See LICENSE-ENTERPRISE.md)

package bpmn

import (
	"testing"

	"github.com/pbinitiative/zenbpm/pkg/bpmn/runtime"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestTracer(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tracerprovider := trace.NewTracerProvider(
		trace.WithBatcher(
			exporter,
			trace.WithBatchTimeout(0),
		),
	)
	origTracer := otel.GetTracerProvider()
	defer otel.SetTracerProvider(origTracer)
	otel.SetTracerProvider(tracerprovider)

	ctx, parent := tracerprovider.Tracer("test-tracer").Start(t.Context(), "parent-test-span")

	process, _ := bpmnEngine.LoadFromFile("./test-cases/simple-link-event-output-variables.bpmn")
	h := bpmnEngine.NewTaskHandler().Type("task").Handler(func(job ActivatedJob) {
		job.Complete()
	})
	defer bpmnEngine.RemoveHandler(h)

	instance, err := bpmnEngine.CreateInstanceByKey(ctx, process.Key, nil)
	assert.NoError(t, err)
	assert.Equal(t, runtime.ActivityStateCompleted, instance.State)

	parent.End()

	tracerprovider.ForceFlush(ctx)
	spans := exporter.GetSpans()
	for _, span := range spans {
		if span.SpanContext.TraceID() == parent.SpanContext().TraceID() {
			continue
		}
		assert.Equal(t, parent.SpanContext().TraceID(), span.Parent.TraceID())
	}
	assert.Len(t, spans, 14)
}
