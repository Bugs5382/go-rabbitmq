package otel

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

import (
	"context"
	"errors"
	"testing"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func testProviders(t *testing.T) (*tracetest.SpanRecorder, []Option) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	prop := propagation.TraceContext{}
	return sr, []Option{WithTracerProvider(tp), WithPropagator(prop)}
}

func TestPublishInjectsTraceContextAndSpan(t *testing.T) {
	sr, opts := testProviders(t)
	ic := PublishInterceptor(opts...)

	msg := &amqp.Publishing{}
	err := ic(context.Background(), "events", "orders.created", msg,
		func(context.Context, string, string, *amqp.Publishing) error { return nil })
	if err != nil {
		t.Fatalf("interceptor: %v", err)
	}

	if _, ok := msg.Headers["traceparent"]; !ok {
		t.Fatalf("expected traceparent injected into headers, got %+v", msg.Headers)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].SpanKind() != trace.SpanKindProducer {
		t.Errorf("expected producer span, got %v", spans[0].SpanKind())
	}
}

func TestPublishToConsumePropagatesTrace(t *testing.T) {
	sr, opts := testProviders(t)
	pub := PublishInterceptor(opts...)
	con := ConsumeInterceptor(opts...)

	// publish, capturing the headers that would go on the wire.
	msg := &amqp.Publishing{}
	if err := pub(context.Background(), "events", "orders.created", msg,
		func(context.Context, string, string, *amqp.Publishing) error { return nil }); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// consume the "same" message: headers carry the trace context.
	del := rabbitmq.Delivery{Exchange: "events", RoutingKey: "orders.created", Headers: msg.Headers}
	handlerRan := false
	err := con(context.Background(), del, func(context.Context, rabbitmq.Delivery) error {
		handlerRan = true
		return nil
	})
	if err != nil {
		t.Fatalf("consume: %v", err)
	}
	if !handlerRan {
		t.Fatal("handler did not run")
	}

	spans := sr.Ended()
	if len(spans) != 2 {
		t.Fatalf("expected producer + consumer spans, got %d", len(spans))
	}
	producer, consumer := spans[0], spans[1]
	if producer.SpanContext().TraceID() != consumer.SpanContext().TraceID() {
		t.Errorf("trace not propagated: producer %s vs consumer %s",
			producer.SpanContext().TraceID(), consumer.SpanContext().TraceID())
	}
	if consumer.Parent().SpanID() != producer.SpanContext().SpanID() {
		t.Errorf("consumer span is not a child of the producer span")
	}
	if consumer.SpanKind() != trace.SpanKindConsumer {
		t.Errorf("expected consumer span kind, got %v", consumer.SpanKind())
	}
}

func TestConsumeInterceptorRecordsHandlerError(t *testing.T) {
	sr, opts := testProviders(t)
	con := ConsumeInterceptor(opts...)

	wantErr := errors.New("handler failed")
	del := rabbitmq.Delivery{Exchange: "events", RoutingKey: "k"}
	err := con(context.Background(), del, func(context.Context, rabbitmq.Delivery) error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("interceptor should surface the handler error, got %v", err)
	}
	spans := sr.Ended()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}
	if spans[0].Status().Code.String() != "Error" {
		t.Errorf("expected error status on span, got %v", spans[0].Status().Code)
	}
}

func TestInstrumentReturnsBothInterceptors(t *testing.T) {
	_, opts := testProviders(t)
	got := Instrument(opts...)
	if len(got) != 2 {
		t.Fatalf("Instrument should return 2 rabbitmq.Options, got %d", len(got))
	}
}
