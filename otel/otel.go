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
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
	otelapi "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/Bugs5382/go-rabbitmq/otel"

	// Attribute keys follow the OpenTelemetry messaging conventions. They are
	// spelled out rather than imported from a semconv package to avoid pinning a
	// specific, fast-moving semconv version onto consumers.
	attrSystem      = "messaging.system"
	attrDestination = "messaging.destination.name"
	attrRoutingKey  = "messaging.rabbitmq.destination.routing_key"
	attrOperation   = "messaging.operation"
	attrOutcome     = "messaging.outcome" // "ok" | "error"
)

// config holds the resolved adapter configuration.
type config struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	meter      metric.Meter
}

// Option configures the adapter.
type Option func(*providers)

type providers struct {
	tp trace.TracerProvider
	mp metric.MeterProvider
	pr propagation.TextMapPropagator
}

// WithTracerProvider overrides the TracerProvider (default: the global one).
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(p *providers) {
		if tp != nil {
			p.tp = tp
		}
	}
}

// WithMeterProvider overrides the MeterProvider (default: the global one).
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(p *providers) {
		if mp != nil {
			p.mp = mp
		}
	}
}

// WithPropagator overrides the TextMapPropagator used to carry trace context in
// message headers (default: the global one, typically W3C tracecontext).
func WithPropagator(pr propagation.TextMapPropagator) Option {
	return func(p *providers) {
		if pr != nil {
			p.pr = pr
		}
	}
}

func newConfig(opts ...Option) config {
	p := providers{
		tp: otelapi.GetTracerProvider(),
		mp: otelapi.GetMeterProvider(),
		pr: otelapi.GetTextMapPropagator(),
	}
	for _, opt := range opts {
		opt(&p)
	}
	return config{
		tracer:     p.tp.Tracer(instrumentationName),
		propagator: p.pr,
		meter:      p.mp.Meter(instrumentationName),
	}
}

// Instrument returns the go-rabbitmq options that enable OpenTelemetry tracing
// and metrics on a connection. Pass it straight to rabbitmq.Connect:
//
//	conn, err := rabbitmq.Connect(ctx, url, rmqotel.Instrument()...)
func Instrument(opts ...Option) []rabbitmq.Option {
	cfg := newConfig(opts...)
	m := newMetrics(cfg.meter)
	return []rabbitmq.Option{
		rabbitmq.WithPublishInterceptor(cfg.publishInterceptor(m)),
		rabbitmq.WithConsumeInterceptor(cfg.consumeInterceptor(m)),
	}
}

// PublishInterceptor returns just the tracing/metrics publish interceptor, for
// callers who wire interceptors individually.
func PublishInterceptor(opts ...Option) rabbitmq.PublishInterceptor {
	cfg := newConfig(opts...)
	return cfg.publishInterceptor(newMetrics(cfg.meter))
}

// ConsumeInterceptor returns just the tracing/metrics consume interceptor.
func ConsumeInterceptor(opts ...Option) rabbitmq.ConsumeInterceptor {
	cfg := newConfig(opts...)
	return cfg.consumeInterceptor(newMetrics(cfg.meter))
}

// publishInterceptor starts a producer span, injects trace context into the
// outgoing headers, and records publish metrics.
func (c config) publishInterceptor(m *metrics) rabbitmq.PublishInterceptor {
	return func(ctx context.Context, exchange, routingKey string, msg *amqp.Publishing, next rabbitmq.PublishFunc) error {
		ctx, span := c.tracer.Start(ctx, spanName("publish", exchange, routingKey),
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				attribute.String(attrSystem, "rabbitmq"),
				attribute.String(attrDestination, exchange),
				attribute.String(attrRoutingKey, routingKey),
				attribute.String(attrOperation, "publish"),
			),
		)
		defer span.End()

		if msg.Headers == nil {
			msg.Headers = amqp.Table{}
		}
		c.propagator.Inject(ctx, tableCarrier(msg.Headers))

		err := next(ctx, exchange, routingKey, msg)
		recordOutcome(span, err)
		m.recordPublish(ctx, exchange, err)
		return err
	}
}

// consumeInterceptor extracts trace context from the delivery headers and starts
// a consumer span linked to the producer, then times and counts handling.
func (c config) consumeInterceptor(m *metrics) rabbitmq.ConsumeInterceptor {
	return func(ctx context.Context, d rabbitmq.Delivery, next rabbitmq.Handler) error {
		parent := c.propagator.Extract(ctx, tableCarrier(d.Headers))
		ctx, span := c.tracer.Start(parent, spanName("process", d.Exchange, d.RoutingKey),
			trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				attribute.String(attrSystem, "rabbitmq"),
				attribute.String(attrDestination, d.Exchange),
				attribute.String(attrRoutingKey, d.RoutingKey),
				attribute.String(attrOperation, "process"),
				attribute.Bool("messaging.rabbitmq.message.redelivered", d.Redelivered),
			),
		)
		defer span.End()

		start := time.Now()
		err := next(ctx, d)
		recordOutcome(span, err)
		m.recordConsume(ctx, d.Exchange, time.Since(start), err)
		return err
	}
}

func recordOutcome(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		span.SetAttributes(attribute.String(attrOutcome, "error"))
		return
	}
	span.SetAttributes(attribute.String(attrOutcome, "ok"))
}

func spanName(op, exchange, routingKey string) string {
	dest := exchange
	if dest == "" {
		dest = routingKey
	}
	if dest == "" {
		dest = "(default)"
	}
	return dest + " " + op
}
