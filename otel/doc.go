// Package otel is the OpenTelemetry adapter for go-rabbitmq. It provides
// distributed tracing and metrics for publishers and consumers by plugging into
// the go-rabbitmq interceptor seam -- so the core rabbitmq package stays free of
// any telemetry dependency, and only programs that import this subpackage pull in
// OpenTelemetry.
//
// It builds on the standard OpenTelemetry API. By default it uses the globally
// registered TracerProvider, MeterProvider and TextMapPropagator, which a base
// telemetry setup (for example one wired with go-otel) configures at start-up, so
// tracing and metrics work with a single call and no further wiring:
//
//	import (
//		"github.com/Bugs5382/go-rabbitmq"
//		rmqotel "github.com/Bugs5382/go-rabbitmq/otel"
//	)
//
//	conn, err := rabbitmq.Connect(ctx, url, rmqotel.Instrument()...)
//
// On publish it starts a producer span and injects W3C trace context into the
// message headers; on consume it extracts that context and starts a consumer span
// linked to the producer, so a trace flows across the broker. It also records
// publish and consume counters and a handler-duration histogram.
//
// Providers can be overridden with WithTracerProvider, WithMeterProvider and
// WithPropagator for tests or non-global setups.
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
