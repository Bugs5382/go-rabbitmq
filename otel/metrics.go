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

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metrics holds the instruments the adapter records. Instrument creation errors
// are tolerated by falling back to no-op instruments, so a misconfigured meter
// never breaks message flow.
type metrics struct {
	published metric.Int64Counter
	consumed  metric.Int64Counter
	duration  metric.Float64Histogram
}

func newMetrics(m metric.Meter) *metrics {
	// On the rare instrument-creation error the field is left nil; the record
	// helpers nil-check, so a misconfigured meter degrades to no metrics rather
	// than breaking message flow.
	out := &metrics{}
	if c, err := m.Int64Counter("rabbitmq.publish.count",
		metric.WithDescription("Number of messages published, by outcome.")); err == nil {
		out.published = c
	}
	if c, err := m.Int64Counter("rabbitmq.consume.count",
		metric.WithDescription("Number of messages consumed, by outcome.")); err == nil {
		out.consumed = c
	}
	if h, err := m.Float64Histogram("rabbitmq.consume.duration",
		metric.WithDescription("Handler duration for consumed messages."),
		metric.WithUnit("s")); err == nil {
		out.duration = h
	}
	return out
}

func outcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}

func (m *metrics) recordPublish(ctx context.Context, exchange string, err error) {
	if m.published == nil {
		return
	}
	m.published.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrDestination, exchange),
		attribute.String(attrOutcome, outcome(err)),
	))
}

func (m *metrics) recordConsume(ctx context.Context, exchange string, d time.Duration, err error) {
	attrs := metric.WithAttributes(
		attribute.String(attrDestination, exchange),
		attribute.String(attrOutcome, outcome(err)),
	)
	if m.consumed != nil {
		m.consumed.Add(ctx, 1, attrs)
	}
	if m.duration != nil {
		m.duration.Record(ctx, d.Seconds(), attrs)
	}
}
