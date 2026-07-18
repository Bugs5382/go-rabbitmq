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

import amqp "github.com/rabbitmq/amqp091-go"

// tableCarrier adapts an AMQP header table to the OpenTelemetry
// propagation.TextMapCarrier interface, so trace context can be injected into and
// extracted from message headers. Only string-valued headers participate in
// propagation, which is what the W3C tracecontext propagator emits.
type tableCarrier amqp.Table

// Get returns the value of a header, or "" if absent or non-string.
func (c tableCarrier) Get(key string) string {
	if c == nil {
		return ""
	}
	if v, ok := c[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// Set stores a header value.
func (c tableCarrier) Set(key, value string) {
	if c != nil {
		c[key] = value
	}
}

// Keys returns the header names present in the table.
func (c tableCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}
