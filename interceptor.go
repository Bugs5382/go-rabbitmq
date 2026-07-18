package rabbitmq

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

	amqp "github.com/rabbitmq/amqp091-go"
)

// PublishFunc performs the actual send of a prepared message. It is the "next"
// link a PublishInterceptor calls.
type PublishFunc func(ctx context.Context, exchange, routingKey string, msg *amqp.Publishing) error

// PublishInterceptor wraps a publish. It runs once per Publish call (not per
// internal retry) and may inspect or mutate msg before calling next -- for
// example to inject W3C trace-context headers -- and observe the outcome next
// returns. Interceptors run outermost-first in the order registered.
//
// It is the seam the OTel adapter uses to start a producer span, inject trace
// context into the message headers, and record publish metrics, without the core
// package depending on any telemetry library.
type PublishInterceptor func(ctx context.Context, exchange, routingKey string, msg *amqp.Publishing, next PublishFunc) error

// ConsumeInterceptor wraps handling of a single delivery. It may extract trace
// context from d.Headers, derive a new ctx (for example one carrying a consumer
// span), and pass it to next. Interceptors run outermost-first in the order
// registered; the innermost next is the user's Handler.
//
// It is the seam the OTel adapter uses to continue a distributed trace across the
// broker and to time and count message handling.
type ConsumeInterceptor func(ctx context.Context, d Delivery, next Handler) error

// chainConsume composes consume interceptors around a handler, preserving order
// (the first interceptor is outermost).
func chainConsume(handler Handler, interceptors []ConsumeInterceptor) Handler {
	for i := len(interceptors) - 1; i >= 0; i-- {
		ic := interceptors[i]
		next := handler
		handler = func(ctx context.Context, d Delivery) error {
			return ic(ctx, d, next)
		}
	}
	return handler
}
