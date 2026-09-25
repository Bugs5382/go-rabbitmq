// Package rabbitmq is a small, dependency-light RabbitMQ client built for
// resilience. It wraps github.com/rabbitmq/amqp091-go and makes the thing every
// naive AMQP program gets wrong -- surviving a connection or channel drop -- the
// default behaviour rather than an afterthought.
//
// # Why
//
// A hand-rolled publisher or consumer that opens one connection and one channel
// works perfectly until the broker restarts, a network blip closes the socket,
// or the channel is killed by a protocol error. After that the naive program is
// stuck forever: its channel is dead and nothing re-opens it. This package owns
// the connection, watches it for closes, and transparently re-dials with bounded
// exponential backoff. Publishers re-open and re-declare on demand; consumers
// re-declare their topology and resume consuming after a reconnect.
//
// # Pieces
//
//   - Conn is the connection manager. Connect returns one; it re-dials in the
//     background and exposes health via IsClosed, Healthy and Reconnects.
//   - Publisher is created from a Conn and an exchange. Publish ensures a live
//     channel and retries within bounded backoff before returning an error, so an
//     outbox worker can simply try again later. WithConfirms makes Publish wait
//     for the broker's publisher confirm, for at-least-once delivery.
//   - Consume runs a consumer that re-declares its queue and bindings and resumes
//     after any reconnect, with manual acknowledgement driven by the handler's
//     returned error.
//   - The topology helpers (DeclareExchange, DeclareQueue, BindQueue,
//     DeclareTopology) declare exchanges, queues and bindings idempotently and
//     guard the quorum-vs-classic queue rules.
//
// # Pluggable cross-cutting concerns
//
// Logging goes through the minimal Logger interface (default no-op) so callers can
// plug slog, zerolog or anything else without this package depending on them.
// Observability goes through the Observer interface (default no-op) so an
// OpenTelemetry-backed implementation can be supplied without adding an
// OpenTelemetry dependency here.
//
// # Minimal example
//
//	ctx := context.Background()
//	conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
//	if err != nil {
//		return err
//	}
//	defer conn.Close()
//
//	pub := conn.NewPublisher("events")
//	if err := pub.PublishJSON(ctx, "orders.created", order); err != nil {
//		return err // retry later; the message was not accepted
//	}
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
