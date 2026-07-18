# 🐇 go-rabbitmq

> A pure-Go RabbitMQ library with best-practice **auto-reconnecting** connections, publishers, and consumers — so you stop re-solving connection resilience in every service.

[![Go Reference](https://pkg.go.dev/badge/github.com/Bugs5382/go-rabbitmq.svg)](https://pkg.go.dev/github.com/Bugs5382/go-rabbitmq)
[![Go Report Card](https://goreportcard.com/badge/github.com/Bugs5382/go-rabbitmq)](https://goreportcard.com/report/github.com/Bugs5382/go-rabbitmq)
[![CI](https://github.com/Bugs5382/go-rabbitmq/actions/workflows/job-go-lang-ci.yaml/badge.svg)](https://github.com/Bugs5382/go-rabbitmq/actions/workflows/job-go-lang-ci.yaml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)

`go-rabbitmq` wraps [`rabbitmq/amqp091-go`](https://github.com/rabbitmq/amqp091-go) and makes the one thing naive AMQP code always gets wrong — surviving a connection or channel drop — the **default**, not an afterthought.

The bug this library exists to kill: a hand-rolled publisher or consumer opens one connection and one channel, works perfectly, then the broker restarts (or a network blip closes the socket) and the program is **stuck forever** — its channel is dead and nothing re-opens it. `go-rabbitmq` owns the connection, watches it, and transparently re-dials with **bounded exponential backoff**. Publishers re-open and re-declare on demand; consumers re-declare their topology and resume.

## ✨ What you get

- 🔁 **Self-healing connection manager** — owns the connection, watches `NotifyClose`, re-dials with bounded exponential backoff + jitter. Exposes `Healthy()` and `Reconnects()`.
- 📤 **Resilient publisher** — ensures a live channel, re-declares its exchange after a drop, and retries within bounded backoff. Returns an error only after the retry budget is spent, so an **outbox worker** can keep the message and try later. JSON + persistent delivery by default.
- 📥 **Resilient consumer** — re-declares its queue and bindings and resumes consuming after any reconnect. Manual ack driven by your handler's returned error (requeue configurable). Survives broker restarts.
- 🧱 **Idempotent topology helpers** — declare exchanges, queues, and bindings; classic **and** quorum queues, with the quorum footguns guarded.
- 🔌 **Pluggable logger and observer** — a minimal `Logger` interface (default no-op) and an `Observer` for metrics (default no-op). No forced logging/telemetry dependency.
- 📊 **First-class OpenTelemetry** — an optional [`otel`](./otel) subpackage adds distributed tracing (context propagated through message headers) and metrics in one line. The core package stays dependency-light.
- 🧪 **Testable core** — a dialer/channel seam means the reconnect state machine is unit-tested with fakes; CI is green **without** a broker.

## 📦 Install

```sh
go get github.com/Bugs5382/go-rabbitmq
```

Single module, two import paths:

| Import path | What it is |
|---|---|
| [`github.com/Bugs5382/go-rabbitmq`](https://pkg.go.dev/github.com/Bugs5382/go-rabbitmq) | The core: `Conn`, `Publisher`, `Consume`, topology helpers. Depends only on `amqp091-go`. |
| [`github.com/Bugs5382/go-rabbitmq/otel`](https://pkg.go.dev/github.com/Bugs5382/go-rabbitmq/otel) | The OpenTelemetry adapter (tracing + metrics). Pull it in only if you want it. |

## 🧭 At a glance

```mermaid
flowchart LR
    App["your code"] --> Pub["Publisher.Publish"]
    App --> Con["Conn.Consume"]
    Pub --> Conn["Conn (connection manager)"]
    Con --> Conn
    Conn -->|"auto-reconnect + backoff"| Broker[("RabbitMQ")]
    Conn -. "NotifyClose watched, re-dial" .-> Conn
```

## 🚀 60-second example

**Connect and publish** — auto-reconnect is already on:

```go
package main

import (
	"context"
	"log"

	"github.com/Bugs5382/go-rabbitmq"
)

func main() {
	ctx := context.Background()

	conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	pub := conn.NewPublisher("events",
		rabbitmq.WithExchangeDeclare(rabbitmq.ExchangeConfig{Name: "events", Kind: "topic"}),
	)

	order := map[string]any{"id": 42, "total": 19.99}
	if err := pub.PublishJSON(ctx, "orders.created", order); err != nil {
		// Not accepted after bounded retries — keep it and retry later (outbox).
		log.Printf("publish failed, will retry: %v", err)
	}
}
```

**Consume** — re-declares topology and resumes after any reconnect:

```go
cfg := rabbitmq.ConsumerConfig{
	Exchange: rabbitmq.ExchangeConfig{Name: "events", Kind: "topic"},
	Queue:    rabbitmq.QueueConfig{Name: "orders", Type: rabbitmq.QueueQuorum},
	Bindings: []rabbitmq.BindingConfig{{Exchange: "events", RoutingKey: "orders.*"}},
	Prefetch: 20,
}

// Consume blocks until ctx is cancelled; run it in its own goroutine.
go func() {
	err := conn.Consume(ctx, cfg, func(_ context.Context, d rabbitmq.Delivery) error {
		log.Printf("received %s: %s", d.RoutingKey, d.Body)
		return nil // nil acks; a returned error nacks (requeue configurable)
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("consumer stopped: %v", err)
	}
}()
```

## 🧱 Topology: classic vs quorum

Declare a whole topology idempotently in one call:

```go
err := conn.DeclareTopology(ctx, rabbitmq.Topology{
	Exchanges: []rabbitmq.ExchangeConfig{{Name: "events", Kind: "topic"}},
	Queues:    []rabbitmq.QueueConfig{{Name: "orders", Type: rabbitmq.QueueQuorum}},
	Bindings:  []rabbitmq.BindingConfig{{Queue: "orders", Exchange: "events", RoutingKey: "orders.*"}},
})
```

The zero value declares a **durable topic exchange** and a **durable classic queue** — the common default. A real gotcha this library guards for you: **quorum queues must be named and may not be exclusive or auto-delete**. Server-named / exclusive / auto-delete queues must be **classic**. Ask for an invalid combination and you get `ErrInvalidQueue` before it ever hits the broker.

## 🔁 Tuning reconnect / retry

```go
conn, err := rabbitmq.Connect(ctx, url,
	rabbitmq.WithBackoff(rabbitmq.Backoff{
		Initial:    time.Second,
		Max:        time.Minute,
		Factor:     2.0,
		Jitter:     0.3,
		MaxRetries: 0, // 0 = retry forever (right for a long-lived connection)
	}),
	rabbitmq.WithHeartbeat(10*time.Second),
)
```

`Connect` honours the passed `ctx` for the *initial* dial, so pass a `context.WithTimeout` if you want start-up to fail fast on a bad URL. After that, drops are handled in the background forever (or up to `MaxRetries`).

## 🔒 TLS / mTLS

```go
conn, err := rabbitmq.Connect(ctx, "amqps://user:pass@rabbit.example.com:5671/",
	rabbitmq.WithTLS(&tls.Config{
		RootCAs:      caPool,                    // trust the broker's issuer
		Certificates: []tls.Certificate{client}, // present a client cert (mTLS)
		MinVersion:   tls.VersionTLS12,
	}),
)
```

## 📊 OpenTelemetry (tracing + metrics)

Distributed tracing across the broker in one line — the adapter starts a producer span on publish, injects W3C trace context into the message headers, and continues the trace with a consumer span on the other side. It uses the **globally registered** OpenTelemetry providers by default, so if your service already sets those up at start-up, this just works:

```go
import (
	"github.com/Bugs5382/go-rabbitmq"
	rmqotel "github.com/Bugs5382/go-rabbitmq/otel"
)

conn, err := rabbitmq.Connect(ctx, url, rmqotel.Instrument()...)
```

Override providers for tests or non-global setups with `rmqotel.WithTracerProvider`, `WithMeterProvider`, and `WithPropagator`. The adapter also records `rabbitmq.publish.count`, `rabbitmq.consume.count`, and a `rabbitmq.consume.duration` histogram.

Prefer to plug your own metrics without OpenTelemetry? Implement the tiny `Observer` interface and pass `rabbitmq.WithObserver(...)`.

## 🧾 Logging

```go
// Logger is a 4-method interface; adapt slog, zerolog, or a house logger.
conn, err := rabbitmq.Connect(ctx, url, rabbitmq.WithLogger(myLogger))
```

The default is a no-op — the library is silent unless you opt in. Passwords in the AMQP URL are always redacted before they reach a log line.

## 🧪 Testing & integration

Unit tests need no broker — the reconnect state machine, publish retry loop, consumer resume, and quorum/classic guard are all exercised with in-memory fakes. An optional integration test runs against a real broker behind a build tag:

```sh
docker run -d --rm -p 5672:5672 rabbitmq:3-management
RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ go test -tags integration -run Integration ./...
```

## 📋 Requirements

- Go **1.26+**

## 📄 License

[MIT](./LICENSE)
