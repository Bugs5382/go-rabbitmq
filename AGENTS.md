# AGENTS.md — go-rabbitmq for AI coding agents

> Dense, example-first reference for using `go-rabbitmq` **as a dependency**. Every code block compiles against the shipping API (Go ≥ 1.26). If you are an agent generating code that imports this library, read this file first and copy the patterns verbatim — the non-obvious rules in [Hard Rules](#hard-rules-read-before-writing-code) are where naive code breaks.

Module path: `github.com/Bugs5382/go-rabbitmq`. Core depends only on `github.com/rabbitmq/amqp091-go`.

```sh
go get github.com/Bugs5382/go-rabbitmq
```

---

## Package map

| Import path | What you use it for | Key exports |
|---|---|---|
| `github.com/Bugs5382/go-rabbitmq` | Everything: connect, publish, consume, declare topology. | `Connect`, `Conn`, `Option` (`WithBackoff`, `WithTLS`, `WithLogger`, `WithObserver`, `WithHeartbeat`, `WithVhost`, `WithClientProperties`, `WithPublishInterceptor`, `WithConsumeInterceptor`), `Backoff`, `DefaultBackoff`, `Publisher`, `PublishOption`, `PublisherOption`, `Delivery`, `Handler`, `ConsumerConfig`, `ExchangeConfig`, `QueueConfig`, `BindingConfig`, `Topology`, `QueueType` (`QueueClassic`/`QueueQuorum`), `Logger`, `Observer`/`NopObserver`, `PublishInterceptor`/`ConsumeInterceptor`, sentinel errors. |
| `github.com/Bugs5382/go-rabbitmq/otel` | OpenTelemetry tracing + metrics. | `Instrument(...) []rabbitmq.Option`, `PublishInterceptor`, `ConsumeInterceptor`, `WithTracerProvider`, `WithMeterProvider`, `WithPropagator`. |

---

## Hard Rules (read before writing code)

1. **Connect once, share the `*Conn`.** `Connect` returns a self-healing manager that owns the underlying connection and re-dials on drop. Do **not** call `Connect` per message. Create publishers and consumers from the one `Conn`. Call `conn.Close()` on shutdown.

2. **`Connect`'s `ctx` governs only the *initial* dial.** It blocks re-dialling until the first success, honouring `ctx` (pass `context.WithTimeout` for fail-fast start-up) and `Backoff.MaxRetries`. After it returns, reconnection runs in the background for the life of the process (or until `MaxRetries`). The `ctx` you pass to `Publish`/`Consume` governs those calls, not the connection.

3. **`Publish` returns an error only after bounded retries.** It ensures a live channel, re-opens/re-declares on a drop, and retries within `Backoff`. Treat a returned error as *retryable*: keep the message (outbox) and try again later. It does **not** guarantee broker-side delivery (no publisher confirms yet) — it guarantees the bytes left the client or you got an error.

4. **`Consume` blocks; run it in a goroutine.** It re-declares its exchange/queue/bindings and resumes after every reconnect. It returns `ctx.Err()` when your `ctx` is cancelled, or `ErrClosed` if the `Conn` is closed. Deliveries are dispatched **sequentially**; for concurrency, fan out inside your handler (respect `Prefetch` for backpressure).

5. **Ack is driven by your handler's error — don't ack yourself.** Return `nil` to ack, an error to nack. `RequeueOnError` (default `true`) decides requeue vs drop. A handler panic is recovered and treated as an error. `Delivery` is a read-only value; there is no `d.Ack()`.

6. **Quorum queues have shape rules — the library guards them.** A `QueueQuorum` queue **must be named** and may **not** be exclusive or auto-delete; it is always durable. Server-named / exclusive / auto-delete queues must be `QueueClassic`, and the library always declares them with `x-queue-type: classic` so a quorum-default broker cannot override the type. A durable, named classic queue is sent without `x-queue-type` (broker default applies); pin it with `Args: amqp.Table{"x-queue-type": "classic"}`. An invalid combo returns `ErrInvalidQueue` before touching the broker. Check with `errors.Is(err, rabbitmq.ErrInvalidQueue)`.

7. **Zero values are sensible defaults.** `ExchangeConfig{}` ⇒ durable topic. `QueueConfig{}` ⇒ durable classic. Use `.Transient()` for a non-durable exchange/queue; use `ConsumerConfig.NoRequeue()` to drop rejects instead of requeueing.

---

## Connect

```go
import "github.com/Bugs5382/go-rabbitmq"

ctx := context.Background()
conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/",
	rabbitmq.WithBackoff(rabbitmq.Backoff{Initial: time.Second, Max: time.Minute, Factor: 2, Jitter: 0.3}),
	rabbitmq.WithHeartbeat(10*time.Second),
	rabbitmq.WithLogger(myLogger), // optional; default no-op
)
if err != nil { /* bad URL or broker unreachable within ctx/MaxRetries */ }
defer conn.Close()

_ = conn.Healthy()    // live connection available right now?
_ = conn.Reconnects() // successful reconnects since Connect
```

TLS/mTLS: pass `rabbitmq.WithTLS(&tls.Config{...})` and an `amqps://` URL.

---

## Publish

```go
pub := conn.NewPublisher("events",
	rabbitmq.WithExchangeDeclare(rabbitmq.ExchangeConfig{Name: "events", Kind: "topic"}), // declare on (re)connect
	rabbitmq.WithPublishRetries(3),           // bounded retries per Publish (default 3)
	rabbitmq.WithPersistentDefault(true),     // delivery mode 2 (default)
	rabbitmq.WithDefaultContentType("application/json"),
)

// raw bytes
err := pub.Publish(ctx, "orders.created", []byte(`{"id":1}`),
	rabbitmq.WithMessageID("m-1"),
	rabbitmq.WithCorrelationID("c-1"),
	rabbitmq.WithHeaders(amqp.Table{"x-source": "orders-svc"}),
	rabbitmq.WithPersistent(true),
)

// JSON convenience (marshals, sets application/json)
err = pub.PublishJSON(ctx, "orders.created", order)

if errors.Is(err, rabbitmq.ErrPublishFailed) {
	// retries exhausted — keep the message and try later
}
```

Per-message `PublishOption`s: `WithContentType`, `WithHeaders`, `WithPersistent`, `WithMessageID`, `WithCorrelationID`, `WithReplyTo`, `WithExpiration`, `WithPriority`, `WithType`, `WithAppID`. An empty exchange name (`conn.NewPublisher("")`) targets the default exchange, where the routing key is the queue name.

---

## Consume

```go
cfg := rabbitmq.ConsumerConfig{
	Exchange: rabbitmq.ExchangeConfig{Name: "events", Kind: "topic"}, // declared if Name != ""
	Queue:    rabbitmq.QueueConfig{Name: "orders", Type: rabbitmq.QueueQuorum},
	Bindings: []rabbitmq.BindingConfig{{Exchange: "events", RoutingKey: "orders.*"}}, // empty Queue ⇒ resolved queue
	Prefetch: 20,   // QoS unacked in flight (default 10)
	// AutoAck: false (default) — manual ack driven by the handler error
}

go func() {
	err := conn.Consume(ctx, cfg, func(ctx context.Context, d rabbitmq.Delivery) error {
		var order Order
		if err := json.Unmarshal(d.Body, &order); err != nil {
			return err // nack; requeued unless cfg.NoRequeue()
		}
		return process(ctx, order) // nil ⇒ ack
	})
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("consumer stopped: %v", err)
	}
}()
```

`Delivery` fields: `Body`, `RoutingKey`, `Exchange`, `ContentType`, `Headers`, `DeliveryTag`, `Redelivered`, `MessageID`, `CorrelationID`, `ReplyTo`, `Type`, `AppID`, `Priority`, `Timestamp`.

To drop poison messages instead of requeuing forever, use `cfg.NoRequeue()` and bind the queue to a dead-letter exchange.

---

## Declare topology

```go
// all at once, idempotent
err := conn.DeclareTopology(ctx, rabbitmq.Topology{
	Exchanges: []rabbitmq.ExchangeConfig{{Name: "events", Kind: "topic"}},
	Queues:    []rabbitmq.QueueConfig{{Name: "orders", Type: rabbitmq.QueueQuorum}},
	Bindings:  []rabbitmq.BindingConfig{{Queue: "orders", Exchange: "events", RoutingKey: "orders.*"}},
})

// or piecemeal
_ = conn.DeclareExchange(ctx, rabbitmq.ExchangeConfig{Name: "events"})
q, _ := conn.DeclareQueue(ctx, rabbitmq.QueueConfig{Name: "orders"}) // q.Name is authoritative for server-named
_ = conn.BindQueue(ctx, rabbitmq.BindingConfig{Queue: q.Name, Exchange: "events", RoutingKey: "orders.*"})
```

---

## Observability

**OpenTelemetry (recommended)** — tracing propagated through message headers + metrics, one line:

```go
import rmqotel "github.com/Bugs5382/go-rabbitmq/otel"

conn, err := rabbitmq.Connect(ctx, url, rmqotel.Instrument()...) // uses global providers
```

**Custom metrics without OTel** — implement `Observer` and pass `rabbitmq.WithObserver(obs)`. Methods: `OnConnect`, `OnDisconnect(err)`, `OnReconnect(attempt)`, `OnPublish(exchange, key, err)`, `OnConsume(queue, d, err)`. Embed `rabbitmq.NopObserver` to implement only some.

**Custom interceptors** — `WithPublishInterceptor` / `WithConsumeInterceptor` wrap publish/handle with context, can mutate headers, and are exactly how the otel adapter is built.

---

## Errors

```go
errors.Is(err, rabbitmq.ErrClosed)        // Conn was closed
errors.Is(err, rabbitmq.ErrNotReady)      // no live connection before ctx expired
errors.Is(err, rabbitmq.ErrPublishFailed) // publish retries exhausted (retryable)
errors.Is(err, rabbitmq.ErrInvalidQueue)  // bad queue shape (e.g. exclusive quorum queue)
```

---

## Deeper docs

- API reference: <https://pkg.go.dev/github.com/Bugs5382/go-rabbitmq>
- OTel adapter: <https://pkg.go.dev/github.com/Bugs5382/go-rabbitmq/otel>
- Runnable examples: `example_test.go`.
