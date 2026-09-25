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
| `github.com/Bugs5382/go-rabbitmq` | Everything: connect, publish, consume, declare topology. | `Connect`, `Conn`, `Option` (`WithBackoff`, `WithTLS`, `WithLogger`, `WithObserver`, `WithHeartbeat`, `WithVhost`, `WithClientProperties`, `WithPublishInterceptor`, `WithConsumeInterceptor`), `Backoff`, `DefaultBackoff`, `Publisher`, `PublishOption`, `PublisherOption` (`WithConfirms`, `WithConfirmTimeout`, ...), `Delivery`, `Handler`, `ConsumerConfig`, `Consumer` (`NewConsumer`, `Run`, `Ready`, `Status`), `ConsumerStatus`, `ConsumerState`, `ErrRequeue`/`ErrDeadLetter`, `ExchangeConfig`, `QueueConfig`, `BindingConfig`, `Topology`, `QueueType` (`QueueClassic`/`QueueQuorum`), `Logger`, `Observer`/`NopObserver`, `PublishInterceptor`/`ConsumeInterceptor`, sentinel errors. |
| `github.com/Bugs5382/go-rabbitmq/otel` | OpenTelemetry tracing + metrics. | `Instrument(...) []rabbitmq.Option`, `PublishInterceptor`, `ConsumeInterceptor`, `WithTracerProvider`, `WithMeterProvider`, `WithPropagator`. |

---

## Hard Rules (read before writing code)

1. **Connect once, share the `*Conn`.** `Connect` returns a self-healing manager that owns the underlying connection and re-dials on drop. Do **not** call `Connect` per message. Create publishers and consumers from the one `Conn`. Call `conn.Close()` on shutdown.

2. **`Connect`'s `ctx` governs only the *initial* dial.** It blocks re-dialling until the first success, honouring `ctx` (pass `context.WithTimeout` for fail-fast start-up) and `Backoff.MaxRetries`. After it returns, reconnection runs in the background for the life of the process (or until `MaxRetries`). The `ctx` you pass to `Publish`/`Consume` governs those calls, not the connection.

3. **`Publish` returns an error only after bounded retries.** It ensures a live channel, re-opens/re-declares on a drop, and retries within `Backoff`. Treat a returned error as *retryable*: keep the message (outbox) and try again later. By default it does **not** guarantee broker-side delivery — it guarantees the bytes left the client or you got an error. Add `WithConfirms()` when you need the broker's word: `Publish` then returns nil only on a broker ack (see [Publisher confirms](#publisher-confirms)).

4. **`Consume` blocks; run it in a goroutine.** It re-declares its exchange/queue/bindings and resumes after every reconnect. It returns `ctx.Err()` when your `ctx` is cancelled, or `ErrClosed` if the `Conn` is closed. Deliveries are dispatched **sequentially**; for concurrency, fan out inside your handler (respect `Prefetch` for backpressure). **Drive readiness from the consumer, not the connection:** `conn.Healthy()` stays true while a consumer retries a failing declare forever. Use `cons := conn.NewConsumer(cfg, h); go cons.Run(ctx)` and report `cons.Ready()` (see [Readiness](#readiness)).

5. **Settlement is driven by your handler's error — don't ack yourself.** Return `nil` to ack. Return an error matching `ErrRequeue` to nack with requeue, or `ErrDeadLetter` to reject without requeue (dead-lettered if the queue has `x-dead-letter-exchange`). Any other error nacks, and `RequeueOnError` (default `true`) decides requeue vs drop. Wrapping works (`errors.Is`); `ErrDeadLetter` wins if both match. A handler panic is recovered and treated as a plain error. If the channel closed while the handler ran, nothing is settled and the broker redelivers. `Delivery` is a read-only value; there is no `d.Ack()`.

6. **Quorum queues have shape rules — the library guards them.** A `QueueQuorum` queue **must be named** and may **not** be exclusive or auto-delete; it is always durable. Server-named / exclusive / auto-delete queues must be `QueueClassic`, and the library always declares them with `x-queue-type: classic` so a quorum-default broker cannot override the type. A durable, named classic queue is sent without `x-queue-type` (broker default applies); pin it with `Args: amqp.Table{"x-queue-type": "classic"}`. An invalid combo returns `ErrInvalidQueue` before touching the broker. Check with `errors.Is(err, rabbitmq.ErrInvalidQueue)`.

7. **Zero values are sensible defaults.** `ExchangeConfig{}` ⇒ durable topic. `QueueConfig{}` ⇒ durable classic. Use `.Transient()` for a non-durable exchange/queue; use `ConsumerConfig.NoRequeue()` to drop rejects instead of requeueing.

8. **On RabbitMQ 4, a transient queue must be exclusive.** RabbitMQ 4 refuses a non-durable, non-exclusive queue by default (deprecated feature `transient_nonexcl_queues`). Write `QueueConfig{Exclusive: true, AutoDelete: true}.Transient()` for a private scratch queue, or keep the queue durable and clean it up with `AutoDelete: true` or `Args: amqp.Table{"x-expires": int32(ms)}`. Never write `QueueConfig{Name: "x", AutoDelete: true}.Transient()`. The library does not block the shape (RabbitMQ 3 accepts it); a refusing broker yields `ErrInvalidQueue` wrapping the broker's `*amqp.Error`, with the fix in the message.

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

### Publisher confirms

For at-least-once delivery (for example an outbox relay that marks a row sent only after the broker has it), turn on confirms. The channel is put in confirm mode on first use and again after every reconnect.

```go
pub := conn.NewPublisher("events",
	rabbitmq.WithExchangeDeclare(rabbitmq.ExchangeConfig{Name: "events"}),
	rabbitmq.WithConfirms(),
	rabbitmq.WithConfirmTimeout(5*time.Second), // per attempt; default 30s
)

err := pub.Publish(ctx, "orders.created", body, rabbitmq.WithMessageID(row.ID))
switch {
case err == nil:
	markSent(row) // broker acked
case errors.Is(err, rabbitmq.ErrNacked):
	// broker refused it; keep the row
case errors.Is(err, rabbitmq.ErrConfirmTimeout), errors.Is(err, rabbitmq.ErrConfirmLost):
	// outcome unknown; keep the row, a retry may duplicate
}
```

| Broker answer | `Publish` returns | Retried inside `Publish`? |
|---|---|---|
| ack | `nil` | — |
| nack | `ErrNacked` | no |
| none within the confirm timeout | `ErrConfirmTimeout` | no |
| channel/connection dropped before the answer | `nil` once a retry is acked, else `ErrConfirmLost` | yes, on a fresh confirm-mode channel, within `WithPublishRetries` |
| caller `ctx` done while waiting | `ctx.Err()` | no |

Every error also matches `ErrPublishFailed`. A lost confirm is never reported as success, but the re-publish can deliver the message twice, so de-duplicate on the consumer side (for example by `MessageID`). An ack does not mean the message was routed; an unroutable message is acked too.

---

## Consume

```go
cfg := rabbitmq.ConsumerConfig{
	Exchange: rabbitmq.ExchangeConfig{Name: "events", Kind: "topic"}, // declared if Name != ""
	Queue:    rabbitmq.QueueConfig{Name: "orders", Type: rabbitmq.QueueQuorum},
	Bindings: []rabbitmq.BindingConfig{{Exchange: "events", RoutingKey: "orders.*"}}, // empty Queue ⇒ resolved queue
	Prefetch: 20,   // QoS unacked in flight (default 10)
	// AutoAck: false (default) — manual settlement driven by the handler error
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

### Readiness

`Consume` is shorthand for `NewConsumer(cfg, handler).Run(ctx)`. Build the `*Consumer` yourself when anything needs its state:

```go
cfg.OnStatus = func(st rabbitmq.ConsumerStatus) { // optional push; runs on the consumer goroutine, keep it quick
	metrics.SetConsumerState(st.Queue, st.State.String())
}
cons := conn.NewConsumer(cfg, handle)
go func() {
	if err := cons.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("consumer stopped: %v", err)
	}
}()

ready := cons.Ready()  // true only while ConsumerConsuming
st := cons.Status()    // State, Queue, Err, Attempt, RetryIn, Since
```

| `st.State` | `Ready()` | `st.Err` |
|---|---|---|
| `ConsumerStarting` | false | nil |
| `ConsumerConsuming` | true | nil |
| `ConsumerRetrying` | false | the declare/bind/QoS/consume error, or the drop; `Attempt` counts consecutive failures, `RetryIn` is the next backoff |
| `ConsumerStopped` | false | what `Run` returned (`ctx.Err()`, `ErrClosed`) |

Retries never stop on their own; the delay grows per consecutive failure (`Backoff.Initial` × `Factor`^(attempt-1), capped at `Max`) and resets after a successful session. Each retry logs a warning with the queue, error, attempt and delay. A second concurrent `Run` on the same `Consumer` returns `ErrConsumerRunning`; `Run` again after it returns is fine.

`Delivery` fields: `Body`, `RoutingKey`, `Exchange`, `ContentType`, `Headers`, `DeliveryTag`, `Redelivered`, `MessageID`, `CorrelationID`, `ReplyTo`, `Type`, `AppID`, `Priority`, `Timestamp`.

### Per-message settlement

```go
cfg := rabbitmq.ConsumerConfig{
	Queue: rabbitmq.QueueConfig{
		Name: "orders",
		Type: rabbitmq.QueueQuorum,
		Args: amqp.Table{"x-dead-letter-exchange": "events.dlx"}, // where rejects go
	},
	Bindings: []rabbitmq.BindingConfig{{Exchange: "events", RoutingKey: "orders.*"}},
}

err := conn.Consume(ctx, cfg, func(ctx context.Context, d rabbitmq.Delivery) error {
	order, err := decode(d.Body)
	if err != nil {
		return fmt.Errorf("decode: %w", rabbitmq.ErrDeadLetter) // reject, no requeue
	}
	if err := store(ctx, order); err != nil {
		if isTransient(err) {
			return fmt.Errorf("store: %w", rabbitmq.ErrRequeue) // nack, requeue
		}
		return err // plain error: nack, requeue per RequeueOnError
	}
	return nil // ack
})
```

| Handler returns | Wire call |
|---|---|
| `nil` | `basic.ack` |
| matches `ErrRequeue` | `basic.nack` requeue=true |
| matches `ErrDeadLetter` | `basic.reject` requeue=false |
| other error / panic | `basic.nack` requeue=`RequeueOnError` |

Settlement always goes to the channel that delivered the message. If that channel closed while the handler ran (for example on a reconnect), the delivery is left unsettled and the broker redelivers it, so a handler can see the same message twice. Sentinels are ignored with `AutoAck`.

To drop poison messages for the whole consumer instead of requeuing forever, use `cfg.NoRequeue()` and bind the queue to a dead-letter exchange.

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
errors.Is(err, rabbitmq.ErrPublishFailed) // publish failed (retryable); every error below also matches it
errors.Is(err, rabbitmq.ErrNacked)        // confirms: broker nacked
errors.Is(err, rabbitmq.ErrConfirmTimeout) // confirms: no answer in time (outcome unknown)
errors.Is(err, rabbitmq.ErrConfirmLost)   // confirms: channel dropped, retries spent (outcome unknown)
errors.Is(err, rabbitmq.ErrConsumerRunning) // Consumer.Run called while that Consumer already runs
errors.Is(err, rabbitmq.ErrInvalidQueue)  // bad queue shape (e.g. exclusive quorum queue, or a transient non-exclusive queue on RabbitMQ 4)

// returned by a Handler to pick the settlement of one message
return fmt.Errorf("...: %w", rabbitmq.ErrRequeue)    // nack, requeue
return fmt.Errorf("...: %w", rabbitmq.ErrDeadLetter) // reject, dead-letter
```

---

## Deeper docs

- API reference: <https://pkg.go.dev/github.com/Bugs5382/go-rabbitmq>
- OTel adapter: <https://pkg.go.dev/github.com/Bugs5382/go-rabbitmq/otel>
- Runnable examples: `example_test.go`.
