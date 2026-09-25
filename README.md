# go-rabbitmq 🐇

> Pure-Go RabbitMQ with best-practice **auto-reconnecting** connections, publishers, and consumers — so you stop re-solving connection resilience in every service. Optional OpenTelemetry tracing + metrics in one line.

## 📦 Install

```bash
go get github.com/Bugs5382/go-rabbitmq
```

One module, two import paths — the core (`github.com/Bugs5382/go-rabbitmq`, depends only on `amqp091-go`) and the optional OTel adapter (`github.com/Bugs5382/go-rabbitmq/otel`).

## 🚀 Usage

`Connect` owns the connection, watches it, and transparently re-dials with bounded exponential backoff — the one thing naive AMQP code always gets wrong is the default here.

```go
conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
if err != nil {
	log.Fatal(err)
}
defer conn.Close()
```

## 📤 Publish

A publisher re-opens its channel and re-declares its exchange after a drop, and retries within the backoff budget. `Publish` returns an error only once that budget is spent — so an outbox worker can keep the message and try later.

```go
pub := conn.NewPublisher("events")
if err := pub.PublishJSON(ctx, "order.created", order); err != nil {
	// retryable: keep the row, try again later
}
```

By default a nil error means the message left the client. Add `WithConfirms()` and `Publish` waits for the broker's publisher confirm, so nil means the broker acked it. The channel goes back into confirm mode after every reconnect.

```go
pub := conn.NewPublisher("events", rabbitmq.WithConfirms())
err := pub.PublishJSON(ctx, "order.created", order, rabbitmq.WithMessageID(id))
switch {
case err == nil:
	// acked: safe to mark the outbox row sent
case errors.Is(err, rabbitmq.ErrNacked):
	// the broker refused it
case errors.Is(err, rabbitmq.ErrConfirmTimeout), errors.Is(err, rabbitmq.ErrConfirmLost):
	// outcome unknown: keep the row; a retry may duplicate
}
```

A nack or a confirm timeout (`WithConfirmTimeout`, default 30s) is returned straight away. If the channel or connection drops while a confirm is outstanding, the message is re-published on a fresh channel within the retry budget, and `ErrConfirmLost` is returned once that is spent. A lost confirm is never reported as success. Delivery is at least once, so consumers should de-duplicate.

## 📥 Consume

A consumer re-declares its topology and resumes after any reconnect, so it survives broker restarts. Your handler's returned error decides how each message is settled:

| Handler returns | Settlement |
|---|---|
| `nil` | ack |
| an error matching `rabbitmq.ErrRequeue` | nack with requeue |
| an error matching `rabbitmq.ErrDeadLetter` | reject without requeue: dead-lettered via the queue's `x-dead-letter-exchange`, or dropped if it has none |
| any other error (or a panic) | nack; requeued unless the config is `NoRequeue()` |

Wrap the sentinels to keep the cause (`fmt.Errorf("decode: %w", rabbitmq.ErrDeadLetter)`); if an error matches both, dead-letter wins. If the channel drops while the handler runs, the message is not settled at all, because its delivery tag died with the channel. The broker redelivers it after the reconnect.

```go
conn.Consume(ctx, rabbitmq.ConsumerConfig{
	Exchange: rabbitmq.ExchangeConfig{Name: "events"},
	Queue: rabbitmq.QueueConfig{
		Name: "orders",
		Type: rabbitmq.QueueQuorum,
		Args: amqp.Table{"x-dead-letter-exchange": "events.dlx"},
	},
	Bindings: []rabbitmq.BindingConfig{{Exchange: "events", RoutingKey: "order.*"}},
}, func(ctx context.Context, d rabbitmq.Delivery) error {
	order, err := decode(d.Body)
	if err != nil {
		return fmt.Errorf("decode: %w", rabbitmq.ErrDeadLetter) // poison: never retry
	}
	if err := store(ctx, order); errors.Is(err, errUnavailable) {
		return fmt.Errorf("store: %w", rabbitmq.ErrRequeue) // transient: try again
	}
	return err // nil acks
})
```

### Readiness

`Conn.Healthy()` only says the connection is up. A consumer whose queue declare keeps failing retries on a healthy connection and consumes nothing, so drive readiness from the consumer. `NewConsumer` returns a `*Consumer` you run yourself; `Consume` is shorthand for `NewConsumer(cfg, handler).Run(ctx)`.

```go
cons := conn.NewConsumer(cfg, handle)
go func() { _ = cons.Run(ctx) }()

http.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
	if !cons.Ready() {
		st := cons.Status() // State, Queue, Err, Attempt, RetryIn, Since
		http.Error(w, fmt.Sprintf("%s: %v", st.State, st.Err), http.StatusServiceUnavailable)
		return
	}
})
```

| State | Ready | Meaning |
|---|---|---|
| `ConsumerStarting` | no | not run yet, or setting up its first session |
| `ConsumerConsuming` | yes | topology declared, the broker is delivering |
| `ConsumerRetrying` | no | a declare, bind, QoS or consume step failed, or the channel or connection dropped; `Err` is the last error |
| `ConsumerStopped` | no | `Run` returned; `Err` is why |

Retries back off with the connection's `Backoff`, growing with each consecutive failure up to `Max`, and each one is logged with the queue, the error, the attempt and the delay. Set `ConsumerConfig.OnStatus` to be told about every change instead of polling. It runs on the consumer's goroutine, so keep it quick.

Ephemeral (server-named / exclusive / auto-delete) queues are declared with `x-queue-type: classic`, so a broker with `default_queue_type = quorum` can't turn them into quorum queues and fail the declare with `PRECONDITION_FAILED`. A quorum queue can't have any of those shapes, and the guard rejects that combination before it reaches the broker. A durable, named classic queue is declared without `x-queue-type` and follows the broker default; set `Args: amqp.Table{"x-queue-type": "classic"}` to pin it.

RabbitMQ 4 refuses a transient queue that is not exclusive (the deprecated `transient_nonexcl_queues` feature) unless the broker operator permits it again. On RabbitMQ 4, use `.Transient()` only with `Exclusive: true`, or keep the queue durable and let `AutoDelete` or an `x-expires` TTL clean it up:

```go
// exclusive and transient: fine on every broker
rabbitmq.QueueConfig{Exclusive: true, AutoDelete: true}.Transient()
// durable, removed when the last consumer leaves: fine on every broker
rabbitmq.QueueConfig{Name: "jobs.scratch", AutoDelete: true}
```

The library still sends a transient non-exclusive queue as asked, since RabbitMQ 3 accepts it. When a broker refuses it, the declare returns `ErrInvalidQueue` (wrapping the broker's error) with the fix in the message.

## 📊 OpenTelemetry

The optional `otel` subpackage propagates W3C trace context through message headers (producer → consumer spans across the broker) and records publish/consume metrics. The core stays telemetry-free; wire it with one call:

```go
conn, _ := rabbitmq.Connect(ctx, url, rmqotel.Instrument()...)
```

## 🛠 Develop

```bash
task ci        # build + vet + lint + test
task license   # inject MIT headers (golic)
```

## ⚖️ License

MIT © 2026 Shane
