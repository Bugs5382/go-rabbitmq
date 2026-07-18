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

## 📥 Consume

A consumer re-declares its topology and resumes after any reconnect. Your handler's returned error drives the ack (requeue configurable), so it survives broker restarts.

```go
conn.Consume(ctx, rabbitmq.ConsumerConfig{
	Queue:    rabbitmq.QueueConfig{Name: "orders", Type: rabbitmq.QueueQuorum},
	Exchange: "events",
	Bindings: []string{"order.*"},
}, func(ctx context.Context, d rabbitmq.Delivery) error {
	return handle(d.Body)
})
```

Ephemeral (server-named / exclusive / auto-delete) queues are forced to classic — quorum queues can't be any of those, and the guard saves you the `PRECONDITION_FAILED`.

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
