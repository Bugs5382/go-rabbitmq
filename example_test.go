package rabbitmq_test

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
	"errors"
	"log"
	"net/http"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
)

// ExampleConnect shows the minimal path: connect (with auto-reconnect on by
// default), publish a JSON event, and clean up.
func ExampleConnect() {
	ctx := context.Background()
	conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	pub := conn.NewPublisher("events",
		rabbitmq.WithExchangeDeclare(rabbitmq.ExchangeConfig{Name: "events", Kind: "topic"}),
	)
	order := map[string]any{"id": 42, "total": 19.99}
	if err := pub.PublishJSON(ctx, "orders.created", order); err != nil {
		// The broker did not accept the message after bounded retries; keep it and
		// try again later (for example from an outbox).
		log.Printf("publish failed, will retry: %v", err)
	}
}

// ExampleConn_Consume shows a resilient consumer that re-declares its topology
// and resumes after any reconnect. The handler's returned error drives ack/nack.
func ExampleConn_Consume() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

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
			return nil // returning nil acks; returning an error nacks
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("consumer stopped: %v", err)
		}
	}()

	time.Sleep(time.Second)
}

// ExampleConn_NewConsumer drives a readiness probe from the consumer itself, so
// a queue declare that keeps failing reads as not ready even though the
// connection is healthy.
func ExampleConn_NewConsumer() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	cons := conn.NewConsumer(rabbitmq.ConsumerConfig{
		Queue: rabbitmq.QueueConfig{Name: "orders", Type: rabbitmq.QueueQuorum},
		OnStatus: func(st rabbitmq.ConsumerStatus) {
			if st.State == rabbitmq.ConsumerRetrying {
				log.Printf("consumer on %s retrying (attempt %d, next in %s): %v", st.Queue, st.Attempt, st.RetryIn, st.Err)
			}
		},
	}, func(_ context.Context, _ rabbitmq.Delivery) error { return nil })
	go func() { _ = cons.Run(ctx) }()

	http.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !cons.Ready() {
			http.Error(w, cons.Status().State.String(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}

// ExampleConn_DeclareTopology declares an exchange, a durable quorum queue and a
// binding in one call, ready for publishers and consumers to use.
func ExampleConn_DeclareTopology() {
	ctx := context.Background()
	conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	err = conn.DeclareTopology(ctx, rabbitmq.Topology{
		Exchanges: []rabbitmq.ExchangeConfig{{Name: "events", Kind: "topic"}},
		Queues:    []rabbitmq.QueueConfig{{Name: "orders", Type: rabbitmq.QueueQuorum}},
		Bindings:  []rabbitmq.BindingConfig{{Queue: "orders", Exchange: "events", RoutingKey: "orders.*"}},
	})
	if err != nil {
		log.Fatal(err)
	}
}

// ExampleWithBackoff tunes the reconnect/retry policy.
func ExampleWithBackoff() {
	ctx := context.Background()
	conn, err := rabbitmq.Connect(ctx, "amqp://guest:guest@localhost:5672/",
		rabbitmq.WithBackoff(rabbitmq.Backoff{
			Initial:    time.Second,
			Max:        time.Minute,
			Factor:     2.0,
			Jitter:     0.3,
			MaxRetries: 0, // retry forever
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
}
