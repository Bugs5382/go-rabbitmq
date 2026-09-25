//go:build integration

// Package rabbitmq integration tests exercise the library against a real broker.
// They are excluded from the default build and skip when RABBITMQ_TEST_URL is
// unset. CI runs them against a RabbitMQ service container (see
// .github/workflows/job-go-integration.yaml). Run them locally with:
//
//	docker run -d --rm -p 5672:5672 rabbitmq:4-management
//	RABBITMQ_TEST_URL=amqp://guest:guest@localhost:5672/ go test -tags integration -run Integration ./...
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
	"os"
	"sync"
	"testing"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
)

// brokerURL returns the broker URL from RABBITMQ_TEST_URL and skips the test when
// it is unset, so a local run without a broker still passes. CI sets
// RABBITMQ_TEST_REQUIRED as well, which turns a missing URL into a failure: a
// broken service wiring must not pass as a run of skipped tests.
func brokerURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("RABBITMQ_TEST_URL")
	if url == "" {
		if os.Getenv("RABBITMQ_TEST_REQUIRED") != "" {
			t.Fatal("RABBITMQ_TEST_REQUIRED is set but RABBITMQ_TEST_URL is empty")
		}
		t.Skip("set RABBITMQ_TEST_URL to run integration tests")
	}
	return url
}

// TestIntegrationRoundTrip declares a topology, publishes, and consumes the
// message back against a live broker.
func TestIntegrationRoundTrip(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	cfg := rabbitmq.ConsumerConfig{
		Exchange: rabbitmq.ExchangeConfig{Name: "it.events", Kind: "topic"},
		Queue:    rabbitmq.QueueConfig{Name: "it.orders", Type: rabbitmq.QueueQuorum},
		Bindings: []rabbitmq.BindingConfig{{Exchange: "it.events", RoutingKey: "orders.*"}},
		Prefetch: 5,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	got := make(chan string, 1)
	go func() {
		defer wg.Done()
		_ = conn.Consume(ctx, cfg, func(_ context.Context, d rabbitmq.Delivery) error {
			select {
			case got <- string(d.Body):
			default:
			}
			return nil
		})
	}()

	// Give the consumer a moment to declare and start.
	time.Sleep(500 * time.Millisecond)

	pub := conn.NewPublisher("it.events",
		rabbitmq.WithExchangeDeclare(rabbitmq.ExchangeConfig{Name: "it.events", Kind: "topic"}),
	)
	if err := pub.Publish(ctx, "orders.created", []byte("hello")); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case body := <-got:
		if body != "hello" {
			t.Errorf("received %q, want hello", body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("did not receive the published message in time")
	}

	cancel()
	wg.Wait()
}

// TestIntegrationServerNamedClassicQueue declares the ephemeral fan-out shape
// (server-named, exclusive, auto-delete). On a broker whose default_queue_type
// is quorum this failed with PRECONDITION_FAILED before issue #3; it must pass
// on any broker.
func TestIntegrationServerNamedClassicQueue(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	q, err := conn.DeclareQueue(ctx, rabbitmq.QueueConfig{
		Type:       rabbitmq.QueueClassic,
		AutoDelete: true,
		Exclusive:  true,
	}.Transient())
	if err != nil {
		t.Fatalf("declare server-named classic queue: %v", err)
	}
	if q.Name == "" {
		t.Error("expected a server-assigned queue name")
	}
}
