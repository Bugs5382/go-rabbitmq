//go:build integration

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
	"fmt"
	"sync"
	"testing"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TestIntegrationSettlement drives each handler settlement against a live broker:
// a message that is requeued once and then acked, and a poison message that is
// dead-lettered through the queue's dead-letter exchange.
func TestIntegrationSettlement(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	events, dlx := "it.settle.events."+suffix, "it.settle.dlx."+suffix
	queue, dlq := "it.settle.work."+suffix, "it.settle.dead."+suffix

	// Dead-letter side: a fanout exchange and a queue that collects rejects. The
	// queues are durable auto-delete rather than transient: RabbitMQ 4 refuses
	// transient non-exclusive queues by default (transient_nonexcl_queues).
	if err := conn.DeclareTopology(ctx, rabbitmq.Topology{
		Exchanges: []rabbitmq.ExchangeConfig{
			rabbitmq.ExchangeConfig{Name: events, Kind: "topic", AutoDelete: true}.Transient(),
			rabbitmq.ExchangeConfig{Name: dlx, Kind: "fanout", AutoDelete: true}.Transient(),
		},
		Queues: []rabbitmq.QueueConfig{
			rabbitmq.QueueConfig{Name: dlq, AutoDelete: true},
		},
		Bindings: []rabbitmq.BindingConfig{{Queue: dlq, Exchange: dlx}},
	}); err != nil {
		t.Fatalf("declare dead-letter topology: %v", err)
	}

	var (
		mu       sync.Mutex
		attempts = map[string]int{}
	)
	acked := make(chan string, 4)
	cfg := rabbitmq.ConsumerConfig{
		Exchange: rabbitmq.ExchangeConfig{Name: events, Kind: "topic", AutoDelete: true}.Transient(),
		Queue: rabbitmq.QueueConfig{
			Name:       queue,
			AutoDelete: true,
			Args:       amqp.Table{"x-dead-letter-exchange": dlx},
		},
		Bindings: []rabbitmq.BindingConfig{{Exchange: events, RoutingKey: "#"}},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = conn.Consume(ctx, cfg, func(_ context.Context, d rabbitmq.Delivery) error {
			body := string(d.Body)
			mu.Lock()
			attempts[body]++
			n := attempts[body]
			mu.Unlock()
			switch {
			case body == "poison":
				return fmt.Errorf("cannot decode: %w", rabbitmq.ErrDeadLetter)
			case body == "flaky" && n == 1:
				return fmt.Errorf("store busy: %w", rabbitmq.ErrRequeue)
			}
			acked <- body
			return nil
		})
	}()
	time.Sleep(500 * time.Millisecond)

	pub := conn.NewPublisher(events, rabbitmq.WithConfirms())
	for _, body := range []string{"flaky", "poison"} {
		if err := pub.Publish(ctx, "work."+body, []byte(body)); err != nil {
			t.Fatalf("publish %s: %v", body, err)
		}
	}

	select {
	case body := <-acked:
		if body != "flaky" {
			t.Errorf("acked %q, want flaky", body)
		}
	case <-ctx.Done():
		t.Fatal("requeued message was never acked")
	}

	dead := make(chan rabbitmq.Delivery, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = conn.Consume(ctx, rabbitmq.ConsumerConfig{
			Queue: rabbitmq.QueueConfig{Name: dlq, AutoDelete: true},
		}, func(_ context.Context, d rabbitmq.Delivery) error {
			select {
			case dead <- d:
			default:
			}
			return nil
		})
	}()
	select {
	case d := <-dead:
		if string(d.Body) != "poison" {
			t.Errorf("dead-lettered %q, want poison", d.Body)
		}
		if _, ok := d.Headers["x-death"]; !ok {
			t.Error("dead-lettered message has no x-death header")
		}
	case <-ctx.Done():
		t.Fatal("poison message never reached the dead-letter queue")
	}

	mu.Lock()
	if attempts["flaky"] != 2 || attempts["poison"] != 1 {
		t.Errorf("attempts = %v, want flaky=2 (requeued once) and poison=1", attempts)
	}
	mu.Unlock()

	cancel()
	wg.Wait()
}
