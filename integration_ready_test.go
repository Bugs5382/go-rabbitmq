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
	"errors"
	"fmt"
	"testing"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// readyBackoff keeps the retry loop quick in tests.
var readyBackoff = rabbitmq.Backoff{Initial: 50 * time.Millisecond, Max: 400 * time.Millisecond, Factor: 2, Jitter: 0}

// waitUntil polls cond until it is true or the timeout elapses.
func waitUntil(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// TestIntegrationConsumerNotReadyUntilDeclareSucceeds covers issue #13 against a
// live broker. The queue already exists with different arguments, so the
// consumer's declare fails with PRECONDITION_FAILED. The connection stays up, so
// Conn.Healthy is true, but the consumer reads as retrying and not ready. Once
// the conflicting queue is deleted the next declare succeeds and the consumer
// becomes ready.
func TestIntegrationConsumerNotReadyUntilDeclareSucceeds(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url, rabbitmq.WithBackoff(readyBackoff))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	queue := fmt.Sprintf("it.ready.%d", time.Now().UnixNano())
	expires := int32(60_000)
	if _, err := conn.DeclareQueue(ctx, rabbitmq.QueueConfig{
		Name: queue, Args: amqp.Table{"x-expires": expires},
	}); err != nil {
		t.Fatalf("declare the conflicting queue: %v", err)
	}

	cons := conn.NewConsumer(rabbitmq.ConsumerConfig{
		Queue: rabbitmq.QueueConfig{
			Name: queue,
			Args: amqp.Table{"x-expires": expires, "x-max-length": int32(10)},
		},
	}, func(context.Context, rabbitmq.Delivery) error { return nil })
	runCtx, stopRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- cons.Run(runCtx) }()

	waitUntil(t, 10*time.Second, "two failed declares", func() bool { return cons.Status().Attempt >= 2 })
	st := cons.Status()
	if cons.Ready() || st.State != rabbitmq.ConsumerRetrying {
		t.Fatalf("status with a failing declare = %+v, want retrying and not ready", st)
	}
	var amqpErr *amqp.Error
	if !errors.As(st.Err, &amqpErr) || amqpErr.Code != amqp.PreconditionFailed {
		t.Errorf("last error = %v, want PRECONDITION_FAILED", st.Err)
	}
	if !conn.Healthy() {
		t.Error("the connection should stay healthy through a channel-level declare failure")
	}
	t.Logf("not ready while the declare fails: %+v", st)

	// Remove the conflict out of band; the next attempt must succeed.
	deleteQueue(t, url, queue)
	waitUntil(t, 10*time.Second, "the consumer to become ready", cons.Ready)
	if st := cons.Status(); st.Err != nil || st.Attempt != 0 || st.Queue != queue {
		t.Errorf("status once ready = %+v", st)
	}

	stopRun()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	if cons.Ready() || cons.Status().State != rabbitmq.ConsumerStopped {
		t.Errorf("status after Run returned = %+v, want stopped", cons.Status())
	}
}

// TestIntegrationConsumerNotReadyOnRefusedTransientQueue uses the issue #12
// shape. RabbitMQ 4 refuses it with a connection-level error, so each attempt
// also drops the connection. The consumer must still read as retrying, with the
// explained ErrInvalidQueue, across those reconnects.
func TestIntegrationConsumerNotReadyOnRefusedTransientQueue(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url, rabbitmq.WithBackoff(readyBackoff))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	statuses := make(chan rabbitmq.ConsumerStatus, 64)
	cons := conn.NewConsumer(rabbitmq.ConsumerConfig{
		Queue: rabbitmq.QueueConfig{
			Name:       fmt.Sprintf("it.ready.transient.%d", time.Now().UnixNano()),
			AutoDelete: true,
			Args:       amqp.Table{"x-expires": int32(60_000)},
		}.Transient(),
		OnStatus: func(st rabbitmq.ConsumerStatus) {
			select {
			case statuses <- st:
			default:
			}
		},
	}, func(context.Context, rabbitmq.Delivery) error { return nil })
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	done := make(chan error, 1)
	go func() { done <- cons.Run(runCtx) }()

	deadline := time.After(10 * time.Second)
	for {
		select {
		case st := <-statuses:
			if st.State == rabbitmq.ConsumerConsuming {
				stopRun()
				<-done
				t.Skip("broker accepts transient non-exclusive queues (RabbitMQ 3, or transient_nonexcl_queues permitted)")
			}
			if st.State != rabbitmq.ConsumerRetrying || st.Attempt < 2 {
				continue
			}
			if cons.Ready() {
				t.Fatalf("ready while the declare is refused: %+v", cons.Status())
			}
			if !errors.Is(st.Err, rabbitmq.ErrInvalidQueue) {
				t.Errorf("last error = %v, want the explained ErrInvalidQueue", st.Err)
			}
			t.Logf("not ready across reconnects (%d so far): %v", conn.Reconnects(), st.Err)
			stopRun()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Errorf("Run returned %v, want context.Canceled", err)
			}
			return
		case <-deadline:
			t.Fatalf("no second retry within 10s; last status %+v", cons.Status())
		}
	}
}

// deleteQueue removes a queue over a separate raw connection.
func deleteQueue(t *testing.T, url, name string) {
	t.Helper()
	raw, err := amqp.Dial(url)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer func() { _ = raw.Close() }()
	ch, err := raw.Channel()
	if err != nil {
		t.Fatalf("raw channel: %v", err)
	}
	defer func() { _ = ch.Close() }()
	if _, err := ch.QueueDelete(name, false, false, false); err != nil {
		t.Fatalf("delete queue %q: %v", name, err)
	}
}
