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
	"sync"
	"testing"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
)

// Integration tests for stream queues (issue #19).

// streamMsg is one delivery read back from a stream.
type streamMsg struct {
	body   string
	offset int64
}

// streamReader runs a consumer on a stream and hands its deliveries over.
type streamReader struct {
	cons   *rabbitmq.Consumer
	got    chan streamMsg
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func readStream(t *testing.T, conn *rabbitmq.Conn, queue rabbitmq.QueueConfig, off rabbitmq.StreamOffset) *streamReader {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &streamReader{got: make(chan streamMsg, 256), cancel: cancel, done: make(chan error, 1)}
	r.cons = conn.NewConsumer(rabbitmq.ConsumerConfig{Queue: queue, StreamOffset: off, Prefetch: 100},
		func(_ context.Context, d rabbitmq.Delivery) error {
			off, _ := d.Headers["x-stream-offset"].(int64)
			r.got <- streamMsg{body: string(d.Body), offset: off}
			return nil
		})
	go func() { r.done <- r.cons.Run(ctx) }()
	waitUntil(t, 15*time.Second, "stream consumer ready", r.cons.Ready)
	t.Cleanup(r.stop)
	return r
}

func (r *streamReader) stop() {
	r.once.Do(func() {
		r.cancel()
		<-r.done
	})
}

// next returns the next delivery or fails the test.
func (r *streamReader) next(t *testing.T) streamMsg {
	t.Helper()
	select {
	case m := <-r.got:
		return m
	case <-time.After(10 * time.Second):
		t.Fatal("no delivery from the stream within 10s")
		return streamMsg{}
	}
}

// until reads deliveries until one has body want, returning everything read.
func (r *streamReader) until(t *testing.T, want string) []streamMsg {
	t.Helper()
	var seen []streamMsg
	for {
		m := r.next(t)
		seen = append(seen, m)
		if m.body == want {
			return seen
		}
	}
}

// TestIntegrationStreamOffsets declares a stream with retention options,
// publishes to it, and reads it back from every kind of offset.
func TestIntegrationStreamOffsets(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	queue := rabbitmq.QueueConfig{
		Name: fmt.Sprintf("it.stream.%d", time.Now().UnixNano()),
		Type: rabbitmq.QueueStream,
		Stream: rabbitmq.StreamOptions{
			MaxAge:              time.Hour,
			MaxLengthBytes:      50_000_000,
			MaxSegmentSizeBytes: 5_000_000,
		},
	}
	if _, err := conn.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("declare stream: %v", err)
	}
	// Declaring again with the same arguments is a no-op.
	if _, err := conn.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("re-declare stream: %v", err)
	}

	// Confirm each publish before the next, so every message is its own chunk
	// and "last" is distinguishable from "first".
	pub := conn.NewPublisher("", rabbitmq.WithMandatory(), rabbitmq.WithConfirmTimeout(5*time.Second))
	defer func() { _ = pub.Close() }()
	publish := func(body string) {
		t.Helper()
		if err := pub.Publish(ctx, queue.Name, []byte(body)); err != nil {
			t.Fatalf("publish %s: %v", body, err)
		}
	}
	for i := 0; i < 10; i++ {
		publish(fmt.Sprintf("m%d", i))
	}

	t.Run("first", func(t *testing.T) {
		r := readStream(t, conn, queue, rabbitmq.StreamOffsetFirst())
		if m := r.next(t); m.body != "m0" || m.offset != 0 {
			t.Fatalf("first delivery = %+v, want m0 at offset 0", m)
		}
		if seen := r.until(t, "m9"); len(seen) != 9 {
			t.Errorf("read %d more messages, want 9", len(seen))
		}
	})

	t.Run("offset", func(t *testing.T) {
		r := readStream(t, conn, queue, rabbitmq.StreamOffsetAt(4))
		if m := r.next(t); m.body != "m4" || m.offset != 4 {
			t.Fatalf("first delivery = %+v, want m4 at offset 4", m)
		}
	})

	t.Run("last", func(t *testing.T) {
		r := readStream(t, conn, queue, rabbitmq.StreamOffsetLast())
		seen := r.until(t, "m9")
		if seen[0].body == "m0" {
			t.Errorf("last started at the beginning of the stream: %+v", seen)
		}
	})

	t.Run("timestamp", func(t *testing.T) {
		// The AMQP timestamp has one-second precision: leave a clear gap on
		// both sides of the cut-off.
		time.Sleep(2 * time.Second)
		cut := time.Now()
		time.Sleep(1100 * time.Millisecond)
		publish("late0")
		publish("late1")

		r := readStream(t, conn, queue, rabbitmq.StreamOffsetTime(cut))
		if m := r.next(t); m.body != "late0" {
			t.Fatalf("first delivery after %s = %+v, want late0", cut.Format(time.RFC3339), m)
		}
	})

	t.Run("next", func(t *testing.T) {
		r := readStream(t, conn, queue, rabbitmq.StreamOffsetNext())
		publish("fresh")
		if m := r.next(t); m.body != "fresh" {
			t.Fatalf("first delivery = %+v, want fresh", m)
		}
	})
}

// TestIntegrationStreamResumesAfterReconnect reads a stream from the start,
// forces a reconnect, and checks the consumer carries on after the last offset
// it handled instead of replaying the stream.
func TestIntegrationStreamResumesAfterReconnect(t *testing.T) {
	url := brokerURL(t)
	mgmt := brokerMgmt(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	name, props := connName("it-stream-resume")
	conn, err := rabbitmq.Connect(ctx, url, rabbitmq.WithClientProperties(props),
		rabbitmq.WithBackoff(rabbitmq.Backoff{Initial: 100 * time.Millisecond, Max: time.Second, Factor: 2}))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	queue := rabbitmq.QueueConfig{Name: fmt.Sprintf("it.stream.resume.%d", time.Now().UnixNano()), Type: rabbitmq.QueueStream}
	if _, err := conn.DeclareQueue(ctx, queue); err != nil {
		t.Fatalf("declare stream: %v", err)
	}
	pub := conn.NewPublisher("", rabbitmq.WithConfirms(), rabbitmq.WithPublishRetries(10))
	defer func() { _ = pub.Close() }()
	for i := 0; i < 5; i++ {
		if err := pub.Publish(ctx, queue.Name, []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	r := readStream(t, conn, queue, rabbitmq.StreamOffsetFirst())
	r.until(t, "m4")

	mgmt.closeConnection(t, name)
	waitUntil(t, 30*time.Second, "consumer back after the reconnect", func() bool {
		return conn.Reconnects() > 0 && r.cons.Ready()
	})
	if err := pub.Publish(ctx, queue.Name, []byte("after")); err != nil {
		t.Fatalf("publish after the reconnect: %v", err)
	}
	if m := r.next(t); m.body != "after" || m.offset != 5 {
		t.Fatalf("first delivery after the reconnect = %+v, want after at offset 5 (no replay)", m)
	}
}

// TestIntegrationStreamRejectsAutoAck checks that an auto-ack stream consumer
// fails at once with ErrInvalidConsumer instead of retrying against the broker.
func TestIntegrationStreamRejectsAutoAck(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := rabbitmq.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	err = conn.Consume(ctx, rabbitmq.ConsumerConfig{
		Queue:   rabbitmq.QueueConfig{Name: "it.stream.autoack", Type: rabbitmq.QueueStream},
		AutoAck: true,
	}, func(context.Context, rabbitmq.Delivery) error { return nil })
	if !errors.Is(err, rabbitmq.ErrInvalidConsumer) {
		t.Fatalf("want ErrInvalidConsumer, got %v", err)
	}
}
