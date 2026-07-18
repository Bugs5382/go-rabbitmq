package rabbitmq

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
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func newTestConn(t *testing.T, b *fakeBroker, opts ...Option) *Conn {
	t.Helper()
	opts = append([]Option{withDialer(b.dial), WithBackoff(fastBackoff())}, opts...)
	conn, err := Connect(context.Background(), "amqp://localhost", opts...)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestPublishSuccessSetsJSONAndPersistentDefaults(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events")

	if err := pub.Publish(context.Background(), "orders.created", []byte(`{"id":1}`)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ch := b.lastConn().channelAt(0)
	if ch.publishedCount() != 1 {
		t.Fatalf("published %d messages, want 1", ch.publishedCount())
	}
	msg := ch.published[0]
	if msg.ContentType != "application/json" {
		t.Errorf("content type = %q, want application/json", msg.ContentType)
	}
	if msg.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery mode = %d, want persistent (2)", msg.DeliveryMode)
	}
	if ch.pubKeys[0] != "orders.created" {
		t.Errorf("routing key = %q", ch.pubKeys[0])
	}
}

func TestPublishRetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	// The first channel opened fails its publish; the publisher resets and opens a
	// second channel that succeeds.
	b.lastConn().setChanTemplate(func(idx int, ch *fakeChannel) {
		if idx == 0 {
			ch.publishErr = errors.New("channel/connection closed")
		}
	})

	pub := conn.NewPublisher("events")
	if err := pub.Publish(context.Background(), "k", []byte("body")); err != nil {
		t.Fatalf("Publish should have recovered after retry: %v", err)
	}
	if b.lastConn().channelCount() < 2 {
		t.Errorf("expected a fresh channel to be opened on retry, opened %d", b.lastConn().channelCount())
	}
}

func TestPublishReturnsErrPublishFailedAfterRetries(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) { ch.publishErr = errors.New("permanently broken") })

	pub := conn.NewPublisher("events", WithPublishRetries(2))
	err := pub.Publish(context.Background(), "k", []byte("body"))
	if !errors.Is(err, ErrPublishFailed) {
		t.Fatalf("expected ErrPublishFailed, got %v", err)
	}
}

func TestPublishHonoursContextCancellationBetweenRetries(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b, WithBackoff(Backoff{Initial: 50 * time.Millisecond, Max: 50 * time.Millisecond, Jitter: 0}))
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) { ch.publishErr = errors.New("broken") })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	pub := conn.NewPublisher("events", WithPublishRetries(10))
	err := pub.Publish(ctx, "k", []byte("body"))
	if !errors.Is(err, ErrPublishFailed) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected ErrPublishFailed wrapping context deadline, got %v", err)
	}
}

func TestPublisherDeclaresExchangeWhenConfigured(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events", WithExchangeDeclare(ExchangeConfig{Name: "events", Kind: "topic"}))
	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ch := b.lastConn().channelAt(0)
	if len(ch.exchanges) != 1 || ch.exchanges[0].Name != "events" {
		t.Errorf("expected exchange declared, got %+v", ch.exchanges)
	}
}

func TestPublishOverridesApply(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events")
	err := pub.Publish(context.Background(), "k", []byte("x"),
		WithContentType("text/plain"),
		WithPersistent(false),
		WithMessageID("m1"),
		WithHeaders(amqp.Table{"x-trace": "abc"}),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := b.lastConn().channelAt(0).published[0]
	if msg.ContentType != "text/plain" || msg.DeliveryMode != amqp.Transient || msg.MessageId != "m1" || msg.Headers["x-trace"] != "abc" {
		t.Errorf("publish options not applied: %+v", msg)
	}
}

func TestPublishJSONMarshals(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events")
	type payload struct {
		ID int `json:"id"`
	}
	if err := pub.PublishJSON(context.Background(), "k", payload{ID: 7}); err != nil {
		t.Fatalf("PublishJSON: %v", err)
	}
	msg := b.lastConn().channelAt(0).published[0]
	if string(msg.Body) != `{"id":7}` {
		t.Errorf("body = %q, want JSON", msg.Body)
	}
}

func TestPublishFailsWhenClosed(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events")
	_ = conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := pub.Publish(ctx, "k", []byte("x")); err == nil {
		t.Error("expected publish to fail on a closed connection")
	}
}
