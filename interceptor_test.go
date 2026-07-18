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
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublishInterceptorRunsAndCanMutateHeaders(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	var seenExchange, seenKey string
	ic := func(ctx context.Context, exchange, key string, msg *amqp.Publishing, next PublishFunc) error {
		seenExchange, seenKey = exchange, key
		if msg.Headers == nil {
			msg.Headers = amqp.Table{}
		}
		msg.Headers["traceparent"] = "00-abc-def-01"
		return next(ctx, exchange, key, msg)
	}
	conn := newTestConn(t, b, WithPublishInterceptor(ic))
	pub := conn.NewPublisher("events")
	if err := pub.Publish(context.Background(), "orders.created", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if seenExchange != "events" || seenKey != "orders.created" {
		t.Errorf("interceptor saw %q/%q", seenExchange, seenKey)
	}
	msg := b.lastConn().channelAt(0).published[0]
	if msg.Headers["traceparent"] != "00-abc-def-01" {
		t.Errorf("interceptor header not carried to the wire: %+v", msg.Headers)
	}
}

func TestPublishInterceptorsRunOutermostFirst(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	var order []string
	mk := func(tag string) PublishInterceptor {
		return func(ctx context.Context, ex, key string, msg *amqp.Publishing, next PublishFunc) error {
			order = append(order, tag+"-before")
			err := next(ctx, ex, key, msg)
			order = append(order, tag+"-after")
			return err
		}
	}
	conn := newTestConn(t, b, WithPublishInterceptor(mk("a"), mk("b")))
	pub := conn.NewPublisher("events")
	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	want := []string{"a-before", "b-before", "b-after", "a-after"}
	if len(order) != 4 {
		t.Fatalf("order = %v", order)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestConsumeInterceptorWrapsHandler(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	var wrapped bool
	ic := func(ctx context.Context, d Delivery, next Handler) error {
		wrapped = true
		return next(ctx, d)
	}
	conn := newTestConn(t, b, WithConsumeInterceptor(ic))

	got := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = conn.Consume(ctx, basicConsumerCfg(), func(context.Context, Delivery) error {
			got <- struct{}{}
			return nil
		})
	}()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch.consumeStarted() })
	ch.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("m")})
	<-got
	waitFor(t, time.Second, func() bool { return len(ch.ackedTags()) == 1 })
	if !wrapped {
		t.Error("consume interceptor did not wrap the handler")
	}
}
