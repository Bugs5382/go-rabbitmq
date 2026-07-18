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
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// collector is a concurrency-safe Handler recorder.
type collector struct {
	mu     sync.Mutex
	bodies []string
	failOn map[string]bool
	panicN string
}

func (c *collector) handle(_ context.Context, d Delivery) error {
	c.mu.Lock()
	c.bodies = append(c.bodies, string(d.Body))
	fail := c.failOn[string(d.Body)]
	shouldPanic := c.panicN == string(d.Body)
	c.mu.Unlock()
	if shouldPanic {
		panic("boom")
	}
	if fail {
		return errors.New("handler rejected")
	}
	return nil
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.bodies)
}

func basicConsumerCfg() ConsumerConfig {
	return ConsumerConfig{
		Exchange: ExchangeConfig{Name: "events"},
		Queue:    QueueConfig{Name: "orders"},
		Bindings: []BindingConfig{{Exchange: "events", RoutingKey: "orders.*"}},
		Prefetch: 5,
	}
}

func TestConsumeDeclaresTopologyAndAcksOnSuccess(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- conn.Consume(ctx, basicConsumerCfg(), col.handle) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch.consumeStarted() })

	ch.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("m1")})
	waitFor(t, time.Second, func() bool { return len(ch.ackedTags()) == 1 })

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Consume returned %v, want context.Canceled", err)
	}

	if len(ch.declaredExchanges()) != 1 || len(ch.declaredQueues()) != 1 || len(ch.declaredBinds()) != 1 {
		t.Errorf("topology not fully declared: ex=%d q=%d b=%d", len(ch.declaredExchanges()), len(ch.declaredQueues()), len(ch.declaredBinds()))
	}
	if q := ch.qosArgs(); q == nil || q.prefetchCount != 5 {
		t.Errorf("qos not applied: %+v", ch.qosArgs())
	}
	// binding with empty queue should resolve to the declared queue name.
	if ch.declaredBinds()[0].Name != "orders" {
		t.Errorf("binding queue = %q, want resolved 'orders'", ch.declaredBinds()[0].Name)
	}
}

func TestConsumeNacksWithRequeueOnHandlerError(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{failOn: map[string]bool{"bad": true}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = conn.Consume(ctx, basicConsumerCfg(), col.handle) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch.consumeStarted() })

	ch.deliver(amqp.Delivery{DeliveryTag: 9, Body: []byte("bad")})
	waitFor(t, time.Second, func() bool { return len(ch.nackRecords()) == 1 })

	nr := ch.nackRecords()[0]
	if nr.tag != 9 || !nr.requeue {
		t.Errorf("nack = %+v, want tag 9 with requeue=true (default)", nr)
	}
}

func TestConsumeNoRequeueDropsRejected(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{failOn: map[string]bool{"poison": true}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = conn.Consume(ctx, basicConsumerCfg().NoRequeue(), col.handle) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch.consumeStarted() })

	ch.deliver(amqp.Delivery{DeliveryTag: 4, Body: []byte("poison")})
	waitFor(t, time.Second, func() bool { return len(ch.nackRecords()) == 1 })
	if ch.nackRecords()[0].requeue {
		t.Error("NoRequeue config must nack with requeue=false")
	}
}

func TestConsumeRecoversFromHandlerPanic(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{panicN: "explode"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = conn.Consume(ctx, basicConsumerCfg(), col.handle) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch.consumeStarted() })

	ch.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("explode")})
	// a panic must be recovered and treated as an error -> nack.
	waitFor(t, time.Second, func() bool { return len(ch.nackRecords()) == 1 })

	// the consumer must still be alive and process the next message.
	ch.deliver(amqp.Delivery{DeliveryTag: 2, Body: []byte("ok")})
	waitFor(t, time.Second, func() bool { return len(ch.ackedTags()) == 1 })
}

func TestConsumeResumesAfterChannelDrop(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = conn.Consume(ctx, basicConsumerCfg(), col.handle) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch1 := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch1.consumeStarted() })

	// Close the channel: the consumer must open a new one and re-declare.
	_ = ch1.Close()
	waitFor(t, 2*time.Second, func() bool { return b.lastConn().channelCount() >= 2 })
	ch2 := b.lastConn().channelAt(1)
	waitFor(t, 2*time.Second, func() bool { return ch2.consumeStarted() })

	ch2.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("after-recover")})
	waitFor(t, time.Second, func() bool { return len(ch2.ackedTags()) == 1 })

	if len(ch2.declaredQueues()) != 1 || len(ch2.declaredBinds()) != 1 {
		t.Error("topology should be re-declared on the recovered channel")
	}
}

func TestConsumeResumesAfterConnectionDrop(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = conn.Consume(ctx, basicConsumerCfg(), col.handle) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	firstConn := b.lastConn()
	waitFor(t, time.Second, func() bool { return firstConn.channelAt(0).consumeStarted() })

	// Drop the whole connection; the manager reconnects and the consumer resumes.
	firstConn.dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})
	waitFor(t, 2*time.Second, func() bool { return conn.Reconnects() == 1 })

	waitFor(t, 2*time.Second, func() bool {
		c := b.lastConn()
		return c != firstConn && c.channelCount() >= 1 && c.channelAt(0).consumeStarted()
	})
	newCh := b.lastConn().channelAt(0)
	newCh.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("resumed")})
	waitFor(t, time.Second, func() bool { return len(newCh.ackedTags()) == 1 })
}

func TestConsumeReturnsOnContextCancel(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- conn.Consume(ctx, basicConsumerCfg(), func(context.Context, Delivery) error { return nil })
	}()
	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Consume = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Consume did not return after context cancel")
	}
}

func TestAutoAckDoesNotManuallyAck(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{}
	cfg := basicConsumerCfg()
	cfg.AutoAck = true

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = conn.Consume(ctx, cfg, col.handle) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch.consumeStarted() })

	ch.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("m")})
	waitFor(t, time.Second, func() bool { return col.count() == 1 })
	// give the loop a moment; with auto-ack the library must not call Ack.
	time.Sleep(10 * time.Millisecond)
	if len(ch.ackedTags()) != 0 {
		t.Errorf("auto-ack must not manually ack, got %d acks", len(ch.ackedTags()))
	}
}
