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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestConnDeclareHelpers(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	ctx := context.Background()

	if err := conn.DeclareExchange(ctx, ExchangeConfig{Name: "events"}); err != nil {
		t.Fatalf("DeclareExchange: %v", err)
	}
	q, err := conn.DeclareQueue(ctx, QueueConfig{Name: ""}) // server-named classic
	if err != nil {
		t.Fatalf("DeclareQueue: %v", err)
	}
	if q.Name == "" {
		t.Error("server-named queue should return a resolved name")
	}
	if err := conn.BindQueue(ctx, BindingConfig{Queue: q.Name, Exchange: "events", RoutingKey: "#"}); err != nil {
		t.Fatalf("BindQueue: %v", err)
	}
	if err := conn.DeclareTopology(ctx, Topology{
		Exchanges: []ExchangeConfig{{Name: "events"}},
		Queues:    []QueueConfig{{Name: "orders"}},
		Bindings:  []BindingConfig{{Queue: "orders", Exchange: "events", RoutingKey: "orders.*"}},
	}); err != nil {
		t.Fatalf("DeclareTopology: %v", err)
	}
}

func TestDeclareExchangeDefaultExchangeIsNoOp(t *testing.T) {
	t.Parallel()
	ch := newFakeChannel()
	if err := declareExchangeOn(ch, ExchangeConfig{Name: ""}); err != nil {
		t.Fatalf("declaring the default exchange should be a no-op, got %v", err)
	}
	if len(ch.declaredExchanges()) != 0 {
		t.Error("the default exchange must not be declared")
	}
}

// recordingObserver counts observer callbacks.
type recordingObserver struct {
	mu         sync.Mutex
	connects   int
	disconnect int
	reconnect  int
	publishOK  int
	publishErr int
	consumed   int
}

func (o *recordingObserver) OnConnect() { o.mu.Lock(); o.connects++; o.mu.Unlock() }
func (o *recordingObserver) OnDisconnect(error) {
	o.mu.Lock()
	o.disconnect++
	o.mu.Unlock()
}
func (o *recordingObserver) OnReconnect(int) { o.mu.Lock(); o.reconnect++; o.mu.Unlock() }
func (o *recordingObserver) OnPublish(_, _ string, err error) {
	o.mu.Lock()
	if err == nil {
		o.publishOK++
	} else {
		o.publishErr++
	}
	o.mu.Unlock()
}
func (o *recordingObserver) OnConsume(string, Delivery, error) {
	o.mu.Lock()
	o.consumed++
	o.mu.Unlock()
}
func (o *recordingObserver) snapshot() recordingObserver {
	o.mu.Lock()
	defer o.mu.Unlock()
	return recordingObserver{connects: o.connects, disconnect: o.disconnect, reconnect: o.reconnect, publishOK: o.publishOK, publishErr: o.publishErr, consumed: o.consumed}
}

func TestObserverReceivesLifecycleAndMessageEvents(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	obs := &recordingObserver{}
	conn := newTestConn(t, b, WithObserver(obs))

	// publish success
	pub := conn.NewPublisher("events")
	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	// drive a reconnect
	b.lastConn().dropWith(&amqp.Error{Code: 320, Reason: "forced"})
	waitFor(t, time.Second, func() bool { return conn.Reconnects() == 1 })

	s := obs.snapshot()
	if s.connects < 2 {
		t.Errorf("expected >=2 OnConnect (initial + reconnect), got %d", s.connects)
	}
	if s.disconnect < 1 {
		t.Errorf("expected >=1 OnDisconnect, got %d", s.disconnect)
	}
	if s.publishOK < 1 {
		t.Errorf("expected >=1 successful OnPublish, got %d", s.publishOK)
	}
}

func TestNopObserverAndLoggerAreSafe(t *testing.T) {
	t.Parallel()
	var o NopObserver
	o.OnConnect()
	o.OnDisconnect(nil)
	o.OnReconnect(1)
	o.OnPublish("e", "k", nil)
	o.OnConsume("q", Delivery{}, nil)

	var l nopLogger
	l.Debugf("a")
	l.Infof("b")
	l.Warnf("c")
	l.Errorf("d")
}

// countingLogger proves a plugged-in logger actually receives calls.
type countingLogger struct{ n atomic.Int64 }

func (c *countingLogger) Debugf(string, ...any) { c.n.Add(1) }
func (c *countingLogger) Infof(string, ...any)  { c.n.Add(1) }
func (c *countingLogger) Warnf(string, ...any)  { c.n.Add(1) }
func (c *countingLogger) Errorf(string, ...any) { c.n.Add(1) }

func TestPluggedLoggerReceivesCalls(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	log := &countingLogger{}
	conn := newTestConn(t, b, WithLogger(log))
	if log.n.Load() == 0 {
		t.Error("expected the logger to record the connect message")
	}
	_ = conn
}

func TestAllPublishOptionsApply(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events",
		WithDefaultContentType("application/octet-stream"),
		WithPersistentDefault(false),
		WithMandatoryDefault(true),
	)
	err := pub.Publish(context.Background(), "k", []byte("x"),
		WithCorrelationID("c1"),
		WithReplyTo("reply-q"),
		WithExpiration("60000"),
		WithPriority(5),
		WithType("order.created"),
		WithAppID("svc"),
		WithPersistent(true),
	)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	msg := b.lastConn().channelAt(0).published[0]
	switch {
	case msg.CorrelationId != "c1":
		t.Error("correlation id not applied")
	case msg.ReplyTo != "reply-q":
		t.Error("reply-to not applied")
	case msg.Expiration != "60000":
		t.Error("expiration not applied")
	case msg.Priority != 5:
		t.Error("priority not applied")
	case msg.Type != "order.created":
		t.Error("type not applied")
	case msg.AppId != "svc":
		t.Error("app id not applied")
	case msg.DeliveryMode != amqp.Persistent:
		t.Error("per-message WithPersistent(true) should override the default")
	}

	if err := pub.Close(); err != nil {
		t.Errorf("publisher Close: %v", err)
	}
}
