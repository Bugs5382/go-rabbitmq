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
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// dlqTopology is the dead-letter shape KeepTopology exists for: an exchange, a
// queue nobody consumes, and the binding between them.
func dlqTopology() Topology {
	return Topology{
		Exchanges: []ExchangeConfig{{Name: "orders.dlx", Kind: "fanout"}},
		Queues:    []QueueConfig{{Name: "orders.dlq", Type: QueueQuorum}},
		Bindings:  []BindingConfig{{Queue: "orders.dlq", Exchange: "orders.dlx"}},
	}
}

// declaredOn reports whether ch saw the dlqTopology exchange, queue and binding.
func declaredOn(ch *fakeChannel) bool {
	if ch == nil {
		return false
	}
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return len(ch.exchanges) == 1 && ch.exchanges[0].Name == "orders.dlx" &&
		len(ch.queues) == 1 && ch.queues[0].Name == "orders.dlq" &&
		len(ch.binds) == 1 && ch.binds[0].Exchange == "orders.dlx"
}

// dialWithTemplates wraps the fake broker's dial and installs tmpl[i] as the
// channel template of the i-th connection, when present.
func dialWithTemplates(b *fakeBroker, tmpl map[int]func(idx int, ch *fakeChannel)) dialFunc {
	var n atomic.Int32
	return func(url string, cfg amqp.Config) (wireConn, error) {
		wc, err := b.dial(url, cfg)
		if err != nil {
			return nil, err
		}
		if fn, ok := tmpl[int(n.Add(1))-1]; ok {
			wc.(*fakeConn).setChanTemplate(fn)
		}
		return wc, nil
	}
}

func TestKeepTopologyDeclaresNow(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.KeepTopology(context.Background(), dlqTopology()); err != nil {
		t.Fatalf("KeepTopology: %v", err)
	}
	if !declaredOn(b.lastConn().channelAt(0)) {
		t.Error("topology was not declared on the live connection")
	}
}

func TestKeepTopologyRejectsInvalidAndDoesNotKeepIt(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	bad := dlqTopology()
	bad.Queues = append(bad.Queues, QueueConfig{Name: "bad", Type: QueueQuorum, Exclusive: true})
	if err := conn.KeepTopology(context.Background(), bad); !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("KeepTopology err = %v, want ErrInvalidQueue", err)
	}
	if n := b.lastConn().channelCount(); n != 0 {
		t.Errorf("channels opened = %d, want 0: an invalid topology must not reach the broker", n)
	}

	b.lastConn().dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})
	waitFor(t, time.Second, func() bool { return conn.Reconnects() == 1 && conn.Healthy() })
	if n := b.lastConn().channelCount(); n != 0 {
		t.Errorf("channels opened after reconnect = %d, want 0: a rejected topology must not be kept", n)
	}
}

func TestKeepTopologyRedeclaresAfterReconnectBeforeReady(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	var live atomic.Pointer[Conn]
	var handedOutDuringRedeclare atomic.Bool
	dial := dialWithTemplates(b, map[int]func(int, *fakeChannel){
		// current, not Healthy: Healthy would call back into the fake connection,
		// whose lock is held while a channel template runs.
		1: func(_ int, _ *fakeChannel) {
			cur, _ := live.Load().current()
			handedOutDuringRedeclare.Store(cur != nil)
		},
	})
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()
	live.Store(conn)

	if err := conn.KeepTopology(context.Background(), dlqTopology()); err != nil {
		t.Fatalf("KeepTopology: %v", err)
	}
	b.lastConn().dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})
	waitFor(t, time.Second, func() bool { return conn.Reconnects() == 1 && conn.Healthy() })

	if !declaredOn(b.lastConn().channelAt(0)) {
		t.Fatal("kept topology was not declared again after the reconnect")
	}
	if handedOutDuringRedeclare.Load() {
		t.Error("the new connection was handed out before the kept topology was declared again")
	}
}

func TestKeepTopologyFailedRedeclareDoesNotStopReconnect(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	dial := dialWithTemplates(b, map[int]func(int, *fakeChannel){
		1: func(_ int, ch *fakeChannel) { ch.declareErr = &amqp.Error{Code: 406, Reason: "PRECONDITION_FAILED"} },
	})
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.KeepTopology(context.Background(), dlqTopology()); err != nil {
		t.Fatalf("KeepTopology: %v", err)
	}

	b.lastConn().dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})
	waitFor(t, time.Second, func() bool { return conn.Reconnects() == 1 && conn.Healthy() })

	b.lastConn().dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})
	waitFor(t, time.Second, func() bool { return conn.Reconnects() == 2 && conn.Healthy() })
	if !declaredOn(b.lastConn().channelAt(0)) {
		t.Error("kept topology was not declared on the reconnect after the failed one")
	}
}

func TestKeepTopologyKeepsACopy(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	topo := dlqTopology()
	if err := conn.KeepTopology(context.Background(), topo); err != nil {
		t.Fatalf("KeepTopology: %v", err)
	}
	topo.Queues[0].Name = "changed.after.keep"

	b.lastConn().dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})
	waitFor(t, time.Second, func() bool { return conn.Reconnects() == 1 && conn.Healthy() })
	if !declaredOn(b.lastConn().channelAt(0)) {
		t.Error("changing the caller's Topology after KeepTopology changed what is re-declared")
	}
}
