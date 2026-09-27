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
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Tests for reporting unroutable mandatory publishes (issue #20).

func TestMandatoryRoutedPublishReturnsNil(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events", WithMandatory())

	callerHeaders := amqp.Table{"x-source": "svc"}
	if err := pub.Publish(context.Background(), "k", []byte("x"), WithHeaders(callerHeaders)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ch := b.lastConn().channelAt(0)
	if !ch.inConfirmMode() {
		t.Error("WithMandatory must put the channel in confirm mode")
	}
	if flags := ch.mandatoryFlags(); len(flags) != 1 || !flags[0] {
		t.Errorf("mandatory flags = %v, want [true]", flags)
	}
	if ch.returnListeners() != 1 {
		t.Errorf("return listeners = %d, want 1", ch.returnListeners())
	}
	msg := ch.publishedAt(0)
	if id, ok := msg.Headers[PublishIDHeader].(string); !ok || id == "" {
		t.Errorf("published headers = %v, want a %s", msg.Headers, PublishIDHeader)
	}
	if msg.Headers["x-source"] != "svc" {
		t.Error("caller headers were dropped")
	}
	if _, ok := callerHeaders[PublishIDHeader]; ok || len(callerHeaders) != 1 {
		t.Errorf("caller headers were mutated: %v", callerHeaders)
	}
}

func TestMandatoryReturnedPublishIsErrUnroutable(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmReturn}
	})
	pub := conn.NewPublisher("events", WithMandatory(), WithPublishRetries(3))

	err := pub.Publish(context.Background(), "nobody.listens", []byte("x"), WithMessageID("m-1"))
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("err = %v, want ErrUnroutable", err)
	}
	if !errors.Is(err, ErrPublishFailed) {
		t.Errorf("err = %v, want it to also match ErrPublishFailed", err)
	}
	var ue *UnroutableError
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v, want an *UnroutableError", err)
	}
	if ue.ReplyCode != amqp.NoRoute || ue.ReplyText != "NO_ROUTE" || ue.Exchange != "events" ||
		ue.RoutingKey != "nobody.listens" || ue.MessageID != "m-1" {
		t.Errorf("UnroutableError = %+v", ue)
	}
	if n := b.lastConn().channelCount(); n != 1 {
		t.Errorf("opened %d channels, want 1 (an unroutable message is not retried)", n)
	}
	if n := b.lastConn().channelAt(0).publishedCount(); n != 1 {
		t.Errorf("published %d times, want 1", n)
	}
}

// Many publishes in flight on one channel: each return must reach the publish
// it belongs to, and only that one.
func TestMandatoryReturnsCorrelateWithConcurrentPublishes(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.returnKeys = map[string]bool{}
		for i := 0; i < 100; i += 2 {
			ch.returnKeys[fmt.Sprintf("k%d", i)] = true
		}
	})
	pub := conn.NewPublisher("events", WithMandatory())

	var wg sync.WaitGroup
	errs := make([]error, 100)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = pub.Publish(context.Background(), fmt.Sprintf("k%d", i), []byte("x"))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		wantUnroutable := i%2 == 0
		if got := errors.Is(err, ErrUnroutable); got != wantUnroutable {
			t.Errorf("publish %d: err = %v, want unroutable=%t", i, err, wantUnroutable)
		}
		var ue *UnroutableError
		if wantUnroutable && errors.As(err, &ue) && ue.RoutingKey != fmt.Sprintf("k%d", i) {
			t.Errorf("publish %d got the return for %q", i, ue.RoutingKey)
		}
	}
}

func TestMandatoryNackIsErrNacked(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmNack}
	})
	pub := conn.NewPublisher("events", WithMandatory())
	err := pub.Publish(context.Background(), "k", []byte("x"))
	if !errors.Is(err, ErrNacked) || errors.Is(err, ErrUnroutable) {
		t.Fatalf("err = %v, want ErrNacked only", err)
	}
}

// WithMandatoryDefault(false) after WithMandatory must not switch the flag off:
// the broker would then drop unroutable messages silently.
func TestMandatoryIsNotUndoneByMandatoryDefault(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events", WithMandatory(), WithMandatoryDefault(false))
	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if flags := b.lastConn().channelAt(0).mandatoryFlags(); len(flags) != 1 || !flags[0] {
		t.Errorf("mandatory flags = %v, want [true]", flags)
	}
}

// Without WithMandatory the v1.2 behaviour stays: a return is not listened for
// and the ack is reported as success.
func TestMandatoryDefaultWithConfirmsKeepsOldBehaviour(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmReturn}
	})
	pub := conn.NewPublisher("events", WithConfirms(), WithMandatoryDefault(true))
	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v, want nil (returns are only reported with WithMandatory)", err)
	}
	ch := b.lastConn().channelAt(0)
	if ch.returnListeners() != 0 {
		t.Error("a return listener was registered without WithMandatory")
	}
	if _, ok := ch.publishedAt(0).Headers[PublishIDHeader]; ok {
		t.Error("the publish id header was added without WithMandatory")
	}
}

// A connection drop while the confirm is outstanding: the publish is retried on
// a fresh channel of the new connection, which listens for returns again.
func TestMandatoryPublishInFlightDuringReconnectIsRetried(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	first := b.lastConn()
	first.setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmNever}
	})
	pub := conn.NewPublisher("events", WithMandatory(), WithPublishRetries(5))

	done := make(chan error, 1)
	go func() { done <- pub.Publish(context.Background(), "k", []byte("x")) }()
	waitFor(t, time.Second, func() bool { return first.channelCount() == 1 && first.channelAt(0).pendingConfirms() == 1 })
	first.dropWith(amqp.ErrClosed)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish after reconnect: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Publish hung across the reconnect")
	}
	second := b.lastConn()
	if second == first {
		t.Fatal("no reconnect happened")
	}
	ch := second.channelAt(0)
	if ch == nil || ch.publishedCount() != 1 || ch.returnListeners() != 1 || !ch.inConfirmMode() {
		t.Error("the retry did not run on a fresh confirm-mode channel with a return listener")
	}
}

// A return that arrives on a channel dropped before its confirm is lost with
// the channel; the retry on the new channel decides the outcome.
func TestMandatoryReturnAfterRetryIsReported(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(idx int, ch *fakeChannel) {
		if idx == 0 {
			ch.confirmPlan = []confirmOutcome{confirmDrop}
			return
		}
		ch.confirmPlan = []confirmOutcome{confirmReturn}
	})
	pub := conn.NewPublisher("events", WithMandatory(), WithPublishRetries(3))
	err := pub.Publish(context.Background(), "k", []byte("x"))
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("err = %v, want ErrUnroutable from the retry", err)
	}
}

// When the reconnect is abandoned (MaxRetries spent), callers waiting for a
// connection get an error instead of blocking forever.
func TestAbandonedReconnectFailsWaitingPublish(t *testing.T) {
	t.Parallel()
	dialErr := errors.New("connection refused")
	b := &fakeBroker{dialErrs: []error{nil}, dialErr: dialErr}
	conn := newTestConn(t, b, WithBackoff(Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond, Factor: 2, MaxRetries: 2}))
	pub := conn.NewPublisher("events", WithMandatory(), WithPublishRetries(1))

	b.lastConn().dropWith(amqp.ErrClosed)
	waitFor(t, time.Second, func() bool { return b.dialsSoFar() >= 3 })

	done := make(chan error, 1)
	go func() { done <- pub.Publish(context.Background(), "k", []byte("x")) }()
	select {
	case err := <-done:
		if !errors.Is(err, ErrReconnectAbandoned) || !errors.Is(err, ErrNotReady) || !errors.Is(err, ErrPublishFailed) {
			t.Fatalf("err = %v, want ErrReconnectAbandoned, ErrNotReady and ErrPublishFailed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish hung after the reconnect was abandoned")
	}
	if conn.Healthy() {
		t.Error("Healthy after an abandoned reconnect")
	}
}
