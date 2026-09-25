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
)

// Tests for publisher confirms (issue #5).

func TestConfirmsOffByDefault(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events")

	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if n := b.lastConn().channelAt(0).confirmCallCount(); n != 0 {
		t.Errorf("Confirm called %d times without WithConfirms, want 0", n)
	}
}

func TestConfirmsAckReturnsNil(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	pub := conn.NewPublisher("events", WithConfirms())

	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ch := b.lastConn().channelAt(0)
	if !ch.inConfirmMode() {
		t.Error("channel was not put in confirm mode")
	}
	if ch.publishedCount() != 1 {
		t.Errorf("published %d, want 1", ch.publishedCount())
	}
}

func TestConfirmsNackReturnsErrorWithoutRetry(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmNack}
	})
	pub := conn.NewPublisher("events", WithConfirms(), WithPublishRetries(3))

	err := pub.Publish(context.Background(), "k", []byte("x"))
	if !errors.Is(err, ErrNacked) {
		t.Fatalf("err = %v, want ErrNacked", err)
	}
	if !errors.Is(err, ErrPublishFailed) {
		t.Errorf("err = %v, want it to also match ErrPublishFailed", err)
	}
	if n := b.lastConn().channelCount(); n != 1 {
		t.Errorf("opened %d channels, want 1 (a nack is not retried)", n)
	}
	if n := b.lastConn().channelAt(0).publishedCount(); n != 1 {
		t.Errorf("published %d times, want 1", n)
	}
}

func TestConfirmsTimeoutReturnsError(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmNever}
	})
	pub := conn.NewPublisher("events", WithConfirms(), WithConfirmTimeout(30*time.Millisecond))

	start := time.Now()
	err := pub.Publish(context.Background(), "k", []byte("x"))
	if !errors.Is(err, ErrConfirmTimeout) {
		t.Fatalf("err = %v, want ErrConfirmTimeout", err)
	}
	if !errors.Is(err, ErrPublishFailed) {
		t.Errorf("err = %v, want it to also match ErrPublishFailed", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Error("Publish did not honour the confirm timeout")
	}
}

func TestConfirmsCallerContextBoundsTheWait(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmNever}
	})
	pub := conn.NewPublisher("events", WithConfirms(), WithConfirmTimeout(time.Minute))

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := pub.Publish(ctx, "k", []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if errors.Is(err, ErrConfirmTimeout) {
		t.Error("a caller deadline must not be reported as ErrConfirmTimeout")
	}
}

// A channel that closes while a confirm is outstanding must never look like an
// ack. The publish is retried on a fresh channel, which is put in confirm mode
// again.
func TestConfirmsLostOnChannelDropIsRetried(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(idx int, ch *fakeChannel) {
		if idx == 0 {
			ch.confirmPlan = []confirmOutcome{confirmDrop}
		}
	})
	pub := conn.NewPublisher("events", WithConfirms(), WithPublishRetries(2))

	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	c := b.lastConn()
	if c.channelCount() != 2 {
		t.Fatalf("opened %d channels, want 2", c.channelCount())
	}
	if !c.channelAt(1).inConfirmMode() {
		t.Error("replacement channel was not put in confirm mode")
	}
	if c.channelAt(1).publishedCount() != 1 {
		t.Errorf("replacement channel published %d, want 1", c.channelAt(1).publishedCount())
	}
}

func TestConfirmsLostWithRetriesExhaustedIsAnError(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmDrop}
	})
	pub := conn.NewPublisher("events", WithConfirms(), WithPublishRetries(1))

	err := pub.Publish(context.Background(), "k", []byte("x"))
	if !errors.Is(err, ErrConfirmLost) {
		t.Fatalf("err = %v, want ErrConfirmLost", err)
	}
	if !errors.Is(err, ErrPublishFailed) {
		t.Errorf("err = %v, want it to also match ErrPublishFailed", err)
	}
}

// A whole-connection drop while waiting for a confirm: the Conn re-dials, and the
// publish is retried on a confirm-mode channel of the new connection.
func TestConfirmsSurviveConnectionDrop(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	first := b.lastConn()
	first.setChanTemplate(func(_ int, ch *fakeChannel) {
		ch.confirmPlan = []confirmOutcome{confirmNever}
	})
	pub := conn.NewPublisher("events", WithConfirms(), WithPublishRetries(5))

	errCh := make(chan error, 1)
	go func() { errCh <- pub.Publish(context.Background(), "k", []byte("x")) }()

	waitFor(t, 2*time.Second, func() bool {
		ch := first.channelAt(0)
		return ch != nil && ch.pendingConfirms() == 1
	})
	first.dropWith(nil)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Publish after reconnect: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish did not return after the connection dropped")
	}
	second := b.lastConn()
	if second == first {
		t.Fatal("connection was not re-dialled")
	}
	ch := second.channelAt(0)
	if ch == nil || !ch.inConfirmMode() || ch.publishedCount() != 1 {
		t.Errorf("message was not re-published on a confirm-mode channel of the new connection")
	}
}

func TestConfirmModeFailureIsRetried(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	b.lastConn().setChanTemplate(func(idx int, ch *fakeChannel) {
		if idx == 0 {
			ch.confirmErr = errors.New("confirm.select refused")
		}
	})
	pub := conn.NewPublisher("events", WithConfirms(), WithPublishRetries(2))

	if err := pub.Publish(context.Background(), "k", []byte("x")); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	c := b.lastConn()
	if c.channelAt(0).publishedCount() != 0 {
		t.Error("published on a channel that failed to enter confirm mode")
	}
	if !c.channelAt(1).inConfirmMode() || c.channelAt(1).publishedCount() != 1 {
		t.Error("expected the publish on a second, confirm-mode channel")
	}
}
