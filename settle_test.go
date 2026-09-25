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
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Tests for per-message settlement from the handler (issue #6).

// startConsumer runs Consume with handler on a fresh fake broker and returns the
// channel it consumes from.
func startConsumer(t *testing.T, cfg ConsumerConfig, handler Handler) (*fakeBroker, *fakeChannel) {
	t.Helper()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = conn.Consume(ctx, cfg, handler) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch.consumeStarted() })
	return b, ch
}

func returning(err error) Handler {
	return func(context.Context, Delivery) error { return err }
}

func TestSettleErrRequeueRequeuesEvenWithNoRequeue(t *testing.T) {
	t.Parallel()
	_, ch := startConsumer(t, basicConsumerCfg().NoRequeue(), returning(fmt.Errorf("db busy: %w", ErrRequeue)))

	ch.deliver(amqp.Delivery{DeliveryTag: 3, Body: []byte("m")})
	waitFor(t, time.Second, func() bool { return ch.settleCount() == 1 })

	nr := ch.nackRecords()
	if len(nr) != 1 || nr[0].tag != 3 || !nr[0].requeue || nr[0].multiple {
		t.Errorf("nacks = %+v, want one nack of tag 3 with requeue=true", nr)
	}
}

func TestSettleErrDeadLetterRejectsEvenWithRequeueDefault(t *testing.T) {
	t.Parallel()
	cfg := basicConsumerCfg()
	cfg.Queue.Args = amqp.Table{"x-dead-letter-exchange": "events.dlx"}
	_, ch := startConsumer(t, cfg, returning(fmt.Errorf("bad payload: %w", ErrDeadLetter)))

	ch.deliver(amqp.Delivery{DeliveryTag: 5, Body: []byte("poison")})
	waitFor(t, time.Second, func() bool { return ch.settleCount() == 1 })

	rr := ch.rejectRecords()
	if len(rr) != 1 || rr[0].tag != 5 || rr[0].requeue {
		t.Errorf("rejects = %+v, want one reject of tag 5 with requeue=false", rr)
	}
	if len(ch.nackRecords()) != 0 {
		t.Error("a dead-lettered message must not also be nacked")
	}
	if q := ch.declaredQueues(); len(q) != 1 || q[0].Args["x-dead-letter-exchange"] != "events.dlx" {
		t.Errorf("queue not declared with its dead-letter exchange: %+v", q)
	}
}

func TestSettleDeadLetterWinsOverRequeue(t *testing.T) {
	t.Parallel()
	_, ch := startConsumer(t, basicConsumerCfg(), returning(errors.Join(ErrRequeue, ErrDeadLetter)))

	ch.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("m")})
	waitFor(t, time.Second, func() bool { return ch.settleCount() == 1 })
	if len(ch.rejectRecords()) != 1 {
		t.Errorf("an error matching both sentinels must dead-letter; nacks=%+v rejects=%+v",
			ch.nackRecords(), ch.rejectRecords())
	}
}

func TestSettlePlainErrorKeepsRequeueOnError(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		cfg     ConsumerConfig
		requeue bool
	}{
		"default":    {basicConsumerCfg(), true},
		"no requeue": {basicConsumerCfg().NoRequeue(), false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, ch := startConsumer(t, tc.cfg, returning(errors.New("plain")))
			ch.deliver(amqp.Delivery{DeliveryTag: 2, Body: []byte("m")})
			waitFor(t, time.Second, func() bool { return ch.settleCount() == 1 })
			nr := ch.nackRecords()
			if len(nr) != 1 || nr[0].requeue != tc.requeue {
				t.Errorf("nacks = %+v, want requeue=%t", nr, tc.requeue)
			}
		})
	}
}

func TestSettleNilAcks(t *testing.T) {
	t.Parallel()
	_, ch := startConsumer(t, basicConsumerCfg(), returning(nil))
	ch.deliver(amqp.Delivery{DeliveryTag: 8, Body: []byte("m")})
	waitFor(t, time.Second, func() bool { return ch.settleCount() == 1 })
	if tags := ch.ackedTags(); len(tags) != 1 || tags[0] != 8 {
		t.Errorf("acks = %v, want [8]", tags)
	}
}

func TestSettleSentinelsIgnoredWithAutoAck(t *testing.T) {
	t.Parallel()
	cfg := basicConsumerCfg()
	cfg.AutoAck = true
	handled := make(chan struct{}, 2)
	h := func(_ context.Context, d Delivery) error {
		handled <- struct{}{}
		if string(d.Body) == "a" {
			return ErrRequeue
		}
		return ErrDeadLetter
	}
	_, ch := startConsumer(t, cfg, h)
	ch.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("a")})
	ch.deliver(amqp.Delivery{DeliveryTag: 2, Body: []byte("b")})
	for range 2 {
		select {
		case <-handled:
		case <-time.After(time.Second):
			t.Fatal("handler not invoked")
		}
	}
	time.Sleep(20 * time.Millisecond)
	if n := ch.settleCount(); n != 0 {
		t.Errorf("auto-ack consumer settled %d messages, want 0", n)
	}
}

// A delivery whose channel closed while the handler ran must not be settled:
// its tag is meaningless on any other channel, and the broker redelivers it.
func TestSettleSkippedWhenChannelClosedDuringHandler(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	h := func(_ context.Context, d Delivery) error {
		if string(d.Body) == "slow" {
			started <- struct{}{}
			<-release
		}
		return nil
	}
	b, ch1 := startConsumer(t, basicConsumerCfg(), h)

	ch1.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("slow")})
	<-started
	_ = ch1.Close()
	close(release)

	// The consumer re-establishes on a new channel and keeps working there.
	waitFor(t, 2*time.Second, func() bool { return b.lastConn().channelCount() >= 2 })
	ch2 := b.lastConn().channelAt(1)
	waitFor(t, 2*time.Second, func() bool { return ch2.consumeStarted() })
	ch2.deliver(amqp.Delivery{DeliveryTag: 1, Body: []byte("fresh")})
	waitFor(t, time.Second, func() bool { return len(ch2.ackedTags()) == 1 })

	if n := ch1.settleCount(); n != 0 {
		t.Errorf("settled %d deliveries on the closed channel, want 0", n)
	}
	if n := ch2.settleCount(); n != 1 {
		t.Errorf("new channel settled %d deliveries, want only its own 1", n)
	}
}
