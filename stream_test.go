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

// Tests for stream queues (issue #19).

func streamQueue() QueueConfig {
	return QueueConfig{Name: "events.log", Type: QueueStream}
}

func TestStreamQueueDeclaresStreamTypeAndStaysDurable(t *testing.T) {
	t.Parallel()
	q := streamQueue().normalize()
	if err := q.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if !q.Durable {
		t.Error("a stream queue must be durable by default")
	}
	args := q.args()
	if args["x-queue-type"] != "stream" {
		t.Errorf("x-queue-type = %v, want stream", args["x-queue-type"])
	}
	for _, k := range []string{"x-max-age", "x-max-length-bytes", "x-stream-max-segment-size-bytes"} {
		if _, ok := args[k]; ok {
			t.Errorf("%s set without a retention option", k)
		}
	}
}

func TestStreamRetentionOptionsBecomeArgs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		age  time.Duration
		want string
	}{
		{7 * 24 * time.Hour, "7D"},
		{36 * time.Hour, "36h"},
		{90 * time.Minute, "90m"},
		{45 * time.Second, "45s"},
		{61 * time.Second, "61s"},
	}
	for _, tc := range cases {
		q := streamQueue()
		q.Stream = StreamOptions{MaxAge: tc.age, MaxLengthBytes: 20_000_000_000, MaxSegmentSizeBytes: 100_000_000}
		q = q.normalize()
		if err := q.validate(); err != nil {
			t.Fatalf("validate %s: %v", tc.age, err)
		}
		args := q.args()
		if args["x-max-age"] != tc.want {
			t.Errorf("x-max-age for %s = %v, want %q", tc.age, args["x-max-age"], tc.want)
		}
		if args["x-max-length-bytes"] != int64(20_000_000_000) {
			t.Errorf("x-max-length-bytes = %#v", args["x-max-length-bytes"])
		}
		if args["x-stream-max-segment-size-bytes"] != int64(100_000_000) {
			t.Errorf("x-stream-max-segment-size-bytes = %#v", args["x-stream-max-segment-size-bytes"])
		}
	}
}

func TestStreamArgsDoNotMutateCallerArgs(t *testing.T) {
	t.Parallel()
	caller := amqp.Table{"x-initial-cluster-size": int32(3)}
	q := streamQueue()
	q.Args = caller
	q.Stream.MaxAge = time.Hour
	args := q.normalize().args()
	if args["x-initial-cluster-size"] != int32(3) {
		t.Error("caller args were dropped")
	}
	if len(caller) != 1 {
		t.Errorf("caller args were mutated: %v", caller)
	}
}

func TestStreamQueueValidationGuards(t *testing.T) {
	t.Parallel()
	cases := map[string]QueueConfig{
		"server-named":        {Type: QueueStream},
		"exclusive":           {Name: "s", Type: QueueStream, Exclusive: true},
		"auto-delete":         {Name: "s", Type: QueueStream, AutoDelete: true},
		"non-durable":         QueueConfig{Name: "s", Type: QueueStream}.Transient(),
		"negative max age":    {Name: "s", Type: QueueStream, Stream: StreamOptions{MaxAge: -time.Second}},
		"sub-second max age":  {Name: "s", Type: QueueStream, Stream: StreamOptions{MaxAge: 1500 * time.Millisecond}},
		"negative max length": {Name: "s", Type: QueueStream, Stream: StreamOptions{MaxLengthBytes: -1}},
		"negative segment":    {Name: "s", Type: QueueStream, Stream: StreamOptions{MaxSegmentSizeBytes: -1}},
		"options on classic":  {Name: "c", Stream: StreamOptions{MaxAge: time.Hour}},
		"options on quorum":   {Name: "q", Type: QueueQuorum, Stream: StreamOptions{MaxLengthBytes: 1}},
		"unknown type":        {Name: "x", Type: QueueType("lazy")},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := cfg.normalize().validate()
			if !errors.Is(err, ErrInvalidQueue) {
				t.Errorf("want ErrInvalidQueue, got %v", err)
			}
		})
	}
}

func TestStreamQueueRejectedBeforeDeclare(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	_, err := conn.DeclareQueue(context.Background(), QueueConfig{Name: "s", Type: QueueStream, Exclusive: true})
	if !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("err = %v, want ErrInvalidQueue", err)
	}
	if n := len(b.lastConn().channelAt(0).declaredQueues()); n != 0 {
		t.Errorf("declared %d queues, want 0", n)
	}
}

func TestStreamOffsetArgs(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		off  StreamOffset
		want any
	}{
		{"first", StreamOffsetFirst(), "first"},
		{"last", StreamOffsetLast(), "last"},
		{"next", StreamOffsetNext(), "next"},
		{"offset", StreamOffsetAt(42), int64(42)},
		{"offset zero", StreamOffsetAt(0), int64(0)},
		{"timestamp", StreamOffsetTime(at), at},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.off.IsZero() {
				t.Fatal("a set offset must not be zero")
			}
			got, err := tc.off.arg()
			if err != nil {
				t.Fatalf("arg: %v", err)
			}
			if got != tc.want {
				t.Errorf("arg = %#v, want %#v", got, tc.want)
			}
			if tc.off.String() == "" {
				t.Error("String is empty")
			}
		})
	}
	if !(StreamOffset{}).IsZero() {
		t.Error("the zero StreamOffset must report IsZero")
	}
	if _, err := StreamOffsetAt(-1).arg(); !errors.Is(err, ErrInvalidConsumer) {
		t.Errorf("negative offset: err = %v, want ErrInvalidConsumer", err)
	}
	if _, err := StreamOffsetTime(time.Time{}).arg(); !errors.Is(err, ErrInvalidConsumer) {
		t.Errorf("zero timestamp: err = %v, want ErrInvalidConsumer", err)
	}
}

func streamConsumerCfg(off StreamOffset) ConsumerConfig {
	return ConsumerConfig{Queue: streamQueue(), StreamOffset: off, Prefetch: 50}
}

func TestStreamConsumerSendsOffsetAndQos(t *testing.T) {
	t.Parallel()
	cfg := streamConsumerCfg(StreamOffsetFirst())
	cfg.Args = amqp.Table{"x-priority": int32(5)}
	_, ch := startConsumer(t, cfg, returning(nil))

	args := ch.lastConsumeArgs()
	if args["x-stream-offset"] != "first" {
		t.Errorf("x-stream-offset = %#v, want first", args["x-stream-offset"])
	}
	if args["x-priority"] != int32(5) {
		t.Error("caller consume args were dropped")
	}
	if len(cfg.Args) != 1 {
		t.Errorf("caller consume args were mutated: %v", cfg.Args)
	}
	if q := ch.qosArgs(); q == nil || q.prefetchCount != 50 {
		t.Errorf("qos = %+v, want prefetch 50", q)
	}
	if qs := ch.declaredQueues(); len(qs) != 1 || qs[0].Args["x-queue-type"] != "stream" {
		t.Errorf("declared queues = %+v, want one stream", qs)
	}
}

func TestStreamConsumerWithoutOffsetUsesBrokerDefault(t *testing.T) {
	t.Parallel()
	_, ch := startConsumer(t, streamConsumerCfg(StreamOffset{}), returning(nil))
	if _, ok := ch.lastConsumeArgs()["x-stream-offset"]; ok {
		t.Error("x-stream-offset sent without a StreamOffset")
	}
	if q := ch.qosArgs(); q == nil || q.prefetchCount <= 0 {
		t.Errorf("qos = %+v, want a prefetch", q)
	}
}

func TestStreamConsumerConfigRejectedWithoutRetry(t *testing.T) {
	t.Parallel()
	autoAck := streamConsumerCfg(StreamOffsetFirst())
	autoAck.AutoAck = true
	cases := map[string]struct {
		cfg  ConsumerConfig
		want error
	}{
		"auto-ack stream":     {autoAck, ErrInvalidConsumer},
		"offset on classic":   {ConsumerConfig{Queue: QueueConfig{Name: "c"}, StreamOffset: StreamOffsetFirst()}, ErrInvalidConsumer},
		"negative offset":     {streamConsumerCfg(StreamOffsetAt(-5)), ErrInvalidConsumer},
		"exclusive stream":    {ConsumerConfig{Queue: QueueConfig{Name: "s", Type: QueueStream, Exclusive: true}}, ErrInvalidQueue},
		"server-named stream": {ConsumerConfig{Queue: QueueConfig{Type: QueueStream}}, ErrInvalidQueue},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			b := &fakeBroker{}
			conn := newTestConn(t, b)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			cons := conn.NewConsumer(tc.cfg, returning(nil))
			err := cons.Run(ctx)
			if !errors.Is(err, tc.want) {
				t.Fatalf("Run err = %v, want %v", err, tc.want)
			}
			if st := cons.Status(); st.State != ConsumerStopped || !errors.Is(st.Err, tc.want) {
				t.Errorf("status = %+v, want stopped with the error", st)
			}
			if n := b.lastConn().channelCount(); n != 0 {
				t.Errorf("opened %d channels, want 0 (rejected before touching the broker)", n)
			}
		})
	}
}

// After a reconnect a stream consumer resumes after the last offset it handled,
// instead of replaying from its configured start.
func TestStreamConsumerResumesAfterLastOffset(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = conn.Consume(ctx, streamConsumerCfg(StreamOffsetFirst()), col.handle) }()

	waitFor(t, time.Second, func() bool { return b.lastConn().channelCount() >= 1 })
	ch1 := b.lastConn().channelAt(0)
	waitFor(t, time.Second, func() bool { return ch1.consumeStarted() })
	if got := ch1.lastConsumeArgs()["x-stream-offset"]; got != "first" {
		t.Fatalf("first session offset = %#v, want first", got)
	}

	for i, off := range []int64{7, 8, 9} {
		ch1.deliver(amqp.Delivery{DeliveryTag: uint64(i + 1), Body: []byte("m"), Headers: amqp.Table{"x-stream-offset": off}})
	}
	waitFor(t, time.Second, func() bool { return len(ch1.ackedTags()) == 3 })

	_ = ch1.Close()
	waitFor(t, 2*time.Second, func() bool { return b.lastConn().channelCount() >= 2 })
	ch2 := b.lastConn().channelAt(1)
	waitFor(t, 2*time.Second, func() bool { return ch2.consumeStarted() })
	if got := ch2.lastConsumeArgs()["x-stream-offset"]; got != int64(10) {
		t.Errorf("resumed offset = %#v, want int64(10)", got)
	}
}
