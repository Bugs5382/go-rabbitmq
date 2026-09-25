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
	"strings"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// statusLog records every status passed to OnStatus.
type statusLog struct {
	mu   sync.Mutex
	seen []ConsumerStatus
}

func (s *statusLog) record(st ConsumerStatus) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seen = append(s.seen, st)
}

func (s *statusLog) all() []ConsumerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ConsumerStatus(nil), s.seen...)
}

func runConsumer(t *testing.T, cons *Consumer) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cons.Run(ctx) }()
	t.Cleanup(cancel)
	return cancel, done
}

// Issue #13: a new consumer is not ready, becomes ready once it consumes, and
// reports stopped (not ready) with the reason after Run returns.
func TestConsumerReadyLifecycle(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	col := &collector{}
	cons := conn.NewConsumer(basicConsumerCfg(), col.handle)

	if cons.Ready() {
		t.Fatal("a consumer that has not run must not be ready")
	}
	if st := cons.Status(); st.State != ConsumerStarting {
		t.Fatalf("initial state = %v, want starting", st.State)
	}

	cancel, done := runConsumer(t, cons)
	waitFor(t, time.Second, cons.Ready)
	st := cons.Status()
	if st.State != ConsumerConsuming || st.Queue != "orders" || st.Err != nil || st.Attempt != 0 {
		t.Errorf("status while consuming = %+v", st)
	}
	if st.Since.IsZero() {
		t.Error("Since must be set")
	}

	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("Run returned %v, want context.Canceled", err)
	}
	st = cons.Status()
	if cons.Ready() || st.State != ConsumerStopped || !errors.Is(st.Err, context.Canceled) {
		t.Errorf("status after stop = %+v, want stopped with context.Canceled", st)
	}
}

// Issue #13: a declare failure reads as retrying, with the last error and the
// attempt, until a declare succeeds. The consumer is never ready in between.
func TestConsumerDeclareFailureNotReadyUntilSuccess(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	log := &recordingLogger{}
	conn := newTestConn(t, b, WithLogger(log))
	declareErr := &amqp.Error{Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - inequivalent arg"}
	const failures = 3
	b.lastConn().setChanTemplate(func(idx int, ch *fakeChannel) {
		if idx < failures {
			ch.declareErr = declareErr
		}
	})

	statuses := &statusLog{}
	cfg := basicConsumerCfg()
	cfg.OnStatus = statuses.record
	cons := conn.NewConsumer(cfg, (&collector{}).handle)
	runConsumer(t, cons)
	waitFor(t, 2*time.Second, cons.Ready)

	var retries []ConsumerStatus
	for _, st := range statuses.all() {
		if st.State == ConsumerRetrying {
			retries = append(retries, st)
		}
	}
	if len(retries) != failures {
		t.Fatalf("saw %d retrying statuses, want %d: %+v", len(retries), failures, statuses.all())
	}
	for i, st := range retries {
		if st.Attempt != i+1 {
			t.Errorf("retry %d: attempt = %d, want %d", i, st.Attempt, i+1)
		}
		if !errors.Is(st.Err, declareErr) {
			t.Errorf("retry %d: err = %v, want the declare error", i, st.Err)
		}
		if st.RetryIn <= 0 {
			t.Errorf("retry %d: RetryIn = %v, want a positive backoff", i, st.RetryIn)
		}
	}
	all := statuses.all()
	for i, st := range all[:len(all)-1] {
		if st.State == ConsumerConsuming {
			t.Errorf("status %d reported consuming before the declare succeeded: %+v", i, st)
		}
	}
	if last := all[len(all)-1]; last.State != ConsumerConsuming || last.Err != nil || last.Attempt != 0 {
		t.Errorf("final status = %+v, want consuming with no error", last)
	}
	for attempt := 1; attempt <= failures; attempt++ {
		if !log.contains("WARN", fmt.Sprintf("attempt %d", attempt)) {
			t.Errorf("no warning for attempt %d: %v", attempt, log.lines)
		}
	}
	if !log.contains("WARN", `"orders"`) || !log.contains("WARN", "inequivalent arg") || !log.contains("WARN", "retrying in") {
		t.Errorf("retry warnings must name the queue, the error and the backoff: %v", log.lines)
	}
}

// A bind failure also keeps the consumer not ready until it succeeds.
func TestConsumerBindFailureNotReadyUntilSuccess(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	bindErr := errors.New("NOT_FOUND - no exchange 'events'")
	var mu sync.Mutex
	failing := true
	b.lastConn().setChanTemplate(func(_ int, ch *fakeChannel) {
		mu.Lock()
		defer mu.Unlock()
		if failing {
			ch.bindErr = bindErr
		}
	})
	cons := conn.NewConsumer(basicConsumerCfg(), (&collector{}).handle)
	runConsumer(t, cons)

	waitFor(t, time.Second, func() bool { return cons.Status().Attempt >= 2 })
	st := cons.Status()
	if cons.Ready() || st.State != ConsumerRetrying || !errors.Is(st.Err, bindErr) {
		t.Fatalf("status during bind failures = %+v, want retrying with the bind error", st)
	}
	if !strings.Contains(st.Err.Error(), "bind") {
		t.Errorf("error %q should say the bind failed", st.Err)
	}

	mu.Lock()
	failing = false
	mu.Unlock()
	waitFor(t, time.Second, cons.Ready)
}

// After a drop the consumer is not ready until it consumes again, and the
// attempt count starts over after a healthy session.
func TestConsumerDropReadsNotReadyUntilResumed(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	statuses := &statusLog{}
	cfg := basicConsumerCfg()
	cfg.OnStatus = statuses.record
	cons := conn.NewConsumer(cfg, (&collector{}).handle)
	runConsumer(t, cons)
	waitFor(t, time.Second, cons.Ready)

	first := b.lastConn()
	first.dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})
	waitFor(t, time.Second, func() bool { return b.lastConn() != first && cons.Ready() })

	var sawRetry bool
	for _, st := range statuses.all() {
		if st.State == ConsumerRetrying {
			sawRetry = true
			if st.Attempt != 1 {
				t.Errorf("first retry after a healthy session has attempt %d, want 1", st.Attempt)
			}
		}
	}
	if !sawRetry {
		t.Errorf("a drop must be reported as retrying: %+v", statuses.all())
	}
}

// Consecutive failures back off further each time, capped at Backoff.Max.
func TestConsumerRetryDelayEscalates(t *testing.T) {
	t.Parallel()
	bo := Backoff{Initial: 10 * time.Millisecond, Max: 80 * time.Millisecond, Factor: 2, Jitter: 0}.normalize()
	want := []time.Duration{10, 20, 40, 80, 80}
	for i, w := range want {
		if got := consumerRetryDelay(bo, i+1); got != w*time.Millisecond {
			t.Errorf("attempt %d: delay = %v, want %v", i+1, got, w*time.Millisecond)
		}
	}
}

// Run is not re-entrant: a second concurrent Run fails fast.
func TestConsumerRunTwiceConcurrently(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	cons := conn.NewConsumer(basicConsumerCfg(), (&collector{}).handle)
	runConsumer(t, cons)
	waitFor(t, time.Second, cons.Ready)
	if err := cons.Run(context.Background()); !errors.Is(err, ErrConsumerRunning) {
		t.Errorf("second Run returned %v, want ErrConsumerRunning", err)
	}
	if !cons.Ready() {
		t.Error("the rejected second Run must not change the running consumer's status")
	}
}

// Closing the Conn stops the consumer with ErrClosed.
func TestConsumerStopsWhenConnCloses(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn := newTestConn(t, b)
	cons := conn.NewConsumer(basicConsumerCfg(), (&collector{}).handle)
	_, done := runConsumer(t, cons)
	waitFor(t, time.Second, cons.Ready)
	_ = conn.Close()
	if err := <-done; !errors.Is(err, ErrClosed) {
		t.Errorf("Run returned %v, want ErrClosed", err)
	}
	if st := cons.Status(); st.State != ConsumerStopped || !errors.Is(st.Err, ErrClosed) {
		t.Errorf("status = %+v, want stopped with ErrClosed", st)
	}
}

func TestConsumerStateString(t *testing.T) {
	t.Parallel()
	for state, want := range map[ConsumerState]string{
		ConsumerStarting:  "starting",
		ConsumerConsuming: "consuming",
		ConsumerRetrying:  "retrying",
		ConsumerStopped:   "stopped",
		ConsumerState(99): "ConsumerState(99)",
	} {
		if got := state.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int(state), got, want)
		}
	}
}
