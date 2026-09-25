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
	"sync/atomic"
	"time"
)

// ConsumerState is where a Consumer is in its lifecycle (issue #13).
type ConsumerState int

const (
	// ConsumerStarting is the state of a Consumer that has not consumed yet: Run
	// has not been called, or its first session is still being set up.
	ConsumerStarting ConsumerState = iota
	// ConsumerConsuming means the topology is declared and the broker is
	// delivering to this consumer. It is the only ready state.
	ConsumerConsuming
	// ConsumerRetrying means the last session failed or dropped (a declare, bind,
	// QoS or consume error, a channel close, or a lost connection) and the
	// consumer is backing off or setting up again. ConsumerStatus.Err holds the
	// last error.
	ConsumerRetrying
	// ConsumerStopped means Run returned. ConsumerStatus.Err holds the error it
	// returned (ctx.Err(), ErrClosed).
	ConsumerStopped
)

// String returns the lower-case state name.
func (s ConsumerState) String() string {
	switch s {
	case ConsumerStarting:
		return "starting"
	case ConsumerConsuming:
		return "consuming"
	case ConsumerRetrying:
		return "retrying"
	case ConsumerStopped:
		return "stopped"
	default:
		return fmt.Sprintf("ConsumerState(%d)", int(s))
	}
}

// ConsumerStatus is a snapshot of a Consumer's state.
type ConsumerStatus struct {
	// State is the lifecycle state.
	State ConsumerState
	// Queue is the queue being consumed: the broker-assigned name once a
	// server-named queue has been declared, the configured name otherwise.
	Queue string
	// Err is the last error. While retrying it is the error that ended the last
	// attempt; once stopped it is the error Run returned. It is nil otherwise.
	Err error
	// Attempt counts consecutive failed sessions while retrying, starting at 1.
	// It goes back to 0 once the consumer is consuming again.
	Attempt int
	// RetryIn is the backoff before the next attempt while retrying.
	RetryIn time.Duration
	// Since is when the consumer entered this status.
	Since time.Time
}

// errSessionDropped stands in for the error of a session that ended without
// one, for example when the broker cancels the consumer or the connection
// closes cleanly.
var errSessionDropped = errors.New("rabbitmq: consumer session ended; re-establishing")

// Consumer is a resilient consumer that reports its own readiness. Create one
// with Conn.NewConsumer and start it with Run.
//
// Conn.Healthy only says the connection is up. A consumer whose queue declare
// keeps failing retries forever on a healthy connection, so a service should
// drive its readiness from Consumer.Ready instead (issue #13).
type Consumer struct {
	conn    *Conn
	cfg     ConsumerConfig
	handler Handler
	running atomic.Bool

	mu     sync.RWMutex
	status ConsumerStatus
}

// NewConsumer returns a Consumer for cfg and handler. Nothing touches the broker
// until Run is called. The consumer starts in ConsumerStarting and is not ready.
func (c *Conn) NewConsumer(cfg ConsumerConfig, handler Handler) *Consumer {
	cfg = cfg.normalize()
	return &Consumer{
		conn:    c,
		cfg:     cfg,
		handler: handler,
		status:  ConsumerStatus{State: ConsumerStarting, Queue: cfg.Queue.Name, Since: time.Now()},
	}
}

// Status returns a snapshot of the consumer's state. It is safe to call from any
// goroutine.
func (cons *Consumer) Status() ConsumerStatus {
	cons.mu.RLock()
	defer cons.mu.RUnlock()
	return cons.status
}

// Ready reports whether the consumer is consuming right now: its topology is
// declared and the broker is delivering to it. It is false before the first
// session is set up, while a declare, bind or reconnect is being retried, and
// after Run returns.
func (cons *Consumer) Ready() bool {
	return cons.Status().State == ConsumerConsuming
}

// setStatus records a new status, logs the transition and calls OnStatus. It
// runs only on the Run goroutine, so OnStatus sees statuses in order.
func (cons *Consumer) setStatus(st ConsumerStatus) {
	st.Since = time.Now()
	cons.mu.Lock()
	prev := cons.status.State
	if st.Queue == "" {
		st.Queue = cons.status.Queue
	}
	cons.status = st
	cons.mu.Unlock()

	if prev != st.State {
		cons.conn.log.Infof("rabbitmq: consumer on %q is %s (was %s)", st.Queue, st.State, prev)
	}
	if cons.cfg.OnStatus != nil {
		cons.cfg.OnStatus(st)
	}
}

// consumerRetryDelay is the backoff before retry number attempt (starting at 1)
// of a consumer session. It escalates with consecutive failures, so a declare
// that keeps failing does not hammer the broker; attempt 1 uses the first,
// short step so a plain drop resumes quickly.
func consumerRetryDelay(b Backoff, attempt int) time.Duration {
	return b.delay(attempt - 1)
}

// Run consumes until ctx is cancelled or the Conn is closed. It (re-)declares
// the configured topology, sets QoS, and consumes, dispatching each delivery to
// the handler. When a session fails or drops it reports ConsumerRetrying, waits
// with escalating backoff, and sets up again; there is no retry limit. It
// returns ctx.Err() when ctx is cancelled, ErrClosed if the Conn is closed, or
// ErrConsumerRunning if this Consumer is already running.
//
// Run blocks; call it in its own goroutine. Deliveries are dispatched
// sequentially on that goroutine, so a handler that must run concurrently should
// fan out internally (respecting Prefetch for backpressure). A Consumer can be
// run again after Run returns.
func (cons *Consumer) Run(ctx context.Context) error {
	if !cons.running.CompareAndSwap(false, true) {
		return ErrConsumerRunning
	}
	defer cons.running.Store(false)

	c, cfg, log := cons.conn, cons.cfg, cons.conn.log
	log.Debugf("rabbitmq: consumer on %q starting (prefetch %d, auto-ack %t)", cfg.Queue.Name, cfg.Prefetch, cfg.AutoAck)
	cons.setStatus(ConsumerStatus{State: ConsumerStarting})

	stop := func(err error) error {
		cons.setStatus(ConsumerStatus{State: ConsumerStopped, Err: err})
		log.Infof("rabbitmq: consumer on %q stopped: %v", cons.Status().Queue, err)
		return err
	}

	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return stop(err)
		}
		consumed := false
		err := c.consumeSession(ctx, cfg, cons.handler, func(queue string) {
			consumed = true
			attempt = 0
			cons.setStatus(ConsumerStatus{State: ConsumerConsuming, Queue: queue})
		})
		switch {
		case ctx.Err() != nil:
			return stop(ctx.Err())
		case c.isClosed():
			return stop(ErrClosed)
		case err == nil:
			err = errSessionDropped
		}

		attempt++
		wait := consumerRetryDelay(c.opts.backoff, attempt)
		queue := cons.Status().Queue
		if consumed {
			log.Warnf("rabbitmq: consumer on %q lost its session (attempt %d): %v; retrying in %s", queue, attempt, err, wait)
		} else {
			log.Warnf("rabbitmq: consumer on %q not ready (attempt %d): %v; retrying in %s", queue, attempt, err, wait)
		}
		cons.setStatus(ConsumerStatus{State: ConsumerRetrying, Err: err, Attempt: attempt, RetryIn: wait})

		select {
		case <-ctx.Done():
			return stop(ctx.Err())
		case <-c.ctx.Done():
			return stop(ErrClosed)
		case <-time.After(wait):
		}
		log.Debugf("rabbitmq: consumer on %q retrying now (attempt %d)", queue, attempt)
	}
}
