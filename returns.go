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
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Reporting of unroutable mandatory publishes (issue #20).

// PublishIDHeader is the message header a publisher created WithMandatory adds
// to every message, so a basic.return can be matched to the publish it belongs
// to. basic.return carries no delivery tag, and many publishes can be in
// flight on one channel. Consumers see the header and can ignore it.
const PublishIDHeader = "x-go-rabbitmq-publish-id"

const (
	// returnBuffer is the capacity of each channel's basic.return listener.
	// amqp091 drops a return if the listener stays full for five seconds.
	returnBuffer = 256
	// returnDrainInterval is how often the background drain empties the
	// listener, so it never fills while publishes are waiting.
	returnDrainInterval = 20 * time.Millisecond
	// returnKeep is how long a return nobody claimed (its publish gave up
	// waiting) is kept before it is discarded.
	returnKeep = time.Minute
)

type heldReturn struct {
	ret amqp.Return
	at  time.Time
}

// returnTracker collects the basic.return messages of one channel and hands
// each to the publish it belongs to.
//
// RabbitMQ sends the return of an unroutable mandatory message before the ack
// of that message, and amqp091 hands the return to the listener before it
// settles the confirm. So once a publish sees its ack, its return (if any) is
// already in the listener buffer or recorded. Every receive from the buffer
// happens with mu held and is recorded before mu is released, so take, which
// drains under mu, cannot miss a return that is on its way to being recorded.
type returnTracker struct {
	log      Logger
	returns  chan amqp.Return
	done     chan struct{}
	stopOnce sync.Once

	mu     sync.Mutex
	held   map[string]heldReturn
	closed bool
}

// newReturnTracker registers a return listener on ch and starts the background
// drain. The drain stops when the channel closes (amqp091 then closes the
// listener) or when stop is called.
func newReturnTracker(ch wireChannel, log Logger) *returnTracker {
	t := &returnTracker{
		log:     log,
		returns: ch.NotifyReturn(make(chan amqp.Return, returnBuffer)),
		done:    make(chan struct{}),
		held:    make(map[string]heldReturn),
	}
	go t.run()
	return t
}

// run drains the listener every returnDrainInterval until it is closed.
func (t *returnTracker) run() {
	tick := time.NewTicker(returnDrainInterval)
	defer tick.Stop()
	for {
		select {
		case <-t.done:
			return
		case <-tick.C:
		}
		t.mu.Lock()
		t.drainLocked()
		t.pruneLocked(time.Now())
		closed := t.closed
		t.mu.Unlock()
		if closed {
			return
		}
	}
}

// stop ends the background drain. It is safe to call more than once.
func (t *returnTracker) stop() { t.stopOnce.Do(func() { close(t.done) }) }

// drainLocked records every return waiting in the listener without blocking.
// t.mu must be held.
func (t *returnTracker) drainLocked() {
	for {
		select {
		case r, ok := <-t.returns:
			if !ok {
				t.closed = true
				return
			}
			id, _ := r.Headers[PublishIDHeader].(string)
			if id == "" {
				t.log.Warnf("rabbitmq: basic.return from exchange %q key %q without %s; ignored",
					r.Exchange, r.RoutingKey, PublishIDHeader)
				continue
			}
			t.log.Debugf("rabbitmq: basic.return %d %s for publish %s (exchange %q, key %q)",
				r.ReplyCode, r.ReplyText, id, r.Exchange, r.RoutingKey)
			t.held[id] = heldReturn{ret: r, at: time.Now()}
		default:
			return
		}
	}
}

// pruneLocked drops returns whose publish stopped waiting long ago. t.mu must
// be held.
func (t *returnTracker) pruneLocked(now time.Time) {
	for id, h := range t.held {
		if now.Sub(h.at) > returnKeep {
			t.log.Debugf("rabbitmq: discarding unclaimed basic.return for publish %s", id)
			delete(t.held, id)
		}
	}
}

// take reports the return of publish id, if the broker sent one, and forgets
// it. Call it once the publish has its confirm.
func (t *returnTracker) take(id string) (amqp.Return, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.drainLocked()
	h, ok := t.held[id]
	delete(t.held, id)
	return h.ret, ok
}

// publishIDs generates the PublishIDHeader values of one publisher: a random
// prefix plus a counter, unique across publishers and processes.
type publishIDs struct {
	prefix string
	n      atomic.Uint64
}

func newPublishIDs() *publishIDs {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return &publishIDs{prefix: hex.EncodeToString(b) + "-"}
}

func (g *publishIDs) next() string { return g.prefix + strconv.FormatUint(g.n.Add(1), 10) }

// withPublishID returns a copy of msg whose headers carry id. The caller's
// header table is not mutated.
func withPublishID(msg amqp.Publishing, id string) amqp.Publishing {
	h := make(amqp.Table, len(msg.Headers)+1)
	for k, v := range msg.Headers {
		h[k] = v
	}
	h[PublishIDHeader] = id
	msg.Headers = h
	return msg
}

// unroutable builds the error for a returned message.
func unroutable(r amqp.Return) *UnroutableError {
	return &UnroutableError{
		ReplyCode:  r.ReplyCode,
		ReplyText:  r.ReplyText,
		Exchange:   r.Exchange,
		RoutingKey: r.RoutingKey,
		MessageID:  r.MessageId,
	}
}
