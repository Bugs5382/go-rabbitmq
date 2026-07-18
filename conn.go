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
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Conn is a self-healing RabbitMQ connection manager. It owns a single
// underlying AMQP connection, watches it for closes, and re-dials in the
// background with bounded exponential backoff. Publishers and consumers created
// from it survive reconnects transparently.
//
// A Conn is safe for concurrent use by multiple goroutines.
type Conn struct {
	url  string
	opts connOptions
	log  Logger

	// lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu         sync.RWMutex
	conn       wireConn
	ready      chan struct{} // closed while a live connection is available
	closed     bool
	reconnects atomic.Uint64
}

// Connect establishes the initial connection and starts the background monitor.
// It retries the first dial with the configured backoff, honouring ctx: if ctx is
// cancelled before a connection is established (or MaxRetries is exhausted) it
// returns the last error. The supplied ctx also governs the lifetime of the
// background reconnect loop; cancel it (or call Close) to shut the manager down.
func Connect(ctx context.Context, url string, opts ...Option) (*Conn, error) {
	o := defaultConnOptions()
	for _, opt := range opts {
		opt(&o)
	}
	o.backoff = o.backoff.normalize()

	mctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		url:    url,
		opts:   o,
		log:    o.logger,
		ctx:    mctx,
		cancel: cancel,
		ready:  make(chan struct{}),
	}

	conn, err := c.dialWithRetry(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	c.setConn(conn)
	c.opts.observer.OnConnect()
	c.log.Infof("rabbitmq: connected to %s", safeURL(url))

	c.wg.Add(1)
	go c.monitor(conn)
	return c, nil
}

// dialWithRetry attempts to dial until success, ctx cancellation, or the backoff
// retry budget is exhausted.
func (c *Conn) dialWithRetry(ctx context.Context) (wireConn, error) {
	var lastErr error
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			c.opts.observer.OnReconnect(attempt)
		}
		conn, err := c.opts.dial(c.url, c.opts.amqpConfig())
		if err == nil {
			return conn, nil
		}
		lastErr = err
		c.log.Warnf("rabbitmq: dial attempt %d failed: %v", attempt+1, err)
		if c.opts.backoff.stop(attempt + 1) {
			return nil, lastErr
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.ctx.Done():
			return nil, ErrClosed
		case <-time.After(c.opts.backoff.delay(attempt)):
		}
	}
}

// monitor waits for the current connection to close, then re-dials and repeats.
func (c *Conn) monitor(conn wireConn) {
	defer c.wg.Done()
	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	for {
		var amqpErr *amqp.Error
		select {
		case <-c.ctx.Done():
			return
		case amqpErr = <-closeCh:
		}

		c.clearConn()
		var cerr error
		if amqpErr != nil {
			cerr = amqpErr
		}
		c.opts.observer.OnDisconnect(cerr)
		if c.isClosed() {
			return
		}
		c.log.Warnf("rabbitmq: connection lost: %v; reconnecting", cerr)

		next, err := c.dialWithRetry(c.ctx)
		if err != nil {
			c.log.Errorf("rabbitmq: reconnect abandoned: %v", err)
			return
		}
		c.reconnects.Add(1)
		c.setConn(next)
		c.opts.observer.OnConnect()
		c.log.Infof("rabbitmq: reconnected to %s (reconnect #%d)", safeURL(c.url), c.reconnects.Load())
		closeCh = next.NotifyClose(make(chan *amqp.Error, 1))
	}
}

// setConn installs a live connection and signals readiness.
func (c *Conn) setConn(conn wireConn) {
	c.mu.Lock()
	c.conn = conn
	select {
	case <-c.ready:
		// already closed (already signalled ready); nothing to do.
	default:
		close(c.ready)
	}
	c.mu.Unlock()
}

// clearConn marks the manager as having no live connection and resets readiness.
func (c *Conn) clearConn() {
	c.mu.Lock()
	c.conn = nil
	select {
	case <-c.ready:
		// was closed/ready; open a fresh gate for the next connection.
		c.ready = make(chan struct{})
	default:
		// already not ready.
	}
	c.mu.Unlock()
}

// current returns the live connection and the readiness gate under a read lock.
func (c *Conn) current() (wireConn, chan struct{}) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.conn, c.ready
}

// waitReady blocks until a live connection is available or ctx expires. It
// returns the live connection.
func (c *Conn) waitReady(ctx context.Context) (wireConn, error) {
	for {
		if c.isClosed() {
			return nil, ErrClosed
		}
		conn, ready := c.current()
		if conn != nil && !conn.IsClosed() {
			return conn, nil
		}
		select {
		case <-ctx.Done():
			return nil, ErrNotReady
		case <-c.ctx.Done():
			return nil, ErrClosed
		case <-ready:
			// readiness signalled; re-read the connection on the next iteration.
		}
	}
}

// openChannel obtains a live connection (waiting if necessary) and opens a fresh
// channel on it.
func (c *Conn) openChannel(ctx context.Context) (wireChannel, error) {
	conn, err := c.waitReady(ctx)
	if err != nil {
		return nil, err
	}
	return conn.Channel()
}

// Healthy reports whether a live, open connection is currently available.
func (c *Conn) Healthy() bool {
	if c.isClosed() {
		return false
	}
	conn, _ := c.current()
	return conn != nil && !conn.IsClosed()
}

// Reconnects returns the number of successful reconnections since Connect.
func (c *Conn) Reconnects() uint64 { return c.reconnects.Load() }

// isClosed reports whether Close has been called.
func (c *Conn) isClosed() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.closed
}

// Close shuts down the background monitor and closes the underlying connection.
// It is idempotent and safe to call concurrently.
func (c *Conn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	c.cancel()
	var err error
	if conn != nil {
		err = conn.Close()
	}
	c.wg.Wait()
	c.log.Infof("rabbitmq: connection manager closed")
	return err
}
