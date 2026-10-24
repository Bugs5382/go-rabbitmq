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

	// serverProps is a private copy of the server properties of the most
	// recent successful connection. It is kept while disconnected, so
	// ServerVersion reports the last broker seen until a reconnect replaces it
	// (issue #24).
	serverProps amqp.Table

	// abandoned is closed, and abandonErr set, once the background reconnect
	// gives up (Backoff.MaxRetries spent), so waiters fail instead of blocking
	// forever (issue #20).
	abandoned  chan struct{}
	abandonErr error

	// kept holds the topologies registered with KeepTopology. Each is declared
	// again on every new connection before it is handed out (issue #25).
	keptMu sync.Mutex
	kept   []*Topology
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
		url:       url,
		opts:      o,
		log:       o.logger,
		ctx:       mctx,
		cancel:    cancel,
		ready:     make(chan struct{}),
		abandoned: make(chan struct{}),
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
			if c.isClosed() || errors.Is(err, ErrClosed) {
				return
			}
			c.log.Errorf("rabbitmq: reconnect abandoned: %v", err)
			c.abandon(err)
			return
		}
		c.redeclareKept(next)
		c.reconnects.Add(1)
		c.setConn(next)
		c.opts.observer.OnConnect()
		c.log.Infof("rabbitmq: reconnected to %s (reconnect #%d)", safeURL(c.url), c.reconnects.Load())
		closeCh = next.NotifyClose(make(chan *amqp.Error, 1))
	}
}

// abandon records that the background reconnect gave up and wakes every
// waiter, which then fails with ErrReconnectAbandoned.
func (c *Conn) abandon(err error) {
	c.mu.Lock()
	c.abandonErr = err
	c.mu.Unlock()
	close(c.abandoned)
}

// abandonedErr returns the error for a call made after the reconnect gave up.
func (c *Conn) abandonedErr() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return fmt.Errorf("%w: %w: %v", ErrNotReady, ErrReconnectAbandoned, c.abandonErr)
}

// setConn installs a live connection, records its server properties and
// signals readiness.
func (c *Conn) setConn(conn wireConn) {
	props := copyTable(conn.serverProperties())
	c.mu.Lock()
	c.conn = conn
	c.serverProps = props
	select {
	case <-c.ready:
		// already closed (already signalled ready); nothing to do.
	default:
		close(c.ready)
	}
	c.mu.Unlock()
	c.log.Debugf("rabbitmq: broker %v version %q", props["product"], tableString(props, "version"))
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
		case <-c.abandoned:
			return nil, c.abandonedErr()
		default:
		}
		select {
		case <-ctx.Done():
			return nil, ErrNotReady
		case <-c.ctx.Done():
			return nil, ErrClosed
		case <-c.abandoned:
			return nil, c.abandonedErr()
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

// ServerVersion returns the broker version reported by the most recent
// successful connection (the "version" server property, for example "4.1.4"),
// or "" when the broker sent none. It never blocks and is safe while the
// connection is down or reconnecting: it keeps reporting the last broker it
// connected to until a reconnect replaces it. Pair it with Healthy for a
// health body that lists the dependency's state and version.
func (c *Conn) ServerVersion() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return tableString(c.serverProps, "version")
}

// ServerProperties returns a copy of the server properties reported by the
// most recent successful connection: product, version, platform, cluster_name,
// capabilities and so on. Like ServerVersion it never blocks and keeps the last
// known values while reconnecting. Changing the returned table does not affect
// the Conn.
func (c *Conn) ServerProperties() amqp.Table {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return copyTable(c.serverProps)
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
