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

// fastBackoff is a tiny, deterministic backoff so reconnect tests run quickly.
func fastBackoff() Backoff {
	return Backoff{Initial: time.Millisecond, Max: 2 * time.Millisecond, Factor: 2, Jitter: 0}
}

// waitFor polls cond until it is true or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

func TestConnectDialsOnceAndIsHealthy(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://guest:guest@localhost:5672/",
		withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if !conn.Healthy() {
		t.Error("expected healthy connection after Connect")
	}
	if b.dialsSoFar() != 1 {
		t.Errorf("dials = %d, want 1", b.dialsSoFar())
	}
}

func TestConnectRetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{dialErrs: []error{errors.New("down"), errors.New("still down"), nil}}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect should have recovered: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if b.dialsSoFar() != 3 {
		t.Errorf("dials = %d, want 3 (2 failures + 1 success)", b.dialsSoFar())
	}
}

func TestConnectRespectsMaxRetries(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{dialErr: errors.New("always down")}
	bo := fastBackoff()
	bo.MaxRetries = 3
	_, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(bo))
	if err == nil {
		t.Fatal("expected Connect to fail after MaxRetries")
	}
	if b.dialsSoFar() != 3 {
		t.Errorf("dials = %d, want 3 (bounded by MaxRetries)", b.dialsSoFar())
	}
}

func TestConnectHonoursContextCancellation(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{dialErr: errors.New("always down")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := Connect(ctx, "amqp://localhost", withDialer(b.dial),
		WithBackoff(Backoff{Initial: 5 * time.Millisecond, Max: 5 * time.Millisecond, Jitter: 0}))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected context deadline error, got %v", err)
	}
}

func TestConnReconnectsAfterDrop(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	first := b.lastConn()
	first.dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})

	waitFor(t, time.Second, func() bool { return conn.Reconnects() == 1 })
	waitFor(t, time.Second, conn.Healthy)
	if b.dialsSoFar() != 2 {
		t.Errorf("dials = %d, want 2 after one drop", b.dialsSoFar())
	}
}

func TestConnSurvivesRepeatedDrops(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	for i := uint64(1); i <= 3; i++ {
		b.lastConn().dropWith(&amqp.Error{Code: 501, Reason: "boom"})
		waitFor(t, time.Second, func() bool { return conn.Reconnects() == i })
	}
	if !conn.Healthy() {
		t.Error("connection should be healthy after surviving repeated drops")
	}
}

func TestCloseIsIdempotentAndStopsHealth(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close should be a no-op: %v", err)
	}
	if conn.Healthy() {
		t.Error("closed connection must not report healthy")
	}
}

func TestWaitReadyFailsFastWhenClosed(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	_ = conn.Close()
	_, err = conn.waitReady(context.Background())
	if !errors.Is(err, ErrClosed) {
		t.Errorf("waitReady on closed conn = %v, want ErrClosed", err)
	}
}

func TestWaitReadyHonoursContext(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Drop and keep the broker down so no connection is available.
	b.mu.Lock()
	b.dialErr = errors.New("down")
	b.mu.Unlock()
	b.lastConn().dropWith(&amqp.Error{Code: 320, Reason: "forced"})
	waitFor(t, time.Second, func() bool { return !conn.Healthy() })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = conn.waitReady(ctx)
	if !errors.Is(err, ErrNotReady) {
		t.Errorf("waitReady with expired ctx = %v, want ErrNotReady", err)
	}
}
