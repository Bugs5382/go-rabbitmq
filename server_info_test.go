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

func TestServerVersionAfterConnect(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{serverProps: []amqp.Table{{"product": "RabbitMQ", "version": "4.1.4"}}}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if got := conn.ServerVersion(); got != "4.1.4" {
		t.Errorf("ServerVersion = %q, want 4.1.4", got)
	}
	if got := conn.ServerProperties()["product"]; got != "RabbitMQ" {
		t.Errorf("ServerProperties[product] = %v, want RabbitMQ", got)
	}
}

func TestServerVersionEmptyWhenBrokerSendsNone(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{serverProps: []amqp.Table{{"product": "RabbitMQ"}}}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if got := conn.ServerVersion(); got != "" {
		t.Errorf("ServerVersion = %q, want empty", got)
	}
}

func TestServerPropertiesIsACopy(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{serverProps: []amqp.Table{{"version": "4.1.4"}}}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial), WithBackoff(fastBackoff()))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	p := conn.ServerProperties()
	p["version"] = "tampered"
	if got := conn.ServerVersion(); got != "4.1.4" {
		t.Errorf("ServerVersion after mutating the copy = %q, want 4.1.4", got)
	}
}

func TestServerVersionKeptWhileReconnectingAndRefreshedAfter(t *testing.T) {
	t.Parallel()
	b := &fakeBroker{serverProps: []amqp.Table{{"version": "3.13.7"}, {"version": "4.1.4"}}}
	conn, err := Connect(context.Background(), "amqp://localhost", withDialer(b.dial),
		WithBackoff(Backoff{Initial: 50 * time.Millisecond, Max: 50 * time.Millisecond, Factor: 1}))
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	b.mu.Lock()
	b.dialErrs = []error{errors.New("broker down")}
	b.mu.Unlock()
	b.lastConn().dropWith(&amqp.Error{Code: 320, Reason: "connection forced"})

	waitFor(t, time.Second, func() bool { return !conn.Healthy() })
	if got := conn.ServerVersion(); got != "3.13.7" {
		t.Errorf("ServerVersion while reconnecting = %q, want the last known 3.13.7", got)
	}

	waitFor(t, 2*time.Second, func() bool { return conn.Reconnects() == 1 })
	if got := conn.ServerVersion(); got != "4.1.4" {
		t.Errorf("ServerVersion after reconnect = %q, want 4.1.4", got)
	}
}
