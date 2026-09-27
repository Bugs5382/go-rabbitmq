//go:build integration

package rabbitmq_test

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

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Integration tests for WithMandatory (issue #20).

// TestIntegrationMandatoryUnboundExchangeIsUnroutable publishes to an exchange
// with no bindings: the broker returns the message and Publish reports it.
// A publish on the same publisher to a bound key is still acked.
func TestIntegrationMandatoryUnboundExchangeIsUnroutable(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exchange := fmt.Sprintf("it.mandatory.%d", time.Now().UnixNano())
	ex := rabbitmq.ExchangeConfig{Name: exchange, Kind: "direct", AutoDelete: true}
	pub := conn.NewPublisher(exchange,
		rabbitmq.WithExchangeDeclare(ex),
		rabbitmq.WithMandatory(),
		rabbitmq.WithConfirmTimeout(5*time.Second),
	)
	defer func() { _ = pub.Close() }()

	err = pub.Publish(ctx, "nobody.home", []byte("lost"), rabbitmq.WithMessageID("m-unroutable"))
	if !errors.Is(err, rabbitmq.ErrUnroutable) {
		t.Fatalf("publish to an unbound exchange: want ErrUnroutable, got %v", err)
	}
	if !errors.Is(err, rabbitmq.ErrPublishFailed) {
		t.Errorf("want ErrPublishFailed too, got %v", err)
	}
	var ue *rabbitmq.UnroutableError
	if !errors.As(err, &ue) {
		t.Fatalf("want an *UnroutableError, got %T", err)
	}
	if ue.ReplyCode != amqp.NoRoute || ue.Exchange != exchange || ue.RoutingKey != "nobody.home" || ue.MessageID != "m-unroutable" {
		t.Errorf("UnroutableError = %+v", ue)
	}

	// Bind a queue: the same publisher now gets an ack for that key.
	q, err := conn.DeclareQueue(ctx, rabbitmq.QueueConfig{Exclusive: true, AutoDelete: true}.Transient())
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	if err := conn.BindQueue(ctx, rabbitmq.BindingConfig{Queue: q.Name, Exchange: exchange, RoutingKey: "home"}); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := pub.Publish(ctx, "home", []byte("routed")); err != nil {
		t.Fatalf("publish to a bound key: want nil, got %v", err)
	}
	if err := pub.Publish(ctx, "nobody.home", []byte("lost again")); !errors.Is(err, rabbitmq.ErrUnroutable) {
		t.Fatalf("unbound key after binding another: want ErrUnroutable, got %v", err)
	}
}

// TestIntegrationConfirmAfterReconnect closes the library's connection on the
// broker side. A publish issued right after the close, while the Conn is
// reconnecting, must end with a confirm on the new connection instead of
// hanging, and a later return is still reported.
func TestIntegrationConfirmAfterReconnect(t *testing.T) {
	url := brokerURL(t)
	mgmt := brokerMgmt(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	name, props := connName("it-confirm-reconnect")
	conn, err := rabbitmq.Connect(ctx, url,
		rabbitmq.WithClientProperties(props),
		rabbitmq.WithBackoff(rabbitmq.Backoff{Initial: 100 * time.Millisecond, Max: time.Second, Factor: 2}),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	queue := fmt.Sprintf("it.reconnect.%d", time.Now().UnixNano())
	if _, err := conn.DeclareQueue(ctx, rabbitmq.QueueConfig{
		Name: queue, Args: amqp.Table{"x-expires": int32(60_000)},
	}); err != nil {
		t.Fatalf("declare queue: %v", err)
	}
	pub := conn.NewPublisher("", rabbitmq.WithMandatory(), rabbitmq.WithPublishRetries(10),
		rabbitmq.WithConfirmTimeout(5*time.Second))
	defer func() { _ = pub.Close() }()

	if err := pub.Publish(ctx, queue, []byte("before")); err != nil {
		t.Fatalf("publish before the reconnect: %v", err)
	}

	before := conn.Reconnects()
	if n := mgmt.closeConnection(t, name); n != 1 {
		t.Fatalf("closed %d connections, want 1", n)
	}

	pubCtx, pubCancel := context.WithTimeout(ctx, 20*time.Second)
	defer pubCancel()
	start := time.Now()
	if err := pub.Publish(pubCtx, queue, []byte("during")); err != nil {
		t.Fatalf("publish across the reconnect: want a confirm, got %v", err)
	}
	t.Logf("publish across the reconnect confirmed after %s", time.Since(start))

	deadline := time.Now().Add(10 * time.Second)
	for conn.Reconnects() == before && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if conn.Reconnects() == before {
		t.Fatal("the Conn did not reconnect")
	}

	if err := pub.Publish(ctx, queue, []byte("after")); err != nil {
		t.Fatalf("publish after the reconnect: %v", err)
	}
	if err := pub.Publish(ctx, queue+".missing", []byte("x")); !errors.Is(err, rabbitmq.ErrUnroutable) {
		t.Fatalf("unroutable publish after the reconnect: want ErrUnroutable, got %v", err)
	}
}
