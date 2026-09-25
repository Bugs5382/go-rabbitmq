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
	"strings"
	"testing"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TestIntegrationTransientNonExclusiveQueue shows the RabbitMQ 4 rule from issue
// #12. A transient queue that is not exclusive is refused by a RabbitMQ 4 broker
// with default settings; the library reports that as ErrInvalidQueue with the
// fix in the message. The two shapes the message suggests are then declared on
// the same Conn and must succeed.
//
// A broker that accepts the shape (RabbitMQ 3, or RabbitMQ 4 with
// transient_nonexcl_queues permitted) skips the refusal check, since there is
// nothing to explain there.
func TestIntegrationTransientNonExclusiveQueue(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	// x-expires removes every queue here shortly after the test, whatever its shape.
	expires := amqp.Table{"x-expires": int32(60_000)}

	refused := rabbitmq.QueueConfig{Name: "it.transient." + suffix, AutoDelete: true, Args: expires}.Transient()
	_, err = conn.DeclareQueue(ctx, refused)
	accepted := err == nil
	if !accepted {
		if !errors.Is(err, rabbitmq.ErrInvalidQueue) {
			t.Fatalf("expected ErrInvalidQueue for a transient non-exclusive queue, got %v", err)
		}
		var amqpErr *amqp.Error
		if !errors.As(err, &amqpErr) {
			t.Errorf("the broker error should stay reachable with errors.As, got %v", err)
		}
		for _, want := range []string{"Exclusive", "Transient()", "AutoDelete"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not name the fix %q", err, want)
			}
		}
		t.Logf("broker refused the shape as expected: %v", err)
	}

	// The fixes the error names. The refusal may have closed the channel or the
	// connection, so these also prove the Conn recovers from it.
	if _, err := conn.DeclareQueue(ctx, rabbitmq.QueueConfig{
		Name: "it.transient.excl." + suffix, Exclusive: true, AutoDelete: true, Args: expires,
	}.Transient()); err != nil {
		t.Errorf("declare exclusive transient queue: %v", err)
	}
	if _, err := conn.DeclareQueue(ctx, rabbitmq.QueueConfig{
		Name: "it.transient.durable." + suffix, AutoDelete: true, Args: expires,
	}); err != nil {
		t.Errorf("declare durable auto-delete queue: %v", err)
	}
	t.Logf("reconnects after the refusal: %d", conn.Reconnects())

	if accepted {
		t.Skip("broker accepts transient non-exclusive queues (RabbitMQ 3, or transient_nonexcl_queues permitted)")
	}
}
