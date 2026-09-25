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
	"testing"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// TestIntegrationConfirmsAckAndNack publishes in confirm mode to a queue that
// holds one message and rejects further publishes: the first publish is acked,
// the second is nacked by the broker.
func TestIntegrationConfirmsAckAndNack(t *testing.T) {
	url := brokerURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := rabbitmq.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	q, err := conn.DeclareQueue(ctx, rabbitmq.QueueConfig{
		Exclusive:  true,
		AutoDelete: true,
		Args: amqp.Table{
			"x-queue-type": "classic",
			"x-max-length": int32(1),
			"x-overflow":   "reject-publish",
		},
	}.Transient())
	if err != nil {
		t.Fatalf("declare queue: %v", err)
	}

	// The default exchange routes by queue name.
	pub := conn.NewPublisher("", rabbitmq.WithConfirms(), rabbitmq.WithConfirmTimeout(5*time.Second))
	defer func() { _ = pub.Close() }()

	if err := pub.Publish(ctx, q.Name, []byte("first")); err != nil {
		t.Fatalf("first publish: want ack, got %v", err)
	}
	err = pub.Publish(ctx, q.Name, []byte("second"))
	if !errors.Is(err, rabbitmq.ErrNacked) {
		t.Fatalf("second publish: want ErrNacked, got %v", err)
	}
	if !errors.Is(err, rabbitmq.ErrPublishFailed) {
		t.Errorf("second publish: want ErrPublishFailed too, got %v", err)
	}
}
