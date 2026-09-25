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

import "errors"

var (
	// ErrClosed is returned by operations on a Conn after Close has been called.
	ErrClosed = errors.New("rabbitmq: connection manager is closed")

	// ErrNotReady is returned when a live connection could not be obtained before
	// the supplied context expired.
	ErrNotReady = errors.New("rabbitmq: no live connection available")

	// ErrPublishFailed wraps the last publish error after all bounded retries were
	// exhausted. Callers (for example an outbox worker) should treat it as
	// retryable and try again later.
	ErrPublishFailed = errors.New("rabbitmq: publish failed after retries")

	// ErrNacked is returned (wrapped with ErrPublishFailed) when a publisher in
	// confirm mode receives a broker nack. The broker did not take the message;
	// keep it and try again later. A nack is not retried inside Publish.
	ErrNacked = errors.New("rabbitmq: broker nacked the message")

	// ErrConfirmTimeout is returned (wrapped with ErrPublishFailed) when the
	// broker does not confirm a publish within the publisher's confirm timeout.
	// The outcome is unknown: the broker may still have the message, so a retry
	// can produce a duplicate. It is not retried inside Publish.
	ErrConfirmTimeout = errors.New("rabbitmq: timed out waiting for publisher confirm")

	// ErrConfirmLost is returned (wrapped with ErrPublishFailed) when the channel
	// or connection closed while confirms were outstanding and every retry was
	// spent. The outcome of the last attempt is unknown, as with
	// ErrConfirmTimeout.
	ErrConfirmLost = errors.New("rabbitmq: channel closed before the publish was confirmed")

	// ErrRequeue, returned (or wrapped) by a Handler, negatively acknowledges the
	// message with requeue=true, whatever ConsumerConfig.RequeueOnError says. Use
	// it for a transient failure the message should be retried for, for example
	// fmt.Errorf("store unavailable: %w", rabbitmq.ErrRequeue).
	ErrRequeue = errors.New("rabbitmq: requeue message")

	// ErrDeadLetter, returned (or wrapped) by a Handler, rejects the message
	// without requeue, whatever ConsumerConfig.RequeueOnError says. The broker
	// dead-letters it to the queue's x-dead-letter-exchange if one is set and
	// drops it otherwise. Use it for a poison message that will never succeed.
	// If an error matches both ErrDeadLetter and ErrRequeue, ErrDeadLetter wins.
	ErrDeadLetter = errors.New("rabbitmq: dead-letter message")

	// ErrInvalidQueue is returned when a QueueConfig violates a broker rule, most
	// commonly a quorum queue that is also exclusive, auto-delete or server-named.
	// Those are caught before the declare. It is also returned when a RabbitMQ 4
	// broker refuses a transient queue that is not exclusive; that error wraps
	// the broker's *amqp.Error as well, and its message names the fix.
	ErrInvalidQueue = errors.New("rabbitmq: invalid queue configuration")
)
