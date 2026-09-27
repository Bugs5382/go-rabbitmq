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
	"errors"
	"fmt"
)

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

	// ErrConsumerRunning is returned by Consumer.Run when the same Consumer is
	// already running. A Consumer can be run again once Run has returned.
	ErrConsumerRunning = errors.New("rabbitmq: consumer is already running")

	// ErrInvalidConsumer is returned by Consumer.Run (and Conn.Consume) when a
	// ConsumerConfig can never work, for example AutoAck on a stream queue, a
	// StreamOffset on a queue that is not a stream, or a negative offset. Run
	// returns it at once, before touching the broker, instead of retrying.
	ErrInvalidConsumer = errors.New("rabbitmq: invalid consumer configuration")

	// ErrUnroutable is matched (with ErrPublishFailed) by the error Publish
	// returns when a publisher created WithMandatory has its message returned by
	// the broker because no queue was bound to receive it. The error is an
	// *UnroutableError carrying the broker's reply code and text. It is not
	// retried inside Publish: the message went nowhere, and it will keep going
	// nowhere until a binding exists.
	ErrUnroutable = errors.New("rabbitmq: message was returned as unroutable")

	// ErrReconnectAbandoned is matched (with ErrNotReady) by the error of any
	// call that needs a connection after the background reconnect gave up
	// because Backoff.MaxRetries was spent. The Conn stays unusable; close it
	// and connect again. With the default MaxRetries of 0 the Conn never gives
	// up, so this is never returned.
	ErrReconnectAbandoned = errors.New("rabbitmq: reconnect abandoned after MaxRetries")
)

// UnroutableError describes a mandatory publish the broker returned. It matches
// ErrUnroutable with errors.Is; use errors.As to read the details.
type UnroutableError struct {
	// ReplyCode is the AMQP reply code of the basic.return, 312 (NO_ROUTE) when
	// no binding matched.
	ReplyCode uint16
	// ReplyText is the broker's reason, for example "NO_ROUTE".
	ReplyText string
	// Exchange and RoutingKey are where the message was published.
	Exchange   string
	RoutingKey string
	// MessageID is the message id of the returned message, if it had one.
	MessageID string
}

// Error describes the return.
func (e *UnroutableError) Error() string {
	return fmt.Sprintf("rabbitmq: message to exchange %q with routing key %q was returned: %d %s",
		e.Exchange, e.RoutingKey, e.ReplyCode, e.ReplyText)
}

// Is reports whether target is ErrUnroutable.
func (e *UnroutableError) Is(target error) bool { return target == ErrUnroutable }
