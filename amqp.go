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

	amqp "github.com/rabbitmq/amqp091-go"
)

// The interfaces below are the seam over amqp091-go. Every network operation the
// library performs goes through wireConn / wireChannel, and every dial goes
// through a dialFunc. Production code uses the real amqp091 types via realConn;
// tests substitute in-memory fakes so the reconnect state machine, publish retry
// loop and consumer loop can be exercised without a live broker.

// wireConn is the subset of *amqp.Connection the library uses.
type wireConn interface {
	Channel() (wireChannel, error)
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	IsClosed() bool
	Close() error
}

// wireChannel is the subset of *amqp.Channel the library uses. Every method but
// publishDeferred has the concrete channel's signature; realChannel adds
// publishDeferred so the confirm path can be faked (amqp.DeferredConfirmation
// cannot be constructed outside amqp091).
type wireChannel interface {
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	PublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	NotifyClose(receiver chan *amqp.Error) chan *amqp.Error
	Close() error
	IsClosed() bool

	// Confirm puts the channel in publisher-confirm mode.
	Confirm(noWait bool) error
	// publishDeferred publishes and returns the pending broker confirmation. It
	// returns a nil confirmation when the channel is not in confirm mode.
	publishDeferred(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error)
}

// confirmation is a pending publisher confirm. *amqp.DeferredConfirmation
// satisfies it. WaitContext returns (true, nil) for an ack and (false, nil) for a
// nack or for a channel that closed before the broker answered.
type confirmation interface {
	WaitContext(ctx context.Context) (bool, error)
}

// realChannel adapts *amqp.Channel to wireChannel.
type realChannel struct {
	*amqp.Channel
}

// publishDeferred publishes with a deferred confirm. The nil check keeps a nil
// *amqp.DeferredConfirmation from turning into a non-nil interface value.
func (c realChannel) publishDeferred(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) (confirmation, error) {
	dc, err := c.PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
	if err != nil || dc == nil {
		return nil, err
	}
	return dc, nil
}

// dialFunc establishes a connection. The production implementation dials with
// amqp091; tests supply a fake.
type dialFunc func(url string, cfg amqp.Config) (wireConn, error)

// realConn adapts *amqp.Connection to wireConn. The only reason it exists is that
// *amqp.Connection.Channel returns the concrete *amqp.Channel rather than
// wireChannel.
type realConn struct {
	*amqp.Connection
}

// Channel opens a channel and returns it as a wireChannel.
func (c realConn) Channel() (wireChannel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return realChannel{ch}, nil
}

// defaultDial is the production dialFunc.
func defaultDial(url string, cfg amqp.Config) (wireConn, error) {
	conn, err := amqp.DialConfig(url, cfg)
	if err != nil {
		return nil, err
	}
	return realConn{conn}, nil
}
