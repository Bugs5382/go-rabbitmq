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
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Delivery is a received message handed to a Handler. It is a value copy of the
// useful fields of an amqp091 delivery; settlement (ack, nack or reject) is
// handled by the consumer based on the Handler's returned error, so the Handler
// never acks directly.
type Delivery struct {
	Body            []byte
	RoutingKey      string
	Exchange        string
	ContentType     string
	ContentEncoding string
	Headers         amqp.Table
	DeliveryTag     uint64
	Redelivered     bool
	MessageID       string
	CorrelationID   string
	ReplyTo         string
	Type            string
	AppID           string
	Priority        uint8
	Timestamp       time.Time
}

func toDelivery(d amqp.Delivery) Delivery {
	return Delivery{
		Body:            d.Body,
		RoutingKey:      d.RoutingKey,
		Exchange:        d.Exchange,
		ContentType:     d.ContentType,
		ContentEncoding: d.ContentEncoding,
		Headers:         d.Headers,
		DeliveryTag:     d.DeliveryTag,
		Redelivered:     d.Redelivered,
		MessageID:       d.MessageId,
		CorrelationID:   d.CorrelationId,
		ReplyTo:         d.ReplyTo,
		Type:            d.Type,
		AppID:           d.AppId,
		Priority:        d.Priority,
		Timestamp:       d.Timestamp,
	}
}

// Handler processes a single Delivery. Its returned error decides how the
// message is settled:
//
//   - nil: ack.
//   - an error matching ErrDeadLetter: reject without requeue, so the broker
//     dead-letters it (or drops it if the queue has no dead-letter exchange).
//   - an error matching ErrRequeue: nack with requeue.
//   - any other error: nack, requeued or not per ConsumerConfig.RequeueOnError.
//
// Matching uses errors.Is, so the sentinels can be wrapped with context:
// fmt.Errorf("decode: %w", rabbitmq.ErrDeadLetter). ErrDeadLetter wins when an
// error matches both. A panic in a Handler is recovered and treated as a plain
// error. With AutoAck the broker has already settled the message and the error
// is only logged.
type Handler func(ctx context.Context, d Delivery) error

// ConsumerConfig describes what and how to consume. Its Exchange, Queue and
// Bindings are re-declared on every (re)connect so the consumer survives broker
// restarts.
type ConsumerConfig struct {
	// Queue is the queue to consume from. It is declared before consuming.
	Queue QueueConfig
	// Exchange, if Name is non-empty, is declared before the queue.
	Exchange ExchangeConfig
	// Bindings are declared after the queue. A binding with an empty Queue uses
	// the resolved queue name (useful for server-named queues).
	Bindings []BindingConfig
	// Prefetch is the QoS prefetch count (unacked messages in flight). Default 10.
	Prefetch int
	// ConsumerTag is the consumer identifier; empty lets the broker assign one.
	ConsumerTag string
	// AutoAck disables manual acknowledgement. Leave false (the default) for
	// at-least-once delivery driven by the Handler's error.
	AutoAck bool
	// RequeueOnError controls whether a plain Handler error requeues the message
	// (default true) or drops/dead-letters it (false). ErrRequeue and
	// ErrDeadLetter override it for a single message.
	RequeueOnError bool
	// Exclusive requests exclusive consumer access to the queue.
	Exclusive bool
	// Args are passed to the underlying Consume call.
	Args amqp.Table

	// requeueSet distinguishes an explicit RequeueOnError:false from the zero
	// value so the requeue default can be applied.
	requeueSet bool
}

// NoRequeue returns a copy of the config that drops (rather than requeues)
// messages a Handler rejects, so a poison message does not loop forever. Pair it
// with a dead-letter exchange to capture rejects.
func (c ConsumerConfig) NoRequeue() ConsumerConfig {
	c.RequeueOnError = false
	c.requeueSet = true
	return c
}

func (c ConsumerConfig) normalize() ConsumerConfig {
	if c.Prefetch <= 0 {
		c.Prefetch = 10
	}
	if !c.requeueSet {
		c.RequeueOnError = true
	}
	return c
}

// Consume runs a resilient consumer until ctx is cancelled. It (re-)declares the
// configured topology, sets QoS, and consumes, dispatching each delivery to
// handler. On any channel or connection drop it waits for the Conn to recover,
// then re-declares and resumes. It returns ctx.Err() when ctx is cancelled, or
// ErrClosed if the Conn is closed.
//
// Consume blocks; run it in its own goroutine. Deliveries are dispatched
// sequentially on the calling goroutine, so a handler that must run concurrently
// should fan out internally (respecting Prefetch for backpressure).
func (c *Conn) Consume(ctx context.Context, cfg ConsumerConfig, handler Handler) error {
	cfg = cfg.normalize()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := c.consumeSession(ctx, cfg, handler)
		switch {
		case err == nil:
			// session ended for a retryable reason (drop); loop and re-establish.
		case ctx.Err() != nil:
			return ctx.Err()
		case c.isClosed():
			return ErrClosed
		default:
			c.log.Warnf("rabbitmq: consumer session on %q ended: %v; re-establishing", cfg.Queue.Name, err)
		}
		// Small pause so a persistently failing session does not hot-loop; the
		// first backoff step is short.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return ErrClosed
		case <-time.After(c.opts.backoff.delay(0)):
		}
	}
}

// consumeSession runs one channel's lifetime: declare, consume, dispatch until
// the channel or connection drops (returns nil for a clean drop) or a setup step
// fails (returns the error).
func (c *Conn) consumeSession(ctx context.Context, cfg ConsumerConfig, handler Handler) error {
	ch, err := c.openChannel(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()

	queueName, err := c.setupConsumer(ch, cfg)
	if err != nil {
		return err
	}

	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("set qos: %w", err)
	}

	deliveries, err := ch.Consume(queueName, cfg.ConsumerTag, cfg.AutoAck, cfg.Exclusive, false, false, cfg.Args)
	if err != nil {
		return fmt.Errorf("consume %q: %w", queueName, err)
	}
	closeCh := ch.NotifyClose(make(chan *amqp.Error, 1))
	c.log.Infof("rabbitmq: consuming from %q (prefetch %d)", queueName, cfg.Prefetch)

	return c.dispatch(ctx, ch, cfg, queueName, handler, deliveries, closeCh)
}

// setupConsumer declares the exchange, queue and bindings for a consumer and
// returns the resolved queue name (important for server-named queues).
func (c *Conn) setupConsumer(ch wireChannel, cfg ConsumerConfig) (string, error) {
	if cfg.Exchange.Name != "" {
		if err := declareExchangeOn(ch, cfg.Exchange); err != nil {
			return "", fmt.Errorf("declare exchange %q: %w", cfg.Exchange.Name, err)
		}
	}
	q, err := declareQueueOn(ch, cfg.Queue)
	if err != nil {
		return "", fmt.Errorf("declare queue %q: %w", cfg.Queue.Name, err)
	}
	name := q.Name
	for _, b := range cfg.Bindings {
		queue := b.Queue
		if queue == "" {
			queue = name
		}
		if err := ch.QueueBind(queue, b.RoutingKey, b.Exchange, b.NoWait, b.Args); err != nil {
			return "", fmt.Errorf("bind %q to %q: %w", queue, b.Exchange, err)
		}
	}
	return name, nil
}

// dispatch pumps deliveries into the handler until the delivery stream ends, the
// channel closes, or ctx is cancelled.
func (c *Conn) dispatch(
	ctx context.Context,
	ch wireChannel,
	cfg ConsumerConfig,
	queueName string,
	handler Handler,
	deliveries <-chan amqp.Delivery,
	closeCh <-chan *amqp.Error,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.ctx.Done():
			return ErrClosed
		case err := <-closeCh:
			if err != nil {
				return fmt.Errorf("channel closed: %w", err)
			}
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil // delivery stream ended; caller re-establishes.
			}
			c.handleDelivery(ctx, ch, cfg, queueName, handler, d)
		}
	}
}

// handleDelivery invokes the handler (recovering panics) and settles the
// delivery on the channel it arrived on, as the handler's error directs. When
// AutoAck is set the broker has already acked, so it only invokes the handler
// and reports to the observer.
func (c *Conn) handleDelivery(
	ctx context.Context,
	ch wireChannel,
	cfg ConsumerConfig,
	queueName string,
	handler Handler,
	d amqp.Delivery,
) {
	del := toDelivery(d)
	wrapped := chainConsume(handler, c.opts.consumeInterceptors)
	herr := safeHandle(ctx, wrapped, del)
	c.opts.observer.OnConsume(queueName, del, herr)

	if cfg.AutoAck {
		if herr != nil {
			c.log.Errorf("rabbitmq: handler error on %q (auto-ack, message lost): %v", queueName, herr)
		}
		return
	}

	// A delivery tag only means something on the channel that issued it. If that
	// channel closed while the handler ran, the broker has already returned the
	// message to the queue and will redeliver it; settling now would at best
	// fail and at worst hit an unrelated delivery.
	if ch.IsClosed() {
		c.log.Warnf("rabbitmq: channel for %q closed during handling; delivery %d not settled, the broker will redeliver it",
			queueName, d.DeliveryTag)
		return
	}

	var err error
	switch {
	case herr == nil:
		err = ch.Ack(d.DeliveryTag, false)
	case errors.Is(herr, ErrDeadLetter):
		c.log.Warnf("rabbitmq: handler dead-lettered a message on %q: %v", queueName, herr)
		err = ch.Reject(d.DeliveryTag, false)
	case errors.Is(herr, ErrRequeue):
		c.log.Warnf("rabbitmq: handler requeued a message on %q: %v", queueName, herr)
		err = ch.Nack(d.DeliveryTag, false, true)
	default:
		c.log.Warnf("rabbitmq: handler error on %q (requeue=%t): %v", queueName, cfg.RequeueOnError, herr)
		err = ch.Nack(d.DeliveryTag, false, cfg.RequeueOnError)
	}
	if err != nil {
		c.log.Warnf("rabbitmq: settling delivery %d on %q failed: %v", d.DeliveryTag, queueName, err)
	}
}

// safeHandle runs a handler and converts a panic into an error.
func safeHandle(ctx context.Context, handler Handler, d Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("rabbitmq: handler panicked: %v", r)
		}
	}()
	return handler(ctx, d)
}
