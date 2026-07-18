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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// publisherOptions is the resolved construction config for a Publisher.
type publisherOptions struct {
	declare     *ExchangeConfig
	contentType string
	persistent  bool
	mandatory   bool
	maxRetries  int
}

// PublisherOption configures a Publisher at construction time.
type PublisherOption func(*publisherOptions)

// WithExchangeDeclare makes the publisher declare its exchange (once per channel,
// and again after every reconnect) before publishing. Without it the exchange is
// assumed to already exist.
func WithExchangeDeclare(cfg ExchangeConfig) PublisherOption {
	return func(o *publisherOptions) { c := cfg; o.declare = &c }
}

// WithDefaultContentType sets the Content-Type stamped on messages that do not
// override it. The default is application/json.
func WithDefaultContentType(ct string) PublisherOption {
	return func(o *publisherOptions) { o.contentType = ct }
}

// WithPersistentDefault sets whether messages are persistent by default. The
// default is true (delivery mode 2).
func WithPersistentDefault(persistent bool) PublisherOption {
	return func(o *publisherOptions) { o.persistent = persistent }
}

// WithMandatoryDefault sets the default for the AMQP mandatory flag. The default
// is false.
func WithMandatoryDefault(mandatory bool) PublisherOption {
	return func(o *publisherOptions) { o.mandatory = mandatory }
}

// WithPublishRetries bounds how many times a single Publish re-opens the channel
// and retries after a failure before returning ErrPublishFailed. The default is
// 3. A value <=0 means "one attempt, no retry".
func WithPublishRetries(n int) PublisherOption {
	return func(o *publisherOptions) { o.maxRetries = n }
}

// Publisher publishes to one exchange over a Conn. It lazily opens a channel,
// re-opens (and optionally re-declares its exchange) after a drop, and retries a
// failed publish within bounded backoff. A Publisher is safe for concurrent use.
type Publisher struct {
	conn     *Conn
	exchange string
	opts     publisherOptions

	mu sync.Mutex
	ch wireChannel
}

// NewPublisher creates a Publisher for the given exchange. An empty exchange name
// targets the default exchange, where the routing key is the destination queue
// name.
func (c *Conn) NewPublisher(exchange string, opts ...PublisherOption) *Publisher {
	o := publisherOptions{
		contentType: "application/json",
		persistent:  true,
		maxRetries:  3,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &Publisher{conn: c, exchange: exchange, opts: o}
}

// PublishOption customises a single Publish call.
type PublishOption func(*amqp.Publishing)

// WithContentType overrides the message Content-Type.
func WithContentType(ct string) PublishOption {
	return func(p *amqp.Publishing) { p.ContentType = ct }
}

// WithHeaders sets AMQP headers on the message.
func WithHeaders(h amqp.Table) PublishOption {
	return func(p *amqp.Publishing) { p.Headers = h }
}

// WithPersistent overrides the persistence of a single message.
func WithPersistent(persistent bool) PublishOption {
	return func(p *amqp.Publishing) {
		if persistent {
			p.DeliveryMode = amqp.Persistent
		} else {
			p.DeliveryMode = amqp.Transient
		}
	}
}

// WithMessageID sets the message id.
func WithMessageID(id string) PublishOption {
	return func(p *amqp.Publishing) { p.MessageId = id }
}

// WithCorrelationID sets the correlation id.
func WithCorrelationID(id string) PublishOption {
	return func(p *amqp.Publishing) { p.CorrelationId = id }
}

// WithReplyTo sets the reply-to address.
func WithReplyTo(queue string) PublishOption {
	return func(p *amqp.Publishing) { p.ReplyTo = queue }
}

// WithExpiration sets a per-message TTL, expressed as a millisecond string per
// the AMQP spec (for example "60000").
func WithExpiration(ms string) PublishOption {
	return func(p *amqp.Publishing) { p.Expiration = ms }
}

// WithPriority sets the message priority (0-9 on a priority queue).
func WithPriority(priority uint8) PublishOption {
	return func(p *amqp.Publishing) { p.Priority = priority }
}

// WithType sets the message type name.
func WithType(t string) PublishOption {
	return func(p *amqp.Publishing) { p.Type = t }
}

// WithAppID sets the publishing application id.
func WithAppID(id string) PublishOption {
	return func(p *amqp.Publishing) { p.AppId = id }
}

// channel returns a live channel, opening (and re-declaring, if configured) a new
// one when necessary.
func (p *Publisher) channel(ctx context.Context) (wireChannel, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		return p.ch, nil
	}
	ch, err := p.conn.openChannel(ctx)
	if err != nil {
		return nil, err
	}
	if p.opts.declare != nil {
		if err := declareExchangeOn(ch, *p.opts.declare); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("declare exchange %q: %w", p.exchange, err)
		}
	}
	p.ch = ch
	return ch, nil
}

// resetChannel drops the cached channel so the next publish opens a fresh one.
func (p *Publisher) resetChannel() {
	p.mu.Lock()
	ch := p.ch
	p.ch = nil
	p.mu.Unlock()
	if ch != nil {
		_ = ch.Close()
	}
}

// Publish sends body to the publisher's exchange with the given routing key. It
// ensures a live channel, re-opening and re-declaring on a drop, and retries
// within the configured bounded backoff. It returns an error (wrapping
// ErrPublishFailed) only after the retry budget is exhausted, so an outbox worker
// can keep the message and try again later.
//
// Any publish interceptors registered on the Conn (for example the OTel adapter's
// tracing interceptor) wrap the whole call: they run once per Publish, may mutate
// the message headers before it is sent, and observe the final outcome.
func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte, opts ...PublishOption) error {
	msg := amqp.Publishing{
		ContentType: p.opts.contentType,
		Body:        body,
		Timestamp:   time.Now(),
	}
	if p.opts.persistent {
		msg.DeliveryMode = amqp.Persistent
	}
	for _, opt := range opts {
		opt(&msg)
	}

	// The innermost function performs the bounded-retry send; interceptors wrap
	// it so a single span/metric covers the logical publish, not each retry.
	send := func(ctx context.Context, exchange, key string, m *amqp.Publishing) error {
		return p.publishWithRetry(ctx, exchange, key, m)
	}
	for i := len(p.conn.opts.publishInterceptors) - 1; i >= 0; i-- {
		ic := p.conn.opts.publishInterceptors[i]
		next := send
		send = func(ctx context.Context, exchange, key string, m *amqp.Publishing) error {
			return ic(ctx, exchange, key, m, next)
		}
	}
	return send(ctx, p.exchange, routingKey, &msg)
}

// publishWithRetry ensures a live channel and sends msg, retrying within bounded
// backoff before returning ErrPublishFailed.
func (p *Publisher) publishWithRetry(ctx context.Context, exchange, routingKey string, msg *amqp.Publishing) error {
	backoff := p.conn.opts.backoff
	var lastErr error
	for attempt := 0; attempt <= p.opts.maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: %w", ErrPublishFailed, ctx.Err())
			case <-time.After(backoff.delay(attempt - 1)):
			}
		}
		ch, err := p.channel(ctx)
		if err != nil {
			lastErr = err
			continue
		}
		err = ch.PublishWithContext(ctx, exchange, routingKey, p.opts.mandatory, false, *msg)
		if err == nil {
			p.conn.opts.observer.OnPublish(exchange, routingKey, nil)
			return nil
		}
		lastErr = err
		p.conn.opts.observer.OnPublish(exchange, routingKey, err)
		p.conn.log.Warnf("rabbitmq: publish to %q/%q failed (attempt %d): %v", exchange, routingKey, attempt+1, err)
		p.resetChannel()
	}
	return fmt.Errorf("%w: %w", ErrPublishFailed, lastErr)
}

// PublishJSON marshals v to JSON and publishes it with Content-Type
// application/json (regardless of the configured default).
func (p *Publisher) PublishJSON(ctx context.Context, routingKey string, v any, opts ...PublishOption) error {
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("rabbitmq: marshal publish payload: %w", err)
	}
	opts = append([]PublishOption{WithContentType("application/json")}, opts...)
	return p.Publish(ctx, routingKey, body, opts...)
}

// Close releases the publisher's channel. The underlying Conn is left open.
func (p *Publisher) Close() error {
	p.resetChannel()
	return nil
}
