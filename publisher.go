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
	"errors"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// DefaultConfirmTimeout is how long a publisher in confirm mode waits for the
// broker to confirm each publish attempt, unless WithConfirmTimeout overrides it.
const DefaultConfirmTimeout = 30 * time.Second

// publisherOptions is the resolved construction config for a Publisher.
type publisherOptions struct {
	declare        *ExchangeConfig
	contentType    string
	persistent     bool
	mandatory      bool
	maxRetries     int
	confirms       bool
	confirmTimeout time.Duration
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

// WithConfirms puts the publisher's channel in publisher-confirm mode, on the
// first publish and again on every channel re-opened after a drop or reconnect.
// Publish then returns only after the broker has confirmed the message:
//
//   - ack: Publish returns nil. The broker has taken responsibility for the
//     message (for a persistent message on a durable queue, it is on disk).
//   - nack: Publish returns an error matching ErrNacked. It is not retried.
//   - no answer within the confirm timeout (see WithConfirmTimeout): Publish
//     returns an error matching ErrConfirmTimeout. It is not retried.
//   - the channel or connection closes before the answer: the confirm is lost,
//     never treated as an ack. Publish re-publishes on a fresh channel within the
//     WithPublishRetries budget, so the message is delivered at least once and
//     may be duplicated. Once the budget is spent it returns an error matching
//     ErrConfirmLost.
//
// Every one of these errors also matches ErrPublishFailed, so the "keep the
// message and try again later" handling stays the same. Consumers of a
// confirmed stream should de-duplicate (for example on MessageID).
//
// A confirm does not mean the message was routed: an unroutable message is still
// acked. Confirms are off by default.
func WithConfirms() PublisherOption {
	return func(o *publisherOptions) { o.confirms = true }
}

// WithConfirmTimeout bounds how long each publish attempt waits for its broker
// confirm when WithConfirms is set. The default is DefaultConfirmTimeout. A value
// <=0 removes the bound, so only the Publish ctx limits the wait.
func WithConfirmTimeout(d time.Duration) PublisherOption {
	return func(o *publisherOptions) { o.confirmTimeout = d }
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
		contentType:    "application/json",
		persistent:     true,
		maxRetries:     3,
		confirmTimeout: DefaultConfirmTimeout,
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
	if p.opts.confirms {
		if err := ch.Confirm(false); err != nil {
			_ = ch.Close()
			return nil, fmt.Errorf("enable publisher confirms: %w", err)
		}
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
// When failed is non-nil the cache is only cleared if it still holds that
// channel, so a concurrent publish that already opened a replacement keeps it.
func (p *Publisher) resetChannel(failed wireChannel) {
	p.mu.Lock()
	ch := p.ch
	if failed != nil && ch != failed {
		p.mu.Unlock()
		_ = failed.Close()
		return
	}
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
// Without WithConfirms, a nil error means the message was written to the
// channel, not that the broker accepted it. With WithConfirms, a nil error means
// the broker acked it; see WithConfirms for how nacks, timeouts and reconnects
// are reported.
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
// backoff before returning ErrPublishFailed. A send failure or a confirm lost to a
// channel drop is retried; a nack, a confirm timeout or a cancelled ctx while
// waiting for a confirm ends the call at once.
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
		retry, err := p.send(ctx, ch, exchange, routingKey, msg)
		p.conn.opts.observer.OnPublish(exchange, routingKey, err)
		if err == nil {
			return nil
		}
		if !retry {
			p.conn.log.Warnf("rabbitmq: publish to %q/%q failed: %v", exchange, routingKey, err)
			return fmt.Errorf("%w: %w", ErrPublishFailed, err)
		}
		lastErr = err
		p.conn.log.Warnf("rabbitmq: publish to %q/%q failed (attempt %d): %v", exchange, routingKey, attempt+1, err)
		p.resetChannel(ch)
	}
	return fmt.Errorf("%w: %w", ErrPublishFailed, lastErr)
}

// send performs one publish attempt on ch and, in confirm mode, waits for the
// broker's answer. retry reports whether a failure should be retried on a fresh
// channel.
func (p *Publisher) send(ctx context.Context, ch wireChannel, exchange, routingKey string, msg *amqp.Publishing) (retry bool, err error) {
	if !p.opts.confirms {
		return true, ch.PublishWithContext(ctx, exchange, routingKey, p.opts.mandatory, false, *msg)
	}
	conf, err := ch.publishDeferred(ctx, exchange, routingKey, p.opts.mandatory, false, *msg)
	if err != nil {
		return true, err
	}
	if conf == nil {
		// The channel is not in confirm mode, which channel() rules out. Re-open
		// rather than report an unconfirmed publish as confirmed.
		return true, errors.New("rabbitmq: channel is not in confirm mode")
	}

	waitCtx := ctx
	if p.opts.confirmTimeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, p.opts.confirmTimeout)
		defer cancel()
	}
	acked, err := conf.WaitContext(waitCtx)
	switch {
	case err != nil && ctx.Err() != nil:
		return false, ctx.Err()
	case err != nil:
		return false, fmt.Errorf("%w after %s", ErrConfirmTimeout, p.opts.confirmTimeout)
	case acked:
		return false, nil
	case ch.IsClosed():
		// amqp091 marks the channel closed before it settles outstanding
		// confirms as not-acked, so this is a drop, not a nack.
		return true, ErrConfirmLost
	default:
		return false, ErrNacked
	}
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
	p.resetChannel(nil)
	return nil
}
