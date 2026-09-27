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
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeBroker is an in-memory stand-in for a RabbitMQ server used to exercise the
// reconnect state machine, publish retry loop and consumer loop without a live
// broker. It is safe for concurrent use.
type fakeBroker struct {
	mu sync.Mutex

	dialErr   error // returned by dial when non-nil
	dialErrs  []error
	dialCount int
	conns     []*fakeConn
}

// dial implements dialFunc.
func (b *fakeBroker) dial(_ string, _ amqp.Config) (wireConn, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.dialCount++
	if len(b.dialErrs) > 0 {
		err := b.dialErrs[0]
		b.dialErrs = b.dialErrs[1:]
		if err != nil {
			return nil, err
		}
	} else if b.dialErr != nil {
		return nil, b.dialErr
	}
	c := newFakeConn()
	b.conns = append(b.conns, c)
	return c, nil
}

func (b *fakeBroker) dialsSoFar() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dialCount
}

func (b *fakeBroker) lastConn() *fakeConn {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.conns) == 0 {
		return nil
	}
	return b.conns[len(b.conns)-1]
}

// fakeConn implements wireConn.
type fakeConn struct {
	mu           sync.Mutex
	closed       bool
	closeNotify  []chan *amqp.Error
	channels     []*fakeChannel
	channelErr   error
	chanTemplate func(idx int, ch *fakeChannel) // configures each channel by open order
}

func newFakeConn() *fakeConn { return &fakeConn{} }

// setChanTemplate installs a hook run against each newly opened channel; idx is
// the zero-based open order.
func (c *fakeConn) setChanTemplate(fn func(idx int, ch *fakeChannel)) {
	c.mu.Lock()
	c.chanTemplate = fn
	c.mu.Unlock()
}

func (c *fakeConn) Channel() (wireChannel, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.channelErr != nil {
		return nil, c.channelErr
	}
	if c.closed {
		return nil, errors.New("connection closed")
	}
	idx := len(c.channels)
	ch := newFakeChannel()
	if c.chanTemplate != nil {
		c.chanTemplate(idx, ch)
	}
	c.channels = append(c.channels, ch)
	return ch, nil
}

// channelAt returns the channel opened at the given index, or nil.
func (c *fakeConn) channelAt(i int) *fakeChannel {
	c.mu.Lock()
	defer c.mu.Unlock()
	if i < 0 || i >= len(c.channels) {
		return nil
	}
	return c.channels[i]
}

// channelCount returns how many channels have been opened.
func (c *fakeConn) channelCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.channels)
}

func (c *fakeConn) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		close(receiver)
		return receiver
	}
	c.closeNotify = append(c.closeNotify, receiver)
	return receiver
}

func (c *fakeConn) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (c *fakeConn) Close() error {
	c.dropWith(nil)
	return nil
}

// dropWith simulates the broker/transport closing the connection, notifying all
// registered connection listeners and, as a real broker does, closing every
// channel opened on the connection.
func (c *fakeConn) dropWith(err *amqp.Error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	listeners := c.closeNotify
	c.closeNotify = nil
	channels := append([]*fakeChannel(nil), c.channels...)
	c.mu.Unlock()
	for _, ch := range channels {
		_ = ch.Close()
	}
	for _, l := range listeners {
		if err != nil {
			l <- err
		}
		close(l)
	}
}

// fakeChannel implements wireChannel.
type fakeChannel struct {
	mu sync.Mutex

	closed      bool
	closeNotify []chan *amqp.Error

	exchanges []ExchangeDeclareArgs
	queues    []QueueDeclareArgs
	binds     []BindArgs
	published []amqp.Publishing
	pubKeys   []string
	acked     []uint64
	nacked    []nackRecord
	rejected  []rejectRecord
	qos       *qosArgs

	publishErr   error
	publishErrs  []error
	consumeErr   error
	declareErr   error
	bindErr      error // fails QueueBind only
	deliveries   chan amqp.Delivery
	consumeCount int
	consumeArgs  []amqp.Table // args of each Consume call
	autoAcks     []bool       // autoAck of each Consume call

	// mandatory publishes and returns
	pubMandatory  []bool
	returnNotify  []chan amqp.Return
	returnPayload amqp.Return     // template for scripted returns (reply code/text)
	returnKeys    map[string]bool // routing keys the broker returns (then acks)

	// confirm mode
	confirmMode  bool
	confirmCalls int
	confirmErr   error
	confirmPlan  []confirmOutcome // outcome per confirmed publish; empty means ack
	pending      []*fakeConfirm   // unresolved confirms, settled false on Close
}

// confirmOutcome scripts how the fake broker answers one confirmed publish.
type confirmOutcome int

const (
	confirmAck    confirmOutcome = iota // broker acks
	confirmNack                         // broker nacks
	confirmNever                        // no answer until the channel closes
	confirmDrop                         // the channel closes before the answer arrives
	confirmReturn                       // the broker returns the message, then acks it
)

// fakeConfirm implements confirmation.
type fakeConfirm struct {
	once sync.Once
	done chan struct{}
	ack  bool
}

func newFakeConfirm() *fakeConfirm { return &fakeConfirm{done: make(chan struct{})} }

func (c *fakeConfirm) resolve(ack bool) {
	c.once.Do(func() { c.ack = ack; close(c.done) })
}

func (c *fakeConfirm) WaitContext(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-c.done:
		return c.ack, nil
	}
}

func newFakeChannel() *fakeChannel {
	return &fakeChannel{deliveries: make(chan amqp.Delivery, 16)}
}

// ExchangeDeclareArgs records an ExchangeDeclare call.
type ExchangeDeclareArgs struct {
	Name, Kind                         string
	Durable, AutoDelete, Internal, NoW bool
	Args                               amqp.Table
}

// QueueDeclareArgs records a QueueDeclare call.
type QueueDeclareArgs struct {
	Name                                string
	Durable, AutoDelete, Exclusive, NoW bool
	Args                                amqp.Table
}

// BindArgs records a QueueBind call.
type BindArgs struct {
	Name, Key, Exchange string
	Args                amqp.Table
}

type qosArgs struct {
	prefetchCount, prefetchSize int
	global                      bool
}

type nackRecord struct {
	tag               uint64
	multiple, requeue bool
}

type rejectRecord struct {
	tag     uint64
	requeue bool
}

func (ch *fakeChannel) ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.declareErr != nil {
		return ch.declareErr
	}
	ch.exchanges = append(ch.exchanges, ExchangeDeclareArgs{name, kind, durable, autoDelete, internal, noWait, args})
	return nil
}

func (ch *fakeChannel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.declareErr != nil {
		return amqp.Queue{}, ch.declareErr
	}
	ch.queues = append(ch.queues, QueueDeclareArgs{name, durable, autoDelete, exclusive, noWait, args})
	resolved := name
	if resolved == "" {
		resolved = "amq.gen-server-named"
	}
	return amqp.Queue{Name: resolved}, nil
}

func (ch *fakeChannel) QueueBind(name, key, exchange string, _ bool, args amqp.Table) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.declareErr != nil {
		return ch.declareErr
	}
	if ch.bindErr != nil {
		return ch.bindErr
	}
	ch.binds = append(ch.binds, BindArgs{name, key, exchange, args})
	return nil
}

func (ch *fakeChannel) PublishWithContext(_ context.Context, exchange, key string, _, _ bool, msg amqp.Publishing) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.recordPublishLocked(exchange, key, msg)
}

// recordPublishLocked applies the scripted publish errors and records a
// successful publish. ch.mu must be held.
func (ch *fakeChannel) recordPublishLocked(exchange, key string, msg amqp.Publishing) error {
	if ch.closed {
		return amqp.ErrClosed
	}
	if len(ch.publishErrs) > 0 {
		err := ch.publishErrs[0]
		ch.publishErrs = ch.publishErrs[1:]
		if err != nil {
			return err
		}
	} else if ch.publishErr != nil {
		return ch.publishErr
	}
	_ = exchange
	ch.published = append(ch.published, msg)
	ch.pubKeys = append(ch.pubKeys, key)
	return nil
}

func (ch *fakeChannel) Confirm(_ bool) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.confirmCalls++
	if ch.confirmErr != nil {
		return ch.confirmErr
	}
	ch.confirmMode = true
	return nil
}

func (ch *fakeChannel) publishDeferred(_ context.Context, exchange, key string, mandatory, _ bool, msg amqp.Publishing) (confirmation, error) {
	ch.mu.Lock()
	if err := ch.recordPublishLocked(exchange, key, msg); err != nil {
		ch.mu.Unlock()
		return nil, err
	}
	ch.pubMandatory = append(ch.pubMandatory, mandatory)
	if !ch.confirmMode {
		ch.mu.Unlock()
		return nil, nil
	}
	outcome := confirmAck
	if len(ch.confirmPlan) > 0 {
		outcome = ch.confirmPlan[0]
		ch.confirmPlan = ch.confirmPlan[1:]
	}
	if ch.returnKeys[key] {
		outcome = confirmReturn
	}
	if outcome == confirmReturn && !mandatory {
		outcome = confirmAck // the broker drops an unroutable non-mandatory message and acks it
	}
	c := newFakeConfirm()
	switch outcome {
	case confirmReturn:
		// Like RabbitMQ, the basic.return reaches the listeners before the ack.
		ret := ch.returnPayload
		if ret.ReplyCode == 0 {
			ret.ReplyCode, ret.ReplyText = amqp.NoRoute, "NO_ROUTE"
		}
		ret.Exchange, ret.RoutingKey = exchange, key
		ret.Headers, ret.MessageId, ret.Body = msg.Headers, msg.MessageId, msg.Body
		for _, l := range ch.returnNotify {
			l <- ret
		}
		c.resolve(true)
	case confirmAck:
		c.resolve(true)
	case confirmNack:
		c.resolve(false)
	case confirmNever, confirmDrop:
		ch.pending = append(ch.pending, c)
	}
	ch.mu.Unlock()
	if outcome == confirmDrop {
		_ = ch.Close()
	}
	return c, nil
}

func (ch *fakeChannel) IsClosed() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.closed
}

func (ch *fakeChannel) Consume(_, _ string, autoAck, _, _, _ bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.consumeCount++
	ch.consumeArgs = append(ch.consumeArgs, args)
	ch.autoAcks = append(ch.autoAcks, autoAck)
	if ch.consumeErr != nil {
		return nil, ch.consumeErr
	}
	return ch.deliveries, nil
}

func (ch *fakeChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.qos = &qosArgs{prefetchCount, prefetchSize, global}
	return nil
}

func (ch *fakeChannel) Ack(tag uint64, _ bool) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.acked = append(ch.acked, tag)
	return nil
}

func (ch *fakeChannel) Nack(tag uint64, multiple, requeue bool) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.nacked = append(ch.nacked, nackRecord{tag, multiple, requeue})
	return nil
}

// Reject records the call even on a closed channel, as Ack and Nack do, so a
// test can detect a settlement attempted on a dead channel.
func (ch *fakeChannel) Reject(tag uint64, requeue bool) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	ch.rejected = append(ch.rejected, rejectRecord{tag, requeue})
	return nil
}

func (ch *fakeChannel) NotifyClose(receiver chan *amqp.Error) chan *amqp.Error {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		close(receiver)
		return receiver
	}
	ch.closeNotify = append(ch.closeNotify, receiver)
	return receiver
}

// NotifyReturn registers a basic.return listener. Like amqp091, the listener is
// closed when the channel closes.
func (ch *fakeChannel) NotifyReturn(receiver chan amqp.Return) chan amqp.Return {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if ch.closed {
		close(receiver)
		return receiver
	}
	ch.returnNotify = append(ch.returnNotify, receiver)
	return receiver
}

func (ch *fakeChannel) Close() error {
	ch.mu.Lock()
	if ch.closed {
		ch.mu.Unlock()
		return nil
	}
	ch.closed = true
	listeners := ch.closeNotify
	ch.closeNotify = nil
	returnListeners := ch.returnNotify
	ch.returnNotify = nil
	pending := ch.pending
	ch.pending = nil
	ch.mu.Unlock()
	// Like amqp091, the channel is marked closed before outstanding confirms are
	// settled as not-acked.
	for _, c := range pending {
		c.resolve(false)
	}
	for _, l := range listeners {
		close(l)
	}
	for _, l := range returnListeners {
		close(l)
	}
	return nil
}

// deliver pushes a message onto the consumer's delivery stream.
func (ch *fakeChannel) deliver(d amqp.Delivery) { ch.deliveries <- d }

// ackedTags returns a copy of the acked delivery tags.
func (ch *fakeChannel) ackedTags() []uint64 {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return append([]uint64(nil), ch.acked...)
}

func (ch *fakeChannel) nackRecords() []nackRecord {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return append([]nackRecord(nil), ch.nacked...)
}

func (ch *fakeChannel) publishedCount() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return len(ch.published)
}

// consumeStarted reports whether Consume has been called on this channel.
func (ch *fakeChannel) consumeStarted() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.consumeCount > 0
}

func (ch *fakeChannel) declaredExchanges() []ExchangeDeclareArgs {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return append([]ExchangeDeclareArgs(nil), ch.exchanges...)
}

func (ch *fakeChannel) declaredQueues() []QueueDeclareArgs {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return append([]QueueDeclareArgs(nil), ch.queues...)
}

func (ch *fakeChannel) declaredBinds() []BindArgs {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return append([]BindArgs(nil), ch.binds...)
}

func (ch *fakeChannel) qosArgs() *qosArgs {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.qos
}

func (ch *fakeChannel) inConfirmMode() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.confirmMode
}

func (ch *fakeChannel) confirmCallCount() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.confirmCalls
}

func (ch *fakeChannel) pendingConfirms() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return len(ch.pending)
}

func (ch *fakeChannel) rejectRecords() []rejectRecord {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return append([]rejectRecord(nil), ch.rejected...)
}

// settleCount is the number of Ack, Nack and Reject calls made on the channel.
func (ch *fakeChannel) settleCount() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return len(ch.acked) + len(ch.nacked) + len(ch.rejected)
}

// lastConsumeArgs returns the args of the most recent Consume call.
func (ch *fakeChannel) lastConsumeArgs() amqp.Table {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	if len(ch.consumeArgs) == 0 {
		return nil
	}
	return ch.consumeArgs[len(ch.consumeArgs)-1]
}

// mandatoryFlags returns the mandatory flag of each confirmed publish.
func (ch *fakeChannel) mandatoryFlags() []bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return append([]bool(nil), ch.pubMandatory...)
}

// returnListeners reports how many basic.return listeners are registered.
func (ch *fakeChannel) returnListeners() int {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return len(ch.returnNotify)
}

// publishedAt returns a copy of the i-th published message.
func (ch *fakeChannel) publishedAt(i int) amqp.Publishing {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	return ch.published[i]
}
