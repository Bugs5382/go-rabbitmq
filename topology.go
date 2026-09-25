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
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// QueueType selects the broker-side queue implementation.
type QueueType string

const (
	// QueueClassic is the traditional (non-replicated) queue. It is the default
	// and the only type that may be exclusive, auto-delete or server-named.
	//
	// For those classic-only shapes the declare always carries
	// x-queue-type=classic, so a broker configured with
	// default_queue_type=quorum still creates a classic queue. A durable, named
	// classic queue is declared without x-queue-type and so follows the broker
	// default; set QueueConfig.Args["x-queue-type"] to "classic" to pin it.
	QueueClassic QueueType = "classic"
	// QueueQuorum is a replicated, Raft-based durable queue. It must be durable
	// and named, and may not be exclusive or auto-delete.
	QueueQuorum QueueType = "quorum"
)

// ExchangeConfig describes an exchange to declare. The zero value declares a
// durable topic exchange, which is the common default.
type ExchangeConfig struct {
	Name       string
	Kind       string // "topic" (default), "direct", "fanout", "headers"
	Durable    bool   // defaults to true via normalize
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table

	// durableSet distinguishes an explicit Durable:false from the zero value so
	// the sensible durable default can be applied.
	durableSet bool
}

// Durable-by-default cannot be expressed with a bool zero value, so callers who
// genuinely want a transient exchange use Transient to make the intent explicit.
func (e ExchangeConfig) normalize() ExchangeConfig {
	if e.Kind == "" {
		e.Kind = "topic"
	}
	if !e.durableSet {
		e.Durable = true
	}
	return e
}

// QueueConfig describes a queue to declare. The zero value declares a durable
// classic queue.
type QueueConfig struct {
	Name       string
	Type       QueueType // QueueClassic (default) or QueueQuorum
	Durable    bool      // defaults to true via normalize
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	// Args are extra declare arguments. A caller-supplied x-queue-type is kept
	// for classic queues; for QueueQuorum it is always set to "quorum".
	Args amqp.Table

	durableSet bool
}

func (q QueueConfig) normalize() QueueConfig {
	if q.Type == "" {
		q.Type = QueueClassic
	}
	if !q.durableSet {
		q.Durable = true
	}
	if q.Type == QueueQuorum {
		q.Durable = true // quorum queues are always durable.
	}
	return q
}

// validate enforces the broker rules that would otherwise fail obscurely at
// declare time. The most common footgun is asking for a quorum queue that is also
// exclusive, auto-delete, or server-named (empty Name); those queue shapes must
// be classic.
func (q QueueConfig) validate() error {
	if q.Type != QueueClassic && q.Type != QueueQuorum {
		return fmt.Errorf("%w: unknown queue type %q", ErrInvalidQueue, q.Type)
	}
	if q.Type == QueueQuorum {
		switch {
		case q.Name == "":
			return fmt.Errorf("%w: a quorum queue must be named (server-named queues must be classic)", ErrInvalidQueue)
		case q.Exclusive:
			return fmt.Errorf("%w: a quorum queue cannot be exclusive", ErrInvalidQueue)
		case q.AutoDelete:
			return fmt.Errorf("%w: a quorum queue cannot be auto-delete", ErrInvalidQueue)
		}
	}
	return nil
}

// classicOnly reports whether the queue shape can only ever be a classic queue:
// server-named, exclusive or auto-delete. No other queue type accepts these
// shapes, so an existing queue of this shape is always classic.
func (q QueueConfig) classicOnly() bool {
	return q.Name == "" || q.Exclusive || q.AutoDelete
}

// args returns the declare arguments without mutating the caller's map.
//
// A quorum queue always gets x-queue-type=quorum. A classic-only shape gets
// x-queue-type=classic unless the caller already set one, so a broker whose
// default_queue_type is quorum cannot turn it into a quorum declare that the
// broker then rejects (issue #3). Asserting the type there is always safe: no
// existing queue of that shape can have a different type.
//
// A durable, named classic queue is sent without x-queue-type, as in v1.0.0, so
// the broker default applies. Stating classic there would make re-declaring a
// queue that a quorum-default broker already created as quorum fail with
// PRECONDITION_FAILED. Set Args["x-queue-type"] to pin the type explicitly.
func (q QueueConfig) args() amqp.Table {
	switch {
	case q.Type == QueueQuorum:
		return withQueueType(q.Args, string(QueueQuorum))
	case q.classicOnly():
		if _, set := q.Args["x-queue-type"]; set {
			return q.Args
		}
		return withQueueType(q.Args, string(QueueClassic))
	default:
		return q.Args
	}
}

// withQueueType returns a copy of args with x-queue-type set to typ.
func withQueueType(args amqp.Table, typ string) amqp.Table {
	out := make(amqp.Table, len(args)+1)
	for k, v := range args {
		out[k] = v
	}
	out["x-queue-type"] = typ
	return out
}

// BindingConfig describes a queue-to-exchange binding.
type BindingConfig struct {
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Args       amqp.Table
}

// Transient returns a copy of the exchange config marked non-durable, recording
// the explicit intent so normalize does not re-apply the durable default.
func (e ExchangeConfig) Transient() ExchangeConfig {
	e.Durable = false
	e.durableSet = true
	return e
}

// Transient returns a copy of the queue config marked non-durable.
func (q QueueConfig) Transient() QueueConfig {
	q.Durable = false
	q.durableSet = true
	return q
}

// declareExchangeOn declares an exchange on an already-open channel.
func declareExchangeOn(ch wireChannel, cfg ExchangeConfig) error {
	cfg = cfg.normalize()
	if cfg.Name == "" {
		return nil // the default exchange always exists; nothing to declare.
	}
	return ch.ExchangeDeclare(cfg.Name, cfg.Kind, cfg.Durable, cfg.AutoDelete, cfg.Internal, cfg.NoWait, cfg.Args)
}

// declareQueueOn validates then declares a queue on an already-open channel,
// returning the server's view of the queue (its name, message and consumer
// counts).
func declareQueueOn(ch wireChannel, cfg QueueConfig) (amqp.Queue, error) {
	cfg = cfg.normalize()
	if err := cfg.validate(); err != nil {
		return amqp.Queue{}, err
	}
	return ch.QueueDeclare(cfg.Name, cfg.Durable, cfg.AutoDelete, cfg.Exclusive, cfg.NoWait, cfg.args())
}

// DeclareExchange idempotently declares an exchange. Repeated calls with matching
// arguments are a no-op on the broker.
func (c *Conn) DeclareExchange(ctx context.Context, cfg ExchangeConfig) error {
	ch, err := c.openChannel(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	return declareExchangeOn(ch, cfg)
}

// DeclareQueue idempotently declares a queue and returns the broker's view of it.
// The returned amqp.Queue.Name is authoritative for server-named queues.
func (c *Conn) DeclareQueue(ctx context.Context, cfg QueueConfig) (amqp.Queue, error) {
	ch, err := c.openChannel(ctx)
	if err != nil {
		return amqp.Queue{}, err
	}
	defer func() { _ = ch.Close() }()
	return declareQueueOn(ch, cfg)
}

// BindQueue binds a queue to an exchange with a routing key.
func (c *Conn) BindQueue(ctx context.Context, cfg BindingConfig) error {
	ch, err := c.openChannel(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	return ch.QueueBind(cfg.Queue, cfg.RoutingKey, cfg.Exchange, cfg.NoWait, cfg.Args)
}

// Topology is a bundle of exchanges, queues and bindings that should exist
// together. DeclareTopology declares them all on a single channel, in order:
// exchanges, then queues, then bindings.
type Topology struct {
	Exchanges []ExchangeConfig
	Queues    []QueueConfig
	Bindings  []BindingConfig
}

// DeclareTopology declares an entire Topology idempotently on one channel. It is
// the convenient way to establish the common "topic exchange + durable queue +
// binding" shape at start-up.
func (c *Conn) DeclareTopology(ctx context.Context, t Topology) error {
	ch, err := c.openChannel(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = ch.Close() }()
	return declareTopologyOn(ch, t)
}

// declareTopologyOn declares a whole topology on an already-open channel. It is
// reused by the consumer when re-establishing its topology after a reconnect.
func declareTopologyOn(ch wireChannel, t Topology) error {
	for _, e := range t.Exchanges {
		if err := declareExchangeOn(ch, e); err != nil {
			return fmt.Errorf("declare exchange %q: %w", e.Name, err)
		}
	}
	for _, q := range t.Queues {
		if _, err := declareQueueOn(ch, q); err != nil {
			return fmt.Errorf("declare queue %q: %w", q.Name, err)
		}
	}
	for _, b := range t.Bindings {
		if err := ch.QueueBind(b.Queue, b.RoutingKey, b.Exchange, b.NoWait, b.Args); err != nil {
			return fmt.Errorf("bind queue %q to %q: %w", b.Queue, b.Exchange, err)
		}
	}
	return nil
}
