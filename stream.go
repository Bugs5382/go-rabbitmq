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
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Stream queue support (issue #19).

const (
	argStreamOffset       = "x-stream-offset"
	argMaxAge             = "x-max-age"
	argMaxLengthBytes     = "x-max-length-bytes"
	argStreamSegmentBytes = "x-stream-max-segment-size-bytes"
)

// StreamOptions are the retention settings of a QueueStream queue. A zero field
// leaves that limit to the broker (and any policy). Retention is applied per
// segment: the broker drops whole segments once they are past every limit, so
// a stream can briefly hold more than the limits say.
type StreamOptions struct {
	// MaxAge drops segments whose messages are all older than this
	// (x-max-age). It must be a whole number of seconds; it is sent in the
	// largest unit that divides it exactly, so 7*24*time.Hour becomes "7D".
	MaxAge time.Duration
	// MaxLengthBytes caps the total size of the stream on disk
	// (x-max-length-bytes).
	MaxLengthBytes int64
	// MaxSegmentSizeBytes sets the size of each segment file
	// (x-stream-max-segment-size-bytes). The broker default is 500 MB.
	MaxSegmentSizeBytes int64
}

func (s StreamOptions) isZero() bool { return s == StreamOptions{} }

// validate checks the retention values RabbitMQ would reject or misread.
func (s StreamOptions) validate(queue string) error {
	switch {
	case s.MaxAge < 0:
		return fmt.Errorf("%w: stream %q has a negative MaxAge (%s)", ErrInvalidQueue, queue, s.MaxAge)
	case s.MaxAge%time.Second != 0:
		return fmt.Errorf("%w: stream %q MaxAge %s is not a whole number of seconds", ErrInvalidQueue, queue, s.MaxAge)
	case s.MaxLengthBytes < 0:
		return fmt.Errorf("%w: stream %q has a negative MaxLengthBytes (%d)", ErrInvalidQueue, queue, s.MaxLengthBytes)
	case s.MaxSegmentSizeBytes < 0:
		return fmt.Errorf("%w: stream %q has a negative MaxSegmentSizeBytes (%d)", ErrInvalidQueue, queue, s.MaxSegmentSizeBytes)
	}
	return nil
}

// formatMaxAge renders d in RabbitMQ's x-max-age syntax, using the largest of
// D, h, m and s that divides it exactly. Months and years are not used: their
// length varies, so they cannot represent a time.Duration exactly.
func formatMaxAge(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dD", d/(24*time.Hour))
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	default:
		return fmt.Sprintf("%ds", d/time.Second)
	}
}

// validateStream enforces the shape rules RabbitMQ has for streams: durable,
// named, not exclusive and not auto-delete. It runs on a normalized config.
func (q QueueConfig) validateStream() error {
	switch {
	case q.Name == "":
		return fmt.Errorf("%w: a stream queue must be named (server-named queues must be classic)", ErrInvalidQueue)
	case q.Exclusive:
		return fmt.Errorf("%w: stream %q cannot be exclusive", ErrInvalidQueue, q.Name)
	case q.AutoDelete:
		return fmt.Errorf("%w: stream %q cannot be auto-delete", ErrInvalidQueue, q.Name)
	case !q.Durable:
		return fmt.Errorf("%w: stream %q must be durable (drop Transient())", ErrInvalidQueue, q.Name)
	}
	return q.Stream.validate(q.Name)
}

// streamArgs returns the declare arguments of a stream: a copy of the caller's
// Args with x-queue-type=stream and the typed retention options, which win
// over the same keys in Args.
func (q QueueConfig) streamArgs() amqp.Table {
	out := withQueueType(q.Args, string(QueueStream))
	if q.Stream.MaxAge > 0 {
		out[argMaxAge] = formatMaxAge(q.Stream.MaxAge)
	}
	if q.Stream.MaxLengthBytes > 0 {
		out[argMaxLengthBytes] = q.Stream.MaxLengthBytes
	}
	if q.Stream.MaxSegmentSizeBytes > 0 {
		out[argStreamSegmentBytes] = q.Stream.MaxSegmentSizeBytes
	}
	return out
}

type streamOffsetKind int

const (
	offsetUnset streamOffsetKind = iota
	offsetFirst
	offsetLast
	offsetNext
	offsetAt
	offsetTime
)

// StreamOffset is where a stream consumer starts reading (x-stream-offset).
// Build one with StreamOffsetFirst, StreamOffsetLast, StreamOffsetNext,
// StreamOffsetAt or StreamOffsetTime. The zero value sends no offset, so the
// broker default (next) applies.
type StreamOffset struct {
	kind   streamOffsetKind
	offset int64
	at     time.Time
}

// StreamOffsetFirst starts at the oldest message still in the stream.
func StreamOffsetFirst() StreamOffset { return StreamOffset{kind: offsetFirst} }

// StreamOffsetLast starts at the last chunk written, so the consumer first sees
// the most recent messages (a chunk can hold several).
func StreamOffsetLast() StreamOffset { return StreamOffset{kind: offsetLast} }

// StreamOffsetNext skips everything already in the stream and delivers only
// messages published after the consumer attached. It is the broker default.
func StreamOffsetNext() StreamOffset { return StreamOffset{kind: offsetNext} }

// StreamOffsetAt starts at a numeric offset. The first message of a stream is
// offset 0; each delivery carries its own offset in the x-stream-offset header.
// A negative offset is an ErrInvalidConsumer.
func StreamOffsetAt(offset int64) StreamOffset { return StreamOffset{kind: offsetAt, offset: offset} }

// StreamOffsetTime starts at the first chunk written at or after t. The AMQP
// timestamp has one-second precision, and the broker attaches at chunk
// granularity, so a few messages from just before t can be delivered too.
func StreamOffsetTime(t time.Time) StreamOffset { return StreamOffset{kind: offsetTime, at: t} }

// IsZero reports whether no offset was chosen.
func (o StreamOffset) IsZero() bool { return o.kind == offsetUnset }

// String describes the offset, for logs.
func (o StreamOffset) String() string {
	switch o.kind {
	case offsetFirst:
		return "first"
	case offsetLast:
		return "last"
	case offsetNext:
		return "next"
	case offsetAt:
		return fmt.Sprintf("offset %d", o.offset)
	case offsetTime:
		return "timestamp " + o.at.UTC().Format(time.RFC3339)
	default:
		return "broker default"
	}
}

// arg returns the x-stream-offset consume argument.
func (o StreamOffset) arg() (any, error) {
	switch o.kind {
	case offsetFirst, offsetLast, offsetNext:
		return o.String(), nil
	case offsetAt:
		if o.offset < 0 {
			return nil, fmt.Errorf("%w: stream offset %d is negative", ErrInvalidConsumer, o.offset)
		}
		return o.offset, nil
	case offsetTime:
		if o.at.IsZero() {
			return nil, fmt.Errorf("%w: stream offset timestamp is the zero time", ErrInvalidConsumer)
		}
		return o.at, nil
	default:
		return nil, nil
	}
}

// validateStream checks the rules a consumer config must meet for streams,
// before Run touches the broker: a stream queue must have a valid shape and be
// consumed with manual acks, and a StreamOffset needs a stream queue. The
// errors are final, so Run returns them instead of retrying.
func (c ConsumerConfig) validateStream() error {
	q := c.Queue.normalize()
	if q.Type != QueueStream {
		if !c.StreamOffset.IsZero() {
			return fmt.Errorf("%w: StreamOffset is set but queue %q has type %q; it needs QueueStream",
				ErrInvalidConsumer, q.Name, q.Type)
		}
		return nil
	}
	if err := q.validate(); err != nil {
		return err
	}
	if c.AutoAck {
		return fmt.Errorf("%w: stream %q must be consumed with manual acks (AutoAck false)", ErrInvalidConsumer, q.Name)
	}
	_, err := c.StreamOffset.arg()
	return err
}

// streamCursor remembers the offset of the last delivery a stream consumer
// handled, so a session opened after a drop resumes after it instead of
// replaying from the configured StreamOffset.
type streamCursor struct {
	mu   sync.Mutex
	last int64
	set  bool
}

// record notes the offset carried by a delivery's x-stream-offset header.
func (s *streamCursor) record(headers amqp.Table) (int64, bool) {
	off, ok := headers[argStreamOffset].(int64)
	if !ok {
		return 0, false
	}
	s.mu.Lock()
	s.last, s.set = off, true
	s.mu.Unlock()
	return off, true
}

// resume returns the offset to start the next session at: just after the last
// handled delivery, or the configured one when nothing was handled yet.
func (s *streamCursor) resume(configured StreamOffset) StreamOffset {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.set {
		return configured
	}
	return StreamOffsetAt(s.last + 1)
}

// consumeArgs returns the Consume arguments for a session: the caller's Args,
// plus x-stream-offset for a stream queue. The caller's map is not mutated.
func (c ConsumerConfig) consumeArgs(offset StreamOffset) (amqp.Table, error) {
	if c.Queue.Type != QueueStream || offset.IsZero() {
		return c.Args, nil
	}
	v, err := offset.arg()
	if err != nil {
		return nil, err
	}
	out := make(amqp.Table, len(c.Args)+1)
	for k, val := range c.Args {
		out[k] = val
	}
	out[argStreamOffset] = v
	return out, nil
}
