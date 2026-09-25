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
	"strings"
	"sync"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// recordingLogger keeps every formatted line so a test can assert what was
// logged.
type recordingLogger struct {
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) add(level, format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, level+" "+fmt.Sprintf(format, args...))
}

func (l *recordingLogger) Debugf(f string, a ...any) { l.add("DEBUG", f, a...) }
func (l *recordingLogger) Infof(f string, a ...any)  { l.add("INFO", f, a...) }
func (l *recordingLogger) Warnf(f string, a ...any)  { l.add("WARN", f, a...) }
func (l *recordingLogger) Errorf(f string, a ...any) { l.add("ERROR", f, a...) }

func (l *recordingLogger) contains(level, substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, line := range l.lines {
		if strings.HasPrefix(line, level+" ") && strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

// transientRefusal is the error RabbitMQ 4 returns for a transient,
// non-exclusive queue when the transient_nonexcl_queues feature is not
// permitted (issue #12).
var transientRefusal = &amqp.Error{
	Code: amqp.InternalError,
	Reason: "INTERNAL_ERROR - Feature `transient_nonexcl_queues` is deprecated.\n" +
		"By default, this feature is not permitted anymore.",
}

// Issue #12: the broker's refusal of a transient non-exclusive queue is turned
// into ErrInvalidQueue with a message that names the fix, and the broker error
// stays reachable with errors.As.
func TestDeclareTransientNonExclusiveQueueRefusalNamesTheFix(t *testing.T) {
	t.Parallel()
	ch := newFakeChannel()
	ch.declareErr = transientRefusal
	log := &recordingLogger{}

	_, err := declareQueueOn(ch, QueueConfig{Name: "tmp", AutoDelete: true}.Transient(), log)
	if !errors.Is(err, ErrInvalidQueue) {
		t.Fatalf("expected ErrInvalidQueue, got %v", err)
	}
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != amqp.InternalError {
		t.Errorf("the broker error must stay reachable with errors.As, got %v", err)
	}
	for _, want := range []string{`"tmp"`, "RabbitMQ 4", "Exclusive", "Transient()", "AutoDelete"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if !log.contains("WARN", `"tmp"`) {
		t.Errorf("expected a warning naming the queue, got %v", log.lines)
	}
}

// Any other declare failure is passed through untouched.
func TestDeclareQueueOtherErrorsPassThrough(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		cfg QueueConfig
		err error
	}{
		"precondition failed": {
			cfg: QueueConfig{Name: "tmp", AutoDelete: true}.Transient(),
			err: &amqp.Error{Code: amqp.PreconditionFailed, Reason: "PRECONDITION_FAILED - inequivalent arg 'durable'"},
		},
		"plain error": {
			cfg: QueueConfig{Name: "orders"},
			err: errors.New("boom"),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ch := newFakeChannel()
			ch.declareErr = tc.err
			_, err := declareQueueOn(ch, tc.cfg, nopLogger{})
			if !errors.Is(err, tc.err) {
				t.Errorf("expected the original error, got %v", err)
			}
			if errors.Is(err, ErrInvalidQueue) {
				t.Errorf("%s must not be reported as ErrInvalidQueue: %v", name, err)
			}
		})
	}
}

// The library never rejects the shape on its own: RabbitMQ 3, and a RabbitMQ 4
// broker that re-enabled the feature, accept it, so the declare must reach the
// broker.
func TestTransientNonExclusiveQueueIsStillDeclared(t *testing.T) {
	t.Parallel()
	ch := newFakeChannel()
	log := &recordingLogger{}
	if _, err := declareQueueOn(ch, QueueConfig{Name: "tmp"}.Transient(), log); err != nil {
		t.Fatalf("declareQueueOn: %v", err)
	}
	if q := ch.declaredQueues(); len(q) != 1 || q[0].Durable || q[0].Exclusive {
		t.Errorf("declared queues = %+v, want one transient non-exclusive queue", q)
	}
	if !log.contains("DEBUG", "transient_nonexcl_queues") {
		t.Errorf("expected a debug note about the RabbitMQ 4 rule, got %v", log.lines)
	}
}

// The refusal is explained on every declare path, including DeclareTopology.
func TestDeclareTopologyExplainsTransientRefusal(t *testing.T) {
	t.Parallel()
	ch := newFakeChannel()
	ch.declareErr = transientRefusal
	err := declareTopologyOn(ch, nopLogger{}, Topology{
		Queues: []QueueConfig{QueueConfig{Name: "tmp"}.Transient()},
	})
	if !errors.Is(err, ErrInvalidQueue) || !strings.Contains(err.Error(), "Exclusive") {
		t.Errorf("expected the explained ErrInvalidQueue, got %v", err)
	}
}

func TestTransientNonExclusiveShape(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		cfg  QueueConfig
		want bool
	}{
		"durable":                    {QueueConfig{Name: "q"}, false},
		"durable auto-delete":        {QueueConfig{Name: "q", AutoDelete: true}, false},
		"transient":                  {QueueConfig{Name: "q"}.Transient(), true},
		"transient auto-delete":      {QueueConfig{Name: "q", AutoDelete: true}.Transient(), true},
		"transient exclusive":        {QueueConfig{Name: "q", Exclusive: true}.Transient(), false},
		"transient quorum (durable)": {QueueConfig{Name: "q", Type: QueueQuorum}.Transient(), false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := tc.cfg.normalize().transientNonExclusive(); got != tc.want {
				t.Errorf("transientNonExclusive() = %t, want %t", got, tc.want)
			}
		})
	}
}
