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
	"testing"
)

func TestQueueConfigNormalizeDefaults(t *testing.T) {
	t.Parallel()
	q := QueueConfig{}.normalize()
	if q.Type != QueueClassic {
		t.Errorf("default type = %q, want classic", q.Type)
	}
	if !q.Durable {
		t.Error("default queue should be durable")
	}
}

func TestQueueConfigTransientIsNotDurable(t *testing.T) {
	t.Parallel()
	q := QueueConfig{Name: "tmp"}.Transient().normalize()
	if q.Durable {
		t.Error("Transient queue must not be durable after normalize")
	}
}

func TestQuorumQueueIsForcedDurable(t *testing.T) {
	t.Parallel()
	q := QueueConfig{Name: "orders", Type: QueueQuorum}.Transient().normalize()
	if !q.Durable {
		t.Error("quorum queue must always be durable, even if marked transient")
	}
}

func TestQuorumQueueValidationGuards(t *testing.T) {
	t.Parallel()
	cases := map[string]QueueConfig{
		"server-named": {Name: "", Type: QueueQuorum},
		"exclusive":    {Name: "q", Type: QueueQuorum, Exclusive: true},
		"auto-delete":  {Name: "q", Type: QueueQuorum, AutoDelete: true},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := cfg.normalize().validate()
			if !errors.Is(err, ErrInvalidQueue) {
				t.Errorf("expected ErrInvalidQueue for %s quorum queue, got %v", name, err)
			}
		})
	}
}

func TestClassicQueueAllowsExclusiveAndServerNamed(t *testing.T) {
	t.Parallel()
	cfg := QueueConfig{Name: "", Exclusive: true, AutoDelete: true}.Transient().normalize()
	if err := cfg.validate(); err != nil {
		t.Errorf("classic server-named/exclusive/auto-delete queue should be valid, got %v", err)
	}
}

func TestQuorumQueueInjectsQueueTypeArg(t *testing.T) {
	t.Parallel()
	q := QueueConfig{Name: "orders", Type: QueueQuorum, Args: map[string]any{"x-max-length": 100}}.normalize()
	args := q.args()
	if args["x-queue-type"] != "quorum" {
		t.Errorf("expected x-queue-type=quorum, got %v", args["x-queue-type"])
	}
	if args["x-max-length"] != 100 {
		t.Error("existing args must be preserved")
	}
	// original caller map must be untouched.
	if _, ok := q.Args["x-queue-type"]; ok {
		t.Error("args() must not mutate the caller's map")
	}
}

func TestClassicQueuePassesArgsThrough(t *testing.T) {
	t.Parallel()
	q := QueueConfig{Name: "q", Args: map[string]any{"k": "v"}}.normalize()
	if q.args()["k"] != "v" {
		t.Error("classic queue args should pass through unchanged")
	}
}

func TestExchangeNormalizeDefaultsToDurableTopic(t *testing.T) {
	t.Parallel()
	e := ExchangeConfig{Name: "events"}.normalize()
	if e.Kind != "topic" {
		t.Errorf("default kind = %q, want topic", e.Kind)
	}
	if !e.Durable {
		t.Error("default exchange should be durable")
	}
	if (ExchangeConfig{Name: "events"}).Transient().normalize().Durable {
		t.Error("transient exchange must not be durable")
	}
}

func TestDeclareTopologyOnRecordsAllInOrder(t *testing.T) {
	t.Parallel()
	ch := newFakeChannel()
	err := declareTopologyOn(ch, nopLogger{}, Topology{
		Exchanges: []ExchangeConfig{{Name: "events"}},
		Queues:    []QueueConfig{{Name: "orders"}},
		Bindings:  []BindingConfig{{Queue: "orders", Exchange: "events", RoutingKey: "orders.*"}},
	})
	if err != nil {
		t.Fatalf("declareTopologyOn: %v", err)
	}
	if len(ch.exchanges) != 1 || ch.exchanges[0].Name != "events" || ch.exchanges[0].Kind != "topic" || !ch.exchanges[0].Durable {
		t.Errorf("exchange not declared as durable topic: %+v", ch.exchanges)
	}
	if len(ch.queues) != 1 || ch.queues[0].Name != "orders" || !ch.queues[0].Durable {
		t.Errorf("queue not declared durable: %+v", ch.queues)
	}
	if len(ch.binds) != 1 || ch.binds[0].Key != "orders.*" {
		t.Errorf("binding not recorded: %+v", ch.binds)
	}
}

func TestDeclareTopologyOnPropagatesInvalidQueue(t *testing.T) {
	t.Parallel()
	ch := newFakeChannel()
	err := declareTopologyOn(ch, nopLogger{}, Topology{
		Queues: []QueueConfig{{Name: "", Type: QueueQuorum}},
	})
	if !errors.Is(err, ErrInvalidQueue) {
		t.Errorf("expected ErrInvalidQueue to propagate, got %v", err)
	}
}

// Issue #3: a queue shape that can only be classic (server-named, exclusive or
// auto-delete) must state x-queue-type=classic on the wire, so a broker whose
// default_queue_type is quorum cannot turn it into a quorum declare and fail it
// with PRECONDITION_FAILED.
func TestClassicOnlyQueueShapesAssertQueueType(t *testing.T) {
	t.Parallel()
	cases := map[string]QueueConfig{
		"server-named": {Name: ""},
		"exclusive":    {Name: "q", Exclusive: true},
		"auto-delete":  {Name: "q", AutoDelete: true},
		"explicit classic, server-named, transient": QueueConfig{
			Name: "", Type: QueueClassic, Exclusive: true, AutoDelete: true,
		}.Transient(),
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if got := cfg.normalize().args()["x-queue-type"]; got != "classic" {
				t.Errorf("x-queue-type = %v, want classic", got)
			}
		})
	}
}

func TestClassicOnlyQueueDoesNotMutateCallerArgs(t *testing.T) {
	t.Parallel()
	caller := map[string]any{"x-message-ttl": 1000}
	args := QueueConfig{Name: "", Args: caller}.normalize().args()
	if args["x-queue-type"] != "classic" || args["x-message-ttl"] != 1000 {
		t.Errorf("args = %v, want x-queue-type=classic and the caller's ttl", args)
	}
	if _, ok := caller["x-queue-type"]; ok {
		t.Error("args() must not mutate the caller's map")
	}
}

func TestClassicOnlyQueueKeepsCallerQueueType(t *testing.T) {
	t.Parallel()
	args := QueueConfig{Name: "", Args: map[string]any{"x-queue-type": "custom"}}.normalize().args()
	if args["x-queue-type"] != "custom" {
		t.Errorf("caller-supplied x-queue-type was overridden: %v", args["x-queue-type"])
	}
}

// A durable, named classic queue keeps its v1 wire shape (no x-queue-type), so
// re-declaring a queue that a quorum-default broker already created as quorum
// does not start failing after an upgrade.
func TestDurableNamedClassicQueueLeavesTypeToBroker(t *testing.T) {
	t.Parallel()
	for _, typ := range []QueueType{"", QueueClassic} {
		if _, ok := (QueueConfig{Name: "orders", Type: typ}).normalize().args()["x-queue-type"]; ok {
			t.Errorf("type %q: durable named classic queue must not send x-queue-type", typ)
		}
	}
}

func TestDeclareServerNamedQueueSendsClassicType(t *testing.T) {
	t.Parallel()
	ch := newFakeChannel()
	cfg := QueueConfig{Name: "", Type: QueueClassic, AutoDelete: true, Exclusive: true}.Transient()
	if _, err := declareQueueOn(ch, cfg, nopLogger{}); err != nil {
		t.Fatalf("declareQueueOn: %v", err)
	}
	q := ch.declaredQueues()
	if len(q) != 1 || q[0].Args["x-queue-type"] != "classic" {
		t.Errorf("declared queue args = %+v, want x-queue-type=classic", q)
	}
}
