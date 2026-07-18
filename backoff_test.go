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
	"testing"
	"time"
)

func TestBackoffBaseIsExponentialAndBounded(t *testing.T) {
	t.Parallel()
	b := Backoff{Initial: 100 * time.Millisecond, Max: 1 * time.Second, Factor: 2, Jitter: 0}.normalize()

	want := []time.Duration{
		100 * time.Millisecond, // attempt 0
		200 * time.Millisecond, // attempt 1
		400 * time.Millisecond, // attempt 2
		800 * time.Millisecond, // attempt 3
		1 * time.Second,        // attempt 4 -> clamped to Max
		1 * time.Second,        // attempt 5 -> clamped to Max
	}
	for attempt, w := range want {
		if got := b.base(attempt); got != w {
			t.Errorf("base(%d) = %v, want %v", attempt, got, w)
		}
	}
}

func TestBackoffBaseDoesNotOverflow(t *testing.T) {
	t.Parallel()
	b := Backoff{Initial: time.Second, Max: 30 * time.Second, Factor: 2, Jitter: 0}.normalize()
	if got := b.base(1000); got != 30*time.Second {
		t.Errorf("base(1000) = %v, want Max %v (huge attempt must clamp, not overflow)", got, 30*time.Second)
	}
}

func TestBackoffNormalizeFillsDefaults(t *testing.T) {
	t.Parallel()
	b := Backoff{}.normalize()
	d := DefaultBackoff()
	if b.Initial != d.Initial || b.Max != d.Max || b.Factor != d.Factor {
		t.Errorf("normalize of zero value did not fill defaults: %+v", b)
	}
}

func TestBackoffNormalizeRaisesMaxBelowInitial(t *testing.T) {
	t.Parallel()
	b := Backoff{Initial: 10 * time.Second, Max: time.Second, Factor: 2}.normalize()
	if b.Max < b.Initial {
		t.Errorf("Max %v must be raised to at least Initial %v", b.Max, b.Initial)
	}
}

func TestBackoffDelayWithinJitterBounds(t *testing.T) {
	t.Parallel()
	// rng returns 0 -> noise = -jitter, and 1 -> noise = +jitter.
	low := Backoff{Initial: time.Second, Max: 10 * time.Second, Factor: 2, Jitter: 0.5, rng: func() float64 { return 0 }}.normalize()
	if got := low.delay(0); got != 500*time.Millisecond {
		t.Errorf("delay with rng=0 = %v, want base*(1-jitter) = 500ms", got)
	}

	high := Backoff{Initial: time.Second, Max: 10 * time.Second, Factor: 2, Jitter: 0.5, rng: func() float64 { return 1 }}.normalize()
	if got := high.delay(0); got != 1500*time.Millisecond {
		t.Errorf("delay with rng=1 = %v, want base*(1+jitter) = 1500ms", got)
	}
}

func TestBackoffDelayClampedToMaxEvenWithJitter(t *testing.T) {
	t.Parallel()
	b := Backoff{Initial: time.Second, Max: time.Second, Factor: 2, Jitter: 0.9, rng: func() float64 { return 1 }}.normalize()
	if got := b.delay(0); got > time.Second {
		t.Errorf("delay = %v exceeded Max %v", got, time.Second)
	}
}

func TestBackoffStop(t *testing.T) {
	t.Parallel()
	infinite := Backoff{MaxRetries: 0}.normalize()
	if infinite.stop(1_000_000) {
		t.Error("MaxRetries=0 must never stop")
	}
	bounded := Backoff{MaxRetries: 3}.normalize()
	if bounded.stop(2) {
		t.Error("should not stop before reaching MaxRetries")
	}
	if !bounded.stop(3) {
		t.Error("should stop at MaxRetries")
	}
}
