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
	"math"
	"math/rand"
	"time"
)

// Backoff configures the bounded exponential backoff used when re-dialling a
// dropped connection and when retrying a failed publish. The zero value is not
// usable on its own; DefaultBackoff supplies sensible defaults and normalize
// fills any unset field.
type Backoff struct {
	// Initial is the delay before the first retry. Default 500ms.
	Initial time.Duration
	// Max caps the delay of any single retry. Default 30s.
	Max time.Duration
	// Factor is the exponential growth multiplier per attempt. Default 2.0.
	Factor float64
	// Jitter is the fraction (0..1) of a delay applied as random +/- noise to
	// avoid a thundering herd of reconnects. Default 0.2. A value <=0 disables
	// jitter (useful for deterministic tests).
	Jitter float64
	// MaxRetries bounds the number of retries. 0 means retry indefinitely (the
	// right choice for a long-lived connection). A positive value bounds publish
	// retries and initial-connect attempts.
	MaxRetries int

	// rng is an injectable source for jitter, used by tests. When nil the package
	// default source is used.
	rng func() float64
}

// DefaultBackoff returns the backoff used when none is supplied: 500ms initial,
// 30s cap, factor 2.0, 20% jitter, retry forever.
func DefaultBackoff() Backoff {
	return Backoff{
		Initial:    500 * time.Millisecond,
		Max:        30 * time.Second,
		Factor:     2.0,
		Jitter:     0.2,
		MaxRetries: 0,
	}
}

// normalize returns a copy with any zero/invalid field replaced by its default.
func (b Backoff) normalize() Backoff {
	d := DefaultBackoff()
	if b.Initial <= 0 {
		b.Initial = d.Initial
	}
	if b.Max <= 0 {
		b.Max = d.Max
	}
	if b.Factor < 1 {
		b.Factor = d.Factor
	}
	if b.Jitter < 0 {
		b.Jitter = 0
	}
	if b.Max < b.Initial {
		b.Max = b.Initial
	}
	return b
}

// base returns the un-jittered delay for a zero-based attempt number, clamped to
// Max. attempt 0 yields Initial.
func (b Backoff) base(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := float64(b.Initial) * math.Pow(b.Factor, float64(attempt))
	if d > float64(b.Max) || math.IsInf(d, 1) {
		return b.Max
	}
	return time.Duration(d)
}

// delay returns the jittered delay for a zero-based attempt number. The result is
// always within [0, Max] and, with the default jitter, sits within
// [base*(1-jitter), base*(1+jitter)] clamped to Max.
func (b Backoff) delay(attempt int) time.Duration {
	base := b.base(attempt)
	if b.Jitter <= 0 {
		return base
	}
	r := b.rng
	if r == nil {
		r = rand.Float64
	}
	// noise in [-jitter, +jitter].
	noise := (r()*2 - 1) * b.Jitter
	d := float64(base) * (1 + noise)
	if d < 0 {
		d = 0
	}
	if d > float64(b.Max) {
		d = float64(b.Max)
	}
	return time.Duration(d)
}

// stop reports whether retrying should stop after the given zero-based attempt
// count. With MaxRetries == 0 it never stops.
func (b Backoff) stop(attempt int) bool {
	return b.MaxRetries > 0 && attempt >= b.MaxRetries
}
