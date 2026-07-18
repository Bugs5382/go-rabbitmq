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

// Observer receives lifecycle and message events so callers can wire in metrics
// and tracing (for example an OpenTelemetry-backed implementation) without this
// package taking a hard dependency on any telemetry library. Every method must be
// safe for concurrent use and must not block; the library calls them inline on
// its connection, publish and consume paths. The default is a no-op; supply one
// with WithObserver.
type Observer interface {
	// OnConnect fires after a connection (or reconnection) is established.
	OnConnect()
	// OnDisconnect fires when the managed connection is observed closed. err is
	// the broker/transport error, or nil for a deliberate Close.
	OnDisconnect(err error)
	// OnReconnect fires before each re-dial attempt; attempt starts at 1.
	OnReconnect(attempt int)
	// OnPublish fires after each publish attempt with its outcome; err is nil on
	// success.
	OnPublish(exchange, routingKey string, err error)
	// OnConsume fires after a delivery is handled; err is the handler's returned
	// error (nil on success), which also decided ack vs nack.
	OnConsume(queue string, d Delivery, err error)
}

// NopObserver is an Observer that ignores every event. It is the default and a
// convenient base to embed when implementing only a subset of the interface.
type NopObserver struct{}

// OnConnect implements Observer.
func (NopObserver) OnConnect() {}

// OnDisconnect implements Observer.
func (NopObserver) OnDisconnect(error) {}

// OnReconnect implements Observer.
func (NopObserver) OnReconnect(int) {}

// OnPublish implements Observer.
func (NopObserver) OnPublish(string, string, error) {}

// OnConsume implements Observer.
func (NopObserver) OnConsume(string, Delivery, error) {}
