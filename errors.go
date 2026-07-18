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

import "errors"

var (
	// ErrClosed is returned by operations on a Conn after Close has been called.
	ErrClosed = errors.New("rabbitmq: connection manager is closed")

	// ErrNotReady is returned when a live connection could not be obtained before
	// the supplied context expired.
	ErrNotReady = errors.New("rabbitmq: no live connection available")

	// ErrPublishFailed wraps the last publish error after all bounded retries were
	// exhausted. Callers (for example an outbox worker) should treat it as
	// retryable and try again later.
	ErrPublishFailed = errors.New("rabbitmq: publish failed after retries")

	// ErrInvalidQueue is returned when a QueueConfig violates a broker rule, most
	// commonly a quorum queue that is also exclusive, auto-delete or server-named.
	ErrInvalidQueue = errors.New("rabbitmq: invalid queue configuration")
)
