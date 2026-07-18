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
	"crypto/tls"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// connOptions holds the resolved configuration for a Conn. It is populated from
// the DefaultBackoff baseline and mutated by the functional Options.
type connOptions struct {
	backoff             Backoff
	tls                 *tls.Config
	logger              Logger
	observer            Observer
	heartbeat           time.Duration
	vhost               string
	properties          amqp.Table
	dial                dialFunc
	publishInterceptors []PublishInterceptor
	consumeInterceptors []ConsumeInterceptor
}

func defaultConnOptions() connOptions {
	return connOptions{
		backoff:   DefaultBackoff(),
		logger:    nopLogger{},
		observer:  NopObserver{},
		heartbeat: 10 * time.Second,
		dial:      defaultDial,
	}
}

// Option configures a Conn at Connect time.
type Option func(*connOptions)

// WithBackoff sets the reconnect/retry backoff policy. Unset fields are filled
// with their defaults.
func WithBackoff(b Backoff) Option {
	return func(o *connOptions) { o.backoff = b.normalize() }
}

// WithTLS enables TLS by supplying a client configuration. This is applied when
// the URL scheme is amqps; use it to present a client certificate (mTLS) or to
// pin a CA bundle. Passing a non-nil config also forces the connection through
// the TLS handshake path.
func WithTLS(cfg *tls.Config) Option {
	return func(o *connOptions) { o.tls = cfg }
}

// WithLogger plugs in a logger. The default is a no-op.
func WithLogger(l Logger) Option {
	return func(o *connOptions) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithObserver plugs in an Observer for metrics/tracing. The default is a no-op.
func WithObserver(obs Observer) Option {
	return func(o *connOptions) {
		if obs != nil {
			o.observer = obs
		}
	}
}

// WithHeartbeat sets the AMQP heartbeat interval. The default is 10s. A value
// under one second lets the server choose.
func WithHeartbeat(d time.Duration) Option {
	return func(o *connOptions) { o.heartbeat = d }
}

// WithVhost overrides the virtual host. By default the vhost parsed from the URL
// is used.
func WithVhost(vhost string) Option {
	return func(o *connOptions) { o.vhost = vhost }
}

// WithClientProperties advertises client properties (for example a connection
// name shown in the management UI) to the broker.
func WithClientProperties(props amqp.Table) Option {
	return func(o *connOptions) { o.properties = props }
}

// WithPublishInterceptor registers one or more publish interceptors, applied to
// every Publisher created from the Conn. They run outermost-first in the order
// given. This is how the OTel adapter injects tracing; see the otel subpackage.
func WithPublishInterceptor(interceptors ...PublishInterceptor) Option {
	return func(o *connOptions) { o.publishInterceptors = append(o.publishInterceptors, interceptors...) }
}

// WithConsumeInterceptor registers one or more consume interceptors, applied to
// every consumer started on the Conn. They run outermost-first in the order
// given, wrapping the Handler.
func WithConsumeInterceptor(interceptors ...ConsumeInterceptor) Option {
	return func(o *connOptions) { o.consumeInterceptors = append(o.consumeInterceptors, interceptors...) }
}

// withDialer overrides the dialer. It is unexported and used only by tests to
// inject a fake broker.
func withDialer(d dialFunc) Option {
	return func(o *connOptions) {
		if d != nil {
			o.dial = d
		}
	}
}

// amqpConfig builds the amqp091 Config from the resolved options.
func (o connOptions) amqpConfig() amqp.Config {
	cfg := amqp.Config{
		Heartbeat:       o.heartbeat,
		Vhost:           o.vhost,
		TLSClientConfig: o.tls,
		Properties:      o.properties,
		Locale:          "en_US",
	}
	return cfg
}
