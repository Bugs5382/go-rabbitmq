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
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDefaultConnOptions(t *testing.T) {
	t.Parallel()
	o := defaultConnOptions()
	if o.logger == nil || o.observer == nil || o.dial == nil {
		t.Fatal("defaults must supply logger, observer and dialer")
	}
	if _, ok := o.logger.(nopLogger); !ok {
		t.Error("default logger should be nopLogger")
	}
	if _, ok := o.observer.(NopObserver); !ok {
		t.Error("default observer should be NopObserver")
	}
}

func TestOptionsApply(t *testing.T) {
	t.Parallel()
	o := defaultConnOptions()
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	for _, opt := range []Option{
		WithBackoff(Backoff{Initial: time.Second}),
		WithTLS(tlsCfg),
		WithHeartbeat(3 * time.Second),
		WithVhost("/app"),
		WithClientProperties(amqp.Table{"connection_name": "svc"}),
	} {
		opt(&o)
	}
	if o.backoff.Initial != time.Second {
		t.Error("WithBackoff not applied")
	}
	if o.tls != tlsCfg {
		t.Error("WithTLS not applied")
	}
	if o.heartbeat != 3*time.Second {
		t.Error("WithHeartbeat not applied")
	}
	if o.vhost != "/app" {
		t.Error("WithVhost not applied")
	}

	cfg := o.amqpConfig()
	if cfg.Heartbeat != 3*time.Second || cfg.Vhost != "/app" || cfg.TLSClientConfig != tlsCfg {
		t.Errorf("amqpConfig did not carry options: %+v", cfg)
	}
	if cfg.Properties["connection_name"] != "svc" {
		t.Error("client properties not carried into amqpConfig")
	}
}

func TestNilLoggerAndObserverAreIgnored(t *testing.T) {
	t.Parallel()
	o := defaultConnOptions()
	WithLogger(nil)(&o)
	WithObserver(nil)(&o)
	if o.logger == nil || o.observer == nil {
		t.Error("passing nil must not clear the default logger/observer")
	}
}

func TestSafeURLRedactsPassword(t *testing.T) {
	t.Parallel()
	got := safeURL("amqp://user:sup3rsecret@rabbit.example.com:5672/vhost")
	if got == "" || contains(got, "sup3rsecret") {
		t.Errorf("safeURL leaked the password: %q", got)
	}
	if !contains(got, "rabbit.example.com") {
		t.Errorf("safeURL should keep the host: %q", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
