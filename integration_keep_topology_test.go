//go:build integration

package rabbitmq_test

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
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// mgmtPath builds a management API path for a named resource in the vhost of
// the broker URL ("/" when the URL names none).
func mgmtPath(t *testing.T, brokerURL, kind, name string) string {
	t.Helper()
	u, err := url.Parse(brokerURL)
	if err != nil {
		t.Fatalf("parse broker URL: %v", err)
	}
	vhost := strings.TrimPrefix(u.Path, "/")
	if vhost == "" {
		vhost = "/"
	}
	return "/api/" + kind + "/" + url.PathEscape(vhost) + "/" + url.PathEscape(name)
}

// TestIntegrationKeepTopologyAfterReconnect covers issue #25 against a live
// broker: a kept dead-letter queue that is deleted on the broker comes back,
// with its binding, after a forced reconnect, without anything consuming it.
func TestIntegrationKeepTopologyAfterReconnect(t *testing.T) {
	brokerAddr := brokerURL(t)
	mgmt := brokerMgmt(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	name, props := connName("it-keep-topology")
	conn, err := rabbitmq.Connect(ctx, brokerAddr,
		rabbitmq.WithClientProperties(props),
		rabbitmq.WithBackoff(rabbitmq.Backoff{Initial: 100 * time.Millisecond, Max: time.Second, Factor: 2}),
	)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	suffix := time.Now().UnixNano()
	exchange := fmt.Sprintf("it.keep.dlx.%d", suffix)
	queue := fmt.Sprintf("it.keep.dlq.%d", suffix)
	t.Cleanup(func() {
		if resp, err := mgmt.do(http.MethodDelete, mgmtPath(t, brokerAddr, "exchanges", exchange)); err == nil {
			_ = resp.Body.Close()
		}
	})

	if err := conn.KeepTopology(ctx, rabbitmq.Topology{
		Exchanges: []rabbitmq.ExchangeConfig{{Name: exchange, Kind: "fanout"}},
		Queues:    []rabbitmq.QueueConfig{{Name: queue, Args: amqp.Table{"x-expires": int32(60_000)}}},
		Bindings:  []rabbitmq.BindingConfig{{Queue: queue, Exchange: exchange}},
	}); err != nil {
		t.Fatalf("KeepTopology: %v", err)
	}

	pub := conn.NewPublisher(exchange, rabbitmq.WithMandatory(), rabbitmq.WithPublishRetries(10),
		rabbitmq.WithConfirmTimeout(5*time.Second))
	defer func() { _ = pub.Close() }()
	if err := pub.Publish(ctx, "", []byte("before")); err != nil {
		t.Fatalf("publish to the kept topology: %v", err)
	}

	resp, err := mgmt.do(http.MethodDelete, mgmtPath(t, brokerAddr, "queues", queue))
	if err != nil {
		t.Fatalf("delete the kept queue on the broker: %v", err)
	}
	_ = resp.Body.Close()
	if err := pub.Publish(ctx, "", []byte("deleted")); !errors.Is(err, rabbitmq.ErrUnroutable) {
		t.Fatalf("publish with the queue deleted: want ErrUnroutable, got %v", err)
	}

	before := conn.Reconnects()
	if n := mgmt.closeConnection(t, name); n != 1 {
		t.Fatalf("closed %d connections, want 1", n)
	}
	waitUntil(t, 30*time.Second, "reconnect", func() bool { return conn.Reconnects() > before && conn.Healthy() })

	if err := pub.Publish(ctx, "", []byte("after")); err != nil {
		t.Fatalf("publish after the reconnect: want the kept queue back, got %v", err)
	}
	got, err := mgmt.get(mgmtPath(t, brokerAddr, "queues", queue))
	if err != nil {
		t.Fatalf("kept queue missing after the reconnect: %v", err)
	}
	_ = got.Body.Close()
}
