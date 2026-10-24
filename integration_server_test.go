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
	"testing"
	"time"

	rabbitmq "github.com/Bugs5382/go-rabbitmq"
)

// TestIntegrationServerVersion reads the broker version from a live connection
// and checks it is still reported after a forced reconnect (issue #24).
func TestIntegrationServerVersion(t *testing.T) {
	url := brokerURL(t)
	mgmt := brokerMgmt(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	name, props := connName("it-server-version")
	conn, err := rabbitmq.Connect(ctx, url, rabbitmq.WithBackoff(readyBackoff), rabbitmq.WithClientProperties(props))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close() }()

	version := conn.ServerVersion()
	if version == "" {
		t.Fatal("ServerVersion is empty on a live RabbitMQ connection")
	}
	if got := conn.ServerProperties()["product"]; got != "RabbitMQ" {
		t.Errorf("ServerProperties[product] = %v, want RabbitMQ", got)
	}
	t.Logf("broker version %s", version)

	mgmt.closeConnection(t, name)
	waitUntil(t, 30*time.Second, "reconnect", func() bool { return conn.Reconnects() >= 1 && conn.Healthy() })
	if got := conn.ServerVersion(); got != version {
		t.Errorf("ServerVersion after reconnect = %q, want %q", got, version)
	}
}
