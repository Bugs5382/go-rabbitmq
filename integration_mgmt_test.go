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
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// mgmtClient talks to the RabbitMQ management API so a test can force a real
// reconnect by closing the library's connection on the broker side.
type mgmtClient struct {
	base       string
	user, pass string
	http       *http.Client
}

// brokerMgmt returns a management client for the broker in RABBITMQ_TEST_URL.
// The API is taken from RABBITMQ_TEST_MGMT_URL, or else port 15672 on the AMQP
// host with the AMQP credentials. The test skips when the API is unreachable,
// unless RABBITMQ_TEST_REQUIRED is set (as in CI), where it fails.
func brokerMgmt(t *testing.T) *mgmtClient {
	t.Helper()
	u, err := url.Parse(brokerURL(t))
	if err != nil {
		t.Fatalf("parse RABBITMQ_TEST_URL: %v", err)
	}
	base := os.Getenv("RABBITMQ_TEST_MGMT_URL")
	if base == "" {
		base = "http://" + net.JoinHostPort(u.Hostname(), "15672")
	}
	pass, _ := u.User.Password()
	m := &mgmtClient{base: base, user: u.User.Username(), pass: pass, http: &http.Client{Timeout: 5 * time.Second}}
	if _, err := m.get("/api/overview"); err != nil {
		if os.Getenv("RABBITMQ_TEST_REQUIRED") != "" {
			t.Fatalf("management API at %s: %v", base, err)
		}
		t.Skipf("management API at %s unreachable (%v); set RABBITMQ_TEST_MGMT_URL", base, err)
	}
	return m
}

func (m *mgmtClient) do(method, path string) (*http.Response, error) {
	req, err := http.NewRequest(method, m.base+path, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(m.user, m.pass)
	resp, err := m.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%s %s: %s", method, path, resp.Status)
	}
	return resp, nil
}

func (m *mgmtClient) get(path string) (*http.Response, error) { return m.do(http.MethodGet, path) }

// closeConnection closes, on the broker side, every connection whose
// connection_name client property is name. The management API lists a new
// connection a little after it opens, so it polls for up to 20 seconds. It
// returns how many connections it closed.
func (m *mgmtClient) closeConnection(t *testing.T, name string) int {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := m.get("/api/connections")
		if err != nil {
			t.Fatalf("list connections: %v", err)
		}
		var conns []struct {
			Name             string         `json:"name"`
			ClientProperties map[string]any `json:"client_properties"`
		}
		err = json.NewDecoder(resp.Body).Decode(&conns)
		_ = resp.Body.Close()
		if err != nil {
			t.Fatalf("decode connections: %v", err)
		}
		closed := 0
		for _, c := range conns {
			if c.ClientProperties["connection_name"] != name {
				continue
			}
			del, err := m.do(http.MethodDelete, "/api/connections/"+url.PathEscape(c.Name))
			if err != nil {
				t.Fatalf("close connection %q: %v", c.Name, err)
			}
			_ = del.Body.Close()
			closed++
		}
		if closed > 0 {
			return closed
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("connection %q never showed up in the management API", name)
	return 0
}

// connName returns a unique connection name and the client properties that
// advertise it.
func connName(prefix string) (string, amqp.Table) {
	name := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	return name, amqp.Table{"connection_name": name}
}
