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
	"net/url"

	amqp "github.com/rabbitmq/amqp091-go"
)

// safeURL returns the AMQP URL with any password redacted, so credentials never
// reach a log line. A URL that cannot be parsed is reported as "<redacted>".
func safeURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "<redacted>"
	}
	return u.Redacted()
}

// copyTable returns a deep copy of t, so nested tables (such as the broker's
// capabilities) are not shared with the caller. A nil table stays nil.
func copyTable(t amqp.Table) amqp.Table {
	if t == nil {
		return nil
	}
	out := make(amqp.Table, len(t))
	for k, v := range t {
		if nested, ok := v.(amqp.Table); ok {
			v = copyTable(nested)
		}
		out[k] = v
	}
	return out
}

// tableString returns the string value of key in t, or "" when it is missing
// or not a string. amqp091 decodes long strings as string and byte arrays as
// []byte; both are accepted.
func tableString(t amqp.Table, key string) string {
	switch v := t[key].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return ""
	}
}
