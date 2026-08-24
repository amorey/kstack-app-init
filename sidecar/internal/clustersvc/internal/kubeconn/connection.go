// Copyright 2026 The Kstack Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// The connection a holder will talk to one server over. **Nothing builds one yet** — the type
// and the error are the seam the probe fills, so a holder that asks gets a clear answer rather
// than a nil it has to interpret.
package kubeconn

import (
	"errors"
	"net/http"
	"net/url"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// ErrNoConnection reports that there is no connection to hand out. Every claim reports it today;
// once the probe lands it means a context that resolves to nothing.
var ErrNoConnection = errors.New("no connection for kube-context")

// Connection is one identity and the clients built over the credentials reaching it. The clients
// share one http.Client, so they share one connection pool — with HTTP/2 that is a single TCP
// connection carrying every concurrent request to that API server.
//
// Read-only, and it never changes after it is built. A caller that mutates Config is editing
// what every other holder is using.
type Connection struct {
	// Identity is what the probe that validated this connection read, stable for its life: a
	// probe reading anything different retires it. So a holder that knows which cluster it
	// means compares before it reads. Nothing here can compare — a context resolving to the
	// same credentials says nothing about which cluster is behind them.
	Identity Identity
	// Config is what the clients below were built from. Exposed for the client-go
	// constructors that take one and build their own transport (exec, port-forward);
	// anything that can use Dynamic or HTTPClient should.
	Config *rest.Config
	// BaseURL is Config.Host resolved to an absolute URL, carrying the scheme and any path
	// prefix; APIPath is the versioned path beside it. Raw paths join onto them.
	BaseURL *url.URL
	APIPath string
	// HTTPClient is authenticated and pooled. Raw API paths go through it directly, and
	// unthrottled: the QPS bucket lives in rest.RESTClient, so it reaches Dynamic alone.
	HTTPClient *http.Client
	Dynamic    dynamic.Interface

	// done closes when this connection is retired. Nil in a connection nobody built, which
	// reads as never retired.
	done chan struct{}
}

// Done closes when a probe reads a different Identity and this connection is retired.
//
// For the holder nothing else reaches: a long-lived stream is blocked in a read, so a field it
// is not re-reading cannot tell it that what it derived here no longer holds. A caller that asks
// per operation re-calls Lease.Conn instead.
func (c *Connection) Done() <-chan struct{} { return c.done }
