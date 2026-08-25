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

package kubeconn

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

// deadHost answers nothing, and does so at once: a literal address, so nothing here resolves a
// name. **No test may dial a name that leaves the machine** — a DNS lookup is slow, and on a
// network with a wildcard resolver it answers, which turns "unreachable" into whatever that
// host is.
const deadHost = "http://127.0.0.1:1"

// apiVersions is what a Kubernetes API server answers at /api.
const apiVersions = `{"kind":"APIVersions","versions":["v1"]}`

// serveAPI is a server that answers /api like a Kubernetes API server and 404s everything else.
func serveAPI(t *testing.T) *httptest.Server {
	t.Helper()
	return serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(apiVersions))
	}))
}

// serve runs h over TLS for the life of the test.
func serve(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// restConfigFor is a config that trusts srv's certificate and nothing else.
func restConfigFor(srv *httptest.Server) *rest.Config {
	return &rest.Config{Host: srv.URL, TLSClientConfig: rest.TLSClientConfig{CAData: caPEM(srv)}}
}

// restConfigForHost is a config aimed at a bare address, trusting nothing.
func restConfigForHost(host string) *rest.Config {
	return &rest.Config{Host: host}
}

// caPEM is srv's self-signed certificate, which is its own CA.
func caPEM(srv *httptest.Server) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}

// connTo builds a connection to srv, trusting its certificate.
func connTo(t *testing.T, srv *httptest.Server) *Connection {
	t.Helper()
	conn, err := newConnection(restConfigFor(srv))
	require.NoError(t, err)
	return conn
}
