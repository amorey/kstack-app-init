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
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

// The trap: a hardcoded scheme turns a plain-HTTP port-forward into https:// and every request
// fails at the handshake. The scheme follows the credentials the config actually carries.
func TestNewConnectionResolvesTheBaseURLWithoutAssumingTLS(t *testing.T) {
	plain, err := newConnection(&rest.Config{Host: "localhost:8080"})
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8080", plain.BaseURL.String())

	secured, err := newConnection(&rest.Config{
		Host:            "example.com",
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
	})
	require.NoError(t, err)
	assert.Equal(t, "https://example.com", secured.BaseURL.String())
}

// The tuning is this package's, and the caller's config is not ours to edit — a caller that
// reads its own config back must find what it wrote.
func TestNewConnectionStampsItsTuningOnACopy(t *testing.T) {
	caller := &rest.Config{Host: "https://one.example"}

	conn, err := newConnection(caller)
	require.NoError(t, err)

	assert.Equal(t, defaultQPS, conn.Config.QPS)
	assert.Equal(t, userAgent, conn.Config.UserAgent)
	assert.Zero(t, caller.QPS, "the caller's config is untouched")
	assert.Empty(t, caller.UserAgent)
}

// A host that will not parse fails the build, where a nil connection handed back as usable
// would panic at the first request instead.
func TestNewConnectionRefusesAHostItCannotParse(t *testing.T) {
	_, err := newConnection(&rest.Config{Host: "://nonsense"})

	assert.Error(t, err)
}

// Retiring is what tells every holder that what it derived over this connection no longer
// holds. Idempotent because publish and Release can both reach one on the way out.
func TestRetireClosesDoneOnce(t *testing.T) {
	conn, err := newConnection(&rest.Config{Host: "https://one.example"})
	require.NoError(t, err)

	conn.retire()
	conn.retire()

	<-conn.Done()
}

// A connection nobody built has a nil channel, which blocks forever — reading as never retired,
// where a closed one would tell every holder its connection had gone.
func TestDoneOnAConnectionNobodyBuiltNeverFires(t *testing.T) {
	var c Connection

	select {
	case <-c.Done():
		t.Fatal("an unbuilt connection reported itself retired")
	default:
	}
}

func TestGetJSONDecodesTheAnswer(t *testing.T) {
	conn := connTo(t, serveAPI(t))

	var body struct {
		Versions []string `json:"versions"`
	}
	require.NoError(t, conn.getJSON(t.Context(), "/api", &body))

	assert.Equal(t, []string{"v1"}, body.Versions)
}

// The status code is the whole evidence a raw endpoint leaves, so it travels as data — a
// classifier reading it back out of a formatted string is one that breaks on a reworded message.
func TestGetJSONReportsTheStatusCode(t *testing.T) {
	conn := connTo(t, serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no", http.StatusForbidden)
	})))

	err := conn.getJSON(t.Context(), "/api", &struct{}{})

	var he *httpErr
	require.ErrorAs(t, err, &he)
	assert.Equal(t, http.StatusForbidden, he.code)
	assert.Contains(t, err.Error(), "/api")
}

// A 200 that will not decode is a proxy or captive portal answering for the API server, which
// is a different fault from either a transport failure or a status code.
func TestGetJSONReportsABodyThatWillNotDecode(t *testing.T) {
	conn := connTo(t, serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("<html>sign in to continue</html>"))
	})))

	err := conn.getJSON(t.Context(), "/api", &struct{}{})

	assert.ErrorIs(t, err, errMalformed)
}

// Nothing listening is the transport failing, and it must not read as a status.
func TestGetJSONReportsATransportFailure(t *testing.T) {
	conn, err := newConnection(restConfigForHost(deadHost))
	require.NoError(t, err)

	err = conn.getJSON(t.Context(), "/api", &struct{}{})

	require.Error(t, err)
	var he *httpErr
	assert.False(t, errors.As(err, &he), "nothing answered, so there is no status")
}

// wrapped is how every one of these actually arrives: the http client returns a *url.Error
// around whatever the transport hit, so classifying the bare error would classify nothing.
func wrapped(err error) error {
	return &url.Error{Op: "Get", URL: "https://one.example/api", Err: err}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want Reason
	}{
		{"connection refused", wrapped(&net.OpError{Err: syscall.ECONNREFUSED}), ReasonUnreachable},
		{"no such host", wrapped(&net.DNSError{IsNotFound: true}), ReasonUnreachable},
		{"untrusted CA", wrapped(x509.UnknownAuthorityError{}), ReasonTLSInvalid},
		{"wrong hostname", wrapped(x509.HostnameError{}), ReasonTLSInvalid},
		{"expired certificate", wrapped(&tls.CertificateVerificationError{}), ReasonTLSInvalid},
		{"our deadline", wrapped(context.DeadlineExceeded), ReasonTimeout},
		{"unauthorized", &httpErr{code: http.StatusUnauthorized}, ReasonUnauthorized},
		{"forbidden", &httpErr{code: http.StatusForbidden}, ReasonForbidden},
		{"throttled", &httpErr{code: http.StatusTooManyRequests}, ReasonThrottled},
		{"server fault", &httpErr{code: http.StatusInternalServerError}, ReasonInternalError},
		{"restarting", &httpErr{code: http.StatusServiceUnavailable}, ReasonServiceUnavailable},
		{"not a kube server", &httpErr{code: http.StatusNotFound}, ReasonMalformed},
		{"a proxy answered", &httpErr{code: http.StatusBadGateway}, ReasonMalformed},
		{"undecodable body", fmt.Errorf("%w: %w", errMalformed, errors.New("invalid character '<'")), ReasonMalformed},
		{"a bug here", fmt.Errorf("%w: %w", errBadRequest, errors.New("bad method")), ReasonInternal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, classify(tt.err))
		})
	}
}

// The forms above are hand-built, so one real rejection is what pins them to what the transport
// actually emits — including that crypto/tls wraps the x509 error in its own.
func TestClassifyReadsARealHostnameMismatchAsTLSInvalid(t *testing.T) {
	srv := serveAPI(t)
	// httptest's certificate covers 127.0.0.1 and example.com, so "localhost" reaches the same
	// server with a name the certificate does not carry.
	conn, err := newConnection(&rest.Config{
		Host:            strings.Replace(srv.URL, "127.0.0.1", "localhost", 1),
		TLSClientConfig: rest.TLSClientConfig{CAData: caPEM(srv)},
	})
	require.NoError(t, err)

	err = conn.getJSON(t.Context(), "/api", &struct{}{})

	require.Error(t, err)
	assert.Equal(t, ReasonTLSInvalid, classify(err))
}

// A TLS failure arrives wrapped in the same *url.Error a refused connection does, so an
// unreachable-first classifier would report every rejected certificate as a server that never
// answered — the opposite of the truth, and a different fix for the user.
func TestClassifyTellsARejectedCertificateFromAServerThatNeverAnswered(t *testing.T) {
	refused := wrapped(&net.OpError{Err: syscall.ECONNREFUSED})
	rejected := wrapped(&net.OpError{Op: "remote error", Err: x509.UnknownAuthorityError{}})

	assert.Equal(t, ReasonUnreachable, classify(refused))
	assert.Equal(t, ReasonTLSInvalid, classify(rejected))
}

func TestKeepaliveTightensTheHealthCheck(t *testing.T) {
	// t.Setenv restores on cleanup but leaves the var set, so unset it here: the
	// unset path is the one production takes.
	t.Setenv(readIdleTimeoutEnv, "")
	t.Setenv(pingTimeoutEnv, "")
	os.Unsetenv(readIdleTimeoutEnv)
	os.Unsetenv(pingTimeoutEnv)

	configureHTTP2Keepalive()

	assert.Equal(t, "10", os.Getenv(readIdleTimeoutEnv))
	assert.Equal(t, "5", os.Getenv(pingTimeoutEnv))
}

// The values stay tunable: 0 disables the health check, which is how an operator turns
// it off.
func TestKeepalivePreservesAnOverride(t *testing.T) {
	t.Setenv(readIdleTimeoutEnv, "0")
	t.Setenv(pingTimeoutEnv, "42")

	configureHTTP2Keepalive()

	assert.Equal(t, "0", os.Getenv(readIdleTimeoutEnv))
	assert.Equal(t, "42", os.Getenv(pingTimeoutEnv))
}

// Building the pool is what applies it: the vars are read when a transport is built, and this
// package is what builds them — a call the composition root has to remember is one that goes
// missing.
func TestNewConfiguresTheKeepalive(t *testing.T) {
	t.Setenv(readIdleTimeoutEnv, "")
	os.Unsetenv(readIdleTimeoutEnv)

	svc := New(resolving("prod", "key-1"))
	defer func() { assert.NoError(t, svc.Close()) }()

	assert.Equal(t, "10", os.Getenv(readIdleTimeoutEnv))
}
