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

package cluster

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// chunkedBody delivers `chunks` single-byte reads, each after `gap`, then EOF. It
// honors ctx so the idle watchdog can cancel it mid-stream.
type chunkedBody struct {
	ctx    context.Context
	chunks int
	gap    time.Duration
	served int
}

func (b *chunkedBody) Read(p []byte) (int, error) {
	if b.served >= b.chunks {
		return 0, io.EOF
	}
	select {
	case <-time.After(b.gap):
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	}
	b.served++
	p[0] = 'x'
	return 1, nil
}

func (b *chunkedBody) Close() error { return nil }

// stallBody never delivers data — Read blocks until ctx is cancelled.
type stallBody struct{ ctx context.Context }

func (b *stallBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *stallBody) Close() error { return nil }

// A body that keeps making read progress — no single gap exceeds the idle window — must
// run to completion even though its TOTAL duration is longer than the window. This is
// the property a wall-clock deadline lacks: slow but progressing is not stalled.
func TestIdleTimeoutAllowsSlowProgressingBody(t *testing.T) {
	timeout := 50 * time.Millisecond
	// TOTAL duration (160ms) deliberately exceeds the window, while each gap (20ms) stays
	// well under it — so the body only completes if every read RESETS the window. A
	// wall-clock deadline, or a wrapper that forgot to reset, would cancel it partway.
	body := &chunkedBody{chunks: 8, gap: 20 * time.Millisecond}
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body.ctx = req.Context()
		return &http.Response{StatusCode: 200, Body: body}, nil
	})
	rt := newIdleTimeoutWrapper(timeout)(base)

	req := httptest.NewRequest(http.MethodGet, "https://k8s/api/v1/pods", nil)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err, "a steadily-progressing body must not be cancelled")
	require.NoError(t, resp.Body.Close())
	require.Len(t, data, 8)
}

// A body that stops delivering (no bytes) is cancelled once the idle window elapses, so
// the request errors and its caller can release the limiter slot.
func TestIdleTimeoutCancelsStalledBody(t *testing.T) {
	body := &stallBody{}
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		body.ctx = req.Context()
		return &http.Response{StatusCode: 200, Body: body}, nil
	})
	rt := newIdleTimeoutWrapper(30 * time.Millisecond)(base)

	req := httptest.NewRequest(http.MethodGet, "https://k8s/api/v1/pods", nil)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	// Read in a goroutine so a wrapper that fails to cancel fails the test fast rather
	// than hanging the suite.
	done := make(chan error, 1)
	go func() { _, rerr := io.ReadAll(resp.Body); done <- rerr }()
	rerr := testutil.Recv(t, done, "the stalled body to be cancelled by the idle timeout")
	require.Error(t, rerr, "a body that never delivers must be cancelled by the idle timeout")
	_ = resp.Body.Close()
}

// A server that never even sends response headers (a pre-first-byte stall) is cancelled
// too — the watchdog is armed before the RoundTrip, not only around the body.
func TestIdleTimeoutCancelsBeforeHeaders(t *testing.T) {
	base := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	rt := newIdleTimeoutWrapper(30 * time.Millisecond)(base)

	req := httptest.NewRequest(http.MethodGet, "https://k8s/api/v1/pods", nil)
	// RoundTrip in a goroutine so a wrapper that never arms the watchdog before dialing
	// fails the test fast instead of deadlocking (the base blocks until cancelled).
	done := make(chan error, 1)
	go func() { _, rerr := rt.RoundTrip(req); done <- rerr }()
	rerr := testutil.Recv(t, done, "the request stalled before headers to be cancelled")
	require.Error(t, rerr, "a request stalled before headers must be cancelled")
}

// Watch requests are exempt: they're long-lived and legitimately quiet between
// bookmarks, so the wrapper must pass them straight through — no idle reader, no
// cancel context — leaving RetryWatcher and HTTP/2 keepalive to govern them.
func TestIdleTimeoutExemptsWatch(t *testing.T) {
	base := roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})
	rt := newIdleTimeoutWrapper(10 * time.Millisecond)(base)

	req := httptest.NewRequest(http.MethodGet, "https://k8s/api/v1/pods?watch=true", nil)
	resp, err := rt.RoundTrip(req)
	require.NoError(t, err)
	_, wrapped := resp.Body.(*idleReadCloser)
	require.False(t, wrapped, "a watch response must be passed through unwrapped (no idle reader)")
}

// A non-positive timeout disables the wrapper entirely — the transport is returned as-is.
func TestIdleTimeoutZeroDisabled(t *testing.T) {
	base := roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, nil })
	rt := newIdleTimeoutWrapper(0)(base)
	_, wrapped := rt.(*idleTimeoutRoundTripper)
	require.False(t, wrapped, "a non-positive timeout must return the transport unwrapped")
}
