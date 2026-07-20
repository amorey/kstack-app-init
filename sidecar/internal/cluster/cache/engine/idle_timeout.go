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

package engine

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/transport"
)

// defaultListIdleTimeout is the read-inactivity window for a resync request (a LIST
// page, the metadata list, or a GET). It is NOT a wall-clock deadline: a request is
// cancelled only when it makes NO read progress for this long — response headers, and
// then every chunk of the body, count as progress. So a slow or large LIST that keeps
// streaming completes no matter how long it takes in total (a consistently slow API
// server, an expensive/huge kind), while a genuinely wedged request — nothing arriving
// for the window — is cancelled so its driver errors, releases its shared listLimiter
// slot (see listConcurrency), and Run backs off rather than one hung kind starving
// every other driver's resync. Generous enough to cover an expensive query's
// time-to-first-byte; only a real stall should hit it. Detection is coarse: the
// watchdog ticks once per window and cancels only after a full tick with no progress,
// so the effective idle-to-cancel time is in [timeout, 2*timeout] — fine for a stall
// backstop.
const defaultListIdleTimeout = 2 * time.Minute

// newIdleTimeoutWrapper returns a transport.WrapperFunc that installs an idle-read
// timeout on a rest client's requests (see idleTimeoutRoundTripper). A non-positive
// timeout returns the transport unwrapped (a test/disabled seam).
func newIdleTimeoutWrapper(timeout time.Duration) transport.WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		if timeout <= 0 {
			return rt
		}
		return &idleTimeoutRoundTripper{base: rt, timeout: timeout}
	}
}

// idleTimeoutRoundTripper cancels a request whose response makes no read progress for
// `timeout`. Unlike a wall-clock deadline it never cancels a slow-but-progressing
// transfer: the arming covers time-to-first-byte (a server that never sends headers),
// then each body read counts as progress, so only a true stall trips it. WATCH requests
// are left untouched — they are long-lived and legitimately quiet between the server's
// periodic bookmarks, so an inactivity bound would wrongly kill a healthy watch.
type idleTimeoutRoundTripper struct {
	base    http.RoundTripper
	timeout time.Duration
}

func (t *idleTimeoutRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Substring-match the raw query rather than parsing it: this runs on every
	// non-watch LIST page and per-object GET in a resync, so parsing the whole
	// query string + allocating a url.Values map just to read one param would be
	// wasted work on a hot path.
	if strings.Contains(req.URL.RawQuery, "watch=true") {
		return t.base.RoundTrip(req)
	}
	// A private cancellable context scoped to THIS request: the watchdog cancels only
	// this request on a stall, never the caller's run context. Armed before dialing so a
	// server that hangs before sending headers is cancelled too.
	ctx, cancel := context.WithCancel(req.Context())
	w := newIdleWatchdog(t.timeout, cancel)
	resp, err := t.base.RoundTrip(req.WithContext(ctx))
	if err != nil {
		w.stopAndCancel()
		return nil, err
	}
	if resp.Body == nil {
		w.stopAndCancel()
		return resp, nil
	}
	w.progress() // headers arrived
	resp.Body = &idleReadCloser{body: resp.Body, w: w}
	return resp, nil
}

// idleWatchdog cancels a request that makes no read progress for a full timeout window.
// Progress (a header arrival or a body chunk) just bumps an atomic counter; a
// self-rescheduling AfterFunc timer cancels only when the counter hasn't advanced since
// its previous tick. Because the timer re-arms itself from inside its own callback —
// never from the read path — it sidesteps AfterFunc's Reset-vs-fire hazard: a read that
// lands exactly as the timer fires merely bumps the counter, so the next tick observes
// the progress and re-arms rather than a Reset racing the firing cancel and killing a
// live transfer.
//
// mu closes the one remaining check-then-act window between tick and stop: without it a
// tick that read stopped==false could still reach its Reset AFTER stop() ran, re-arming
// a timer the watchdog was just retired — a timer that outlives its stop. Holding mu
// across the stopped check + Reset in tick, and across the stopped-set + Stop in stop,
// makes the two mutually exclusive: a tick either re-arms before stop and stop's Stop
// then cancels that timer, or observes stopped and never re-arms.
type idleWatchdog struct {
	cancel  context.CancelFunc
	timeout time.Duration
	reads   atomic.Uint64 // bumped on every progress event
	mu      sync.Mutex    // guards stopped + timer.Reset/Stop against a tick/stop race
	stopped bool          // guarded by mu
	timer   *time.Timer
	seen    uint64 // reads value at the previous tick; touched only in tick (serial)
}

func newIdleWatchdog(timeout time.Duration, cancel context.CancelFunc) *idleWatchdog {
	w := &idleWatchdog{cancel: cancel, timeout: timeout}
	w.timer = time.AfterFunc(timeout, w.tick)
	return w
}

// progress records read activity — a header arrival or a body chunk.
func (w *idleWatchdog) progress() { w.reads.Add(1) }

// tick runs once per window in the timer goroutine. If no progress happened since the
// last tick, the request has been idle for a full window — cancel it. Otherwise re-arm.
func (w *idleWatchdog) tick() {
	n := w.reads.Load()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return // stopped between the last tick and now — don't re-arm
	}
	if n == w.seen {
		w.cancel()
		return
	}
	w.seen = n
	w.timer.Reset(w.timeout) // safe under mu: a concurrent stop can't slip in past the check
}

// stop retires the watchdog so it won't fire (or re-arm) after the body is closed.
func (w *idleWatchdog) stop() {
	w.mu.Lock()
	w.stopped = true
	w.mu.Unlock()
	w.timer.Stop() // cancels an armed timer, including one a racing tick just re-armed
}

// stopAndCancel retires the watchdog and releases its request context in one step — the
// paired "we're done with this request" operation, so no exit path can stop the watchdog
// without also cancelling (a context leak). cancel is idempotent, so calling it after a
// stall-fire already cancelled is harmless.
func (w *idleWatchdog) stopAndCancel() {
	w.stop()
	w.cancel()
}

// idleReadCloser reports read progress to the watchdog and retires it on Close. cancel
// is idempotent, so a stall-fire that races Close is harmless — the request context is
// already cancelled.
type idleReadCloser struct {
	body io.ReadCloser
	w    *idleWatchdog
}

func (r *idleReadCloser) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if n > 0 {
		r.w.progress() // progress — a stall tick won't cancel a streaming transfer
	}
	return n, err
}

func (r *idleReadCloser) Close() error {
	r.w.stopAndCancel()
	return r.body.Close()
}
