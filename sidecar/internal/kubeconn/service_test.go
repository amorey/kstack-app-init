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
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

func TestEntryForBuildsOneConnectionPerKey(t *testing.T) {
	svc := newTestService()
	defer svc.Close()

	first, err := svc.entryFor(&rest.Config{Host: "https://one.example"}, "key-a")
	require.NoError(t, err)
	again, err := svc.entryFor(&rest.Config{Host: "https://one.example"}, "key-a")
	require.NoError(t, err)
	other, err := svc.entryFor(&rest.Config{Host: "https://one.example"}, "key-b")
	require.NoError(t, err)

	require.Same(t, first, again, "one key must yield one connection")
	require.NotSame(t, first, other, "a new key must build its own connection")
}

func TestEntryForIsRacelessForOneKey(t *testing.T) {
	svc := newTestService()
	defer svc.Close()

	const callers = 16
	conns := make([]*Connection, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Go(func() {
			e, err := svc.entryFor(&rest.Config{Host: "https://one.example"}, "shared")
			require.NoError(t, err)
			conns[i] = e.conn
		})
	}
	wg.Wait()

	for _, conn := range conns {
		require.Same(t, conns[0], conn)
	}
}

// The trap: DefaultServerURL with a hardcoded defaultTLS turns a plain-HTTP
// port-forward into https:// and every request fails at the handshake.
func TestEntryForResolvesTheBaseURLWithoutAssumingTLS(t *testing.T) {
	svc := newTestService()
	defer svc.Close()

	plainEntry, err := svc.entryFor(&rest.Config{Host: "localhost:8080"}, "plain")
	require.NoError(t, err)
	require.Equal(t, "http://localhost:8080", plainEntry.conn.BaseURL.String())

	securedEntry, err := svc.entryFor(&rest.Config{
		Host:            "example.com",
		TLSClientConfig: rest.TLSClientConfig{Insecure: true},
	}, "secured")
	require.NoError(t, err)
	require.Equal(t, "https://example.com", securedEntry.conn.BaseURL.String())
}

func TestEntryForStampsItsOwnTuning(t *testing.T) {
	svc := newTestService()
	defer svc.Close()

	caller := &rest.Config{Host: "https://one.example"}
	e, err := svc.entryFor(caller, "key")
	require.NoError(t, err)

	require.Equal(t, defaultQPS, e.conn.Config.QPS)
	require.Equal(t, defaultBurst, e.conn.Config.Burst)
	require.Equal(t, userAgent, e.conn.Config.UserAgent)
	require.Zero(t, caller.QPS, "the caller's config must not be mutated")
}

func TestEntryForReportsABuildFailure(t *testing.T) {
	svc := newTestService()
	defer svc.Close()

	_, err := svc.entryFor(&rest.Config{
		Host:            "https://one.example",
		TLSClientConfig: rest.TLSClientConfig{CAFile: "/nonexistent/ca.crt"},
	}, "bad")
	require.Error(t, err)

	// A failed build must leave nothing behind, or the next caller inherits it.
	require.Empty(t, svc.conns)
}

func TestEntryForRejectsAnUnusableHost(t *testing.T) {
	svc := newTestService()
	defer svc.Close()

	_, err := svc.entryFor(&rest.Config{Host: "://nope"}, "bad-host")
	require.Error(t, err)
}

// Close latches: a Get racing shutdown must not repopulate the pool with a live
// transport that nothing is left to close.
func TestCloseEmptiesThePoolForGood(t *testing.T) {
	svc := newTestService()

	_, err := svc.entryFor(&rest.Config{Host: "https://one.example"}, "key")
	require.NoError(t, err)
	require.NoError(t, svc.Close())
	require.Empty(t, svc.conns)

	_, err = svc.entryFor(&rest.Config{Host: "https://one.example"}, "key")
	require.ErrorIs(t, err, ErrClosed)
	require.Empty(t, svc.conns)
	require.NoError(t, svc.Close())
}

// The dynamic client is the last thing a build makes; a failure there must leave
// nothing pooled, like any other.
func TestEntryForReportsAFailedDynamicClient(t *testing.T) {
	boom := errors.New("boom")
	svc := newTestService()
	defer svc.Close()
	svc.newDynamic = func(*rest.Config, *http.Client) (dynamic.Interface, error) { return nil, boom }

	_, err := svc.entryFor(&rest.Config{Host: "https://one.example"}, "key")
	require.ErrorIs(t, err, boom)
	require.Empty(t, svc.conns)
}

// Close cannot wait for a build it cannot see — the build runs outside the pool's lock, so
// an entry mid-build has no transport for Close to drop. A build that finishes afterwards
// must therefore refuse its caller rather than hand back a connection nothing will close.
func TestEntryForRefusesABuildThatFinishesAfterClose(t *testing.T) {
	svc := newTestService()

	building := make(chan struct{})
	release := make(chan struct{})
	svc.newDynamic = func(cfg *rest.Config, c *http.Client) (dynamic.Interface, error) {
		close(building)
		<-release
		return dynamic.NewForConfigAndClient(cfg, c)
	}

	got := make(chan error, 1)
	go func() {
		_, err := svc.entryFor(&rest.Config{Host: "https://one.example"}, "key-a")
		got <- err
	}()

	testutil.Wait(t, building, "the build to start")
	require.NoError(t, svc.Close())
	close(release)

	require.ErrorIs(t, testutil.Recv(t, got, "the build to finish"), ErrClosed)

	// And a later caller for the same key is refused outright: the entry went with Close.
	_, err := svc.entryFor(&rest.Config{Host: "https://one.example"}, "key-a")
	require.ErrorIs(t, err, ErrClosed)
}

// Close is what a caller reaches for when it never started the service, so it has to leave
// nothing running on its own. The stop func is what waits; this only has to cancel.
func TestCloseEndsLoopsWithoutTheStopFunc(t *testing.T) {
	svc := New(testBudget)
	probed := testutil.NewProbe[struct{}](8)
	svc.probe = func(context.Context, *Connection) (Identity, error) {
		probed.Fire(struct{}{})
		return Identity{}, nil
	}

	lease, err := svc.Acquire(&rest.Config{Host: "https://one.example"}, "key-a")
	require.NoError(t, err)
	defer lease.Release()
	probed.Await(t, "the claimed probe")

	require.NoError(t, svc.Close())

	require.Eventually(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return svc.ctx.Err() != nil
	}, testutil.Timeout, time.Millisecond)
	testutil.NoRecv(t, probed.Chan(), 20*testCadence, "a closed pool must not keep probing")
}

// Everything Close does after the latch leaves something unusable behind, so nothing may
// see a cancelled loop context while shuttingDown still says the pool is open — a claim
// taken in that window would be answered by a loop that exits at once, onto a closed bus.
//
// Held here rather than observed from a goroutine: the latch needs this lock, so while the
// test holds it Close cannot get past it, and anything it does before the latch shows up as
// a cancellation that should not have happened yet.
func TestCloseLatchesShutdownBeforeCancelling(t *testing.T) {
	svc := New(testBudget)
	svc.probe = func(context.Context, *Connection) (Identity, error) { return Identity{}, nil }

	svc.mu.Lock()
	closed := make(chan error, 1)
	go func() { closed <- svc.Close() }()

	// A negative assertion, so it needs a bounded window of its own.
	select {
	case <-svc.ctx.Done():
		svc.mu.Unlock()
		t.Fatal("the loops were cancelled before shutdown was latched")
	case <-time.After(20 * testCadence):
	}
	svc.mu.Unlock()

	require.NoError(t, testutil.Recv(t, closed, "Close to finish"))
	require.ErrorIs(t, svc.ProbeNow(&rest.Config{Host: "https://one.example"}, "key-a"), ErrClosed)
}

// Close reads e.conn under the lock; the build writes it from another goroutine. The Once
// orders the build against its own waiters, not against Close — so the two must meet on the
// service's mutex. Overlapped deliberately: -race is what fails if they do not.
func TestCloseIsRaceFreeAgainstAnInFlightBuild(t *testing.T) {
	svc := newTestService()

	building := make(chan struct{})
	release := make(chan struct{})
	svc.newDynamic = func(cfg *rest.Config, c *http.Client) (dynamic.Interface, error) {
		close(building)
		<-release
		return dynamic.NewForConfigAndClient(cfg, c)
	}

	built := make(chan struct{})
	go func() {
		defer close(built)
		_, _ = svc.entryFor(&rest.Config{Host: "https://one.example"}, "key-a")
	}()
	testutil.Wait(t, building, "the build to start")

	go close(release)
	require.NoError(t, svc.Close())
	testutil.Wait(t, built, "the build to return")
}

// A Budget that names no concurrency would otherwise wedge every probe: probeOnce sends and
// receives on the slots channel from one goroutine, which an unbuffered one cannot satisfy.
func TestNewClampsConcurrency(t *testing.T) {
	svc := New(Budget{Cadence: testCadence, Timeout: testCadence})
	t.Cleanup(func() { assert.NoError(t, svc.Close()) })

	probed := testutil.NewProbe[struct{}](8)
	svc.probe = func(context.Context, *Connection) (Identity, error) {
		probed.Fire(struct{}{})
		return Identity{}, nil
	}

	require.NoError(t, svc.ProbeNow(&rest.Config{Host: "https://one.example"}, "key-a"))
	probed.Await(t, "the probe")
}
