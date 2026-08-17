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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// A claimed connection that answered is served without waiting for a fresh probe — it is
// already good, and every consumer would otherwise pay a round trip.
func TestConnServesAValidatedConnectionWithoutWaiting(t *testing.T) {
	block := make(chan struct{})
	var probes atomic.Int32
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		if probes.Add(1) > 1 {
			<-block
		}
		return Identity{ServerUID: "uid-1"}, nil
	})
	t.Cleanup(func() { close(block) })

	lease := f.acquire(t, "key-a")
	f.awaitResult(t, "key-a")

	conn, err := lease.Conn(t.Context())
	require.NoError(t, err)
	require.NotNil(t, conn)
}

// Waiting for a SUCCESSFUL result would hang a log tail against a down cluster until its
// request context died, with nothing saying why.
func TestConnReportsAProbeThatFails(t *testing.T) {
	boom := errors.New("boom")
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		return Identity{}, boom
	})

	lease := f.acquire(t, "key-a")
	_, err := lease.Conn(t.Context())
	require.ErrorIs(t, err, boom)
}

// The one that makes a claim mean anything: credentials that failed at first must be
// retried by the loop the claim arms, not answered forever with the stale failure.
func TestConnRetriesAFailedProbe(t *testing.T) {
	boom := errors.New("boom")
	var failing atomic.Bool
	failing.Store(true)
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		if failing.Load() {
			return Identity{}, boom
		}
		return Identity{ServerUID: "uid-1"}, nil
	})

	lease := f.acquire(t, "key-a")
	require.Eventually(t, func() bool {
		last := f.State("key-a").Last
		return last != nil && last.Err != nil
	}, testutil.Timeout, testCadence)

	// Polled rather than asserted once: Conn reports the next result whatever it says, and
	// a probe already in flight when this flips still fails. What is under test is that the
	// claim keeps retrying, so the connection arrives — not which attempt delivers it.
	failing.Store(false)
	var conn *Connection
	require.Eventually(t, func() bool {
		var err error
		conn, err = lease.Conn(t.Context())
		return err == nil
	}, testutil.Timeout, testCadence)
	require.NotNil(t, conn)
}

func TestConnGivesUpWithItsContext(t *testing.T) {
	block := make(chan struct{})
	f := newProbeFixture(t, func(context.Context, *Connection) (Identity, error) {
		<-block
		return Identity{}, nil
	})
	t.Cleanup(func() { close(block) })

	lease := f.acquire(t, "key-a")
	f.probes.Await(t, "the probe now parked")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := lease.Conn(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// Credentials that cannot build a transport never get an entry, so nothing is left
// half-claimed behind a connection that does not exist.
func TestAcquireReportsABuildFailure(t *testing.T) {
	boom := errors.New("boom")
	f := newProbeFixture(t, probesTo(Identity{}))
	f.newDynamic = func(*rest.Config, *http.Client) (dynamic.Interface, error) { return nil, boom }

	_, err := f.Acquire(testConfig(), "key-a")
	require.ErrorIs(t, err, boom)
	assert.False(t, f.running("key-a"))

	require.ErrorIs(t, f.ProbeNow(testConfig(), "key-a"), boom)
}

// Credentials the pool never held read as no news, which is what a caller folds as
// "keep what you have" rather than "there is nothing there".
func TestStateOfAnUnknownKeyIsNoNews(t *testing.T) {
	f := newProbeFixture(t, probesTo(Identity{}))

	assert.Equal(t, State{}, f.State("never-seen"))
}

// A failure turning into a different failure is a different answer. Comparing errors by
// presence would leave a consumer rendering the first reason for as long as the cluster
// stays broken in a new way.
func TestResultSameAsDistinguishesFailures(t *testing.T) {
	refused := &Result{Err: errors.New("dial tcp 10.0.0.1:443: connect: connection refused")}
	unauthorized := Result{Err: errors.New("/version: 401 Unauthorized")}

	assert.False(t, unauthorized.sameAs(refused), "a different failure is different news")
	assert.True(t, unauthorized.sameAs(&Result{Err: errors.New(unauthorized.Err.Error())}))
	assert.False(t, unauthorized.sameAs(&Result{}), "recovering is news")
}
