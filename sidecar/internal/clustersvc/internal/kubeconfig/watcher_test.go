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

package kubeconfig

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// testInterval paces the poll loop in tests, shrunk from the production cadence so
// nothing here has to outwait defaultPollInterval.
const testInterval = 5 * time.Millisecond

// newTestConfigWatcher returns an unstarted watcher over a path inside a temp dir,
// paced at interval. The path need not exist: an absent kubeconfig is an ordinary
// state.
func newTestConfigWatcher(t *testing.T, interval time.Duration) *Watcher {
	t.Helper()
	w := New(filepath.Join(t.TempDir(), "config"))
	w.interval = interval
	return w
}

// newTestWatcher returns a started watcher over a path inside a temp dir, stopped
// and closed on cleanup. The path need not exist: an absent kubeconfig is an
// ordinary state.
func newTestWatcher(t *testing.T) *Watcher {
	t.Helper()
	w := newTestConfigWatcher(t, testInterval)
	stop, err := w.Start(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		assert.NoError(t, stop(context.Background()))
		assert.NoError(t, w.Close())
	})
	return w
}

func TestNewReadsNothing(t *testing.T) {
	// Constructing must not touch the filesystem — the machine running this has its
	// own kubeconfig, and picking it up would make every other test host-dependent.
	w := New(filepath.Join(t.TempDir(), "config"))

	cfg := w.Get()
	require.NotNil(t, cfg, "Get before Start")
	assert.Empty(t, cfg.Contexts)
	assert.Equal(t, defaultPollInterval, w.interval, "New paces itself; only tests override")
}

func TestSubscribeIsCurrentOnSubscribe(t *testing.T) {
	// The importer subscribes at startup and must not wait out a poll interval for
	// its first snapshot.
	w := New(filepath.Join(t.TempDir(), "config"))
	sub := w.Subscribe()
	defer sub.Close()

	cfg := testutil.Recv(t, sub.Chan(), "seed config")
	assert.NotNil(t, cfg)
}

func TestUnchangedConfigPublishesNothing(t *testing.T) {
	w := newTestWatcher(t)
	sub := w.Subscribe()
	defer sub.Close()

	testutil.Recv(t, sub.Chan(), "seed config")

	// A negative assertion, so there is no event to wait for: the window is a
	// multiple of the shrunk cadence, wide enough that a poll publishing
	// unconditionally would have fired several times. Fails the moment a frame
	// arrives rather than at the deadline.
	select {
	case cfg := <-sub.Chan():
		t.Fatalf("unchanged kubeconfig published %v", cfg)
	case <-time.After(10 * testInterval):
	}
}

// Stopping is not closing: a subscriber must still be able to read what the last
// poll published after the loop is joined.
func TestStopLeavesSubscriptionsOpen(t *testing.T) {
	w := newTestConfigWatcher(t, testInterval)
	sub := w.Subscribe()
	defer sub.Close()
	stop, err := w.Start(context.Background())
	require.NoError(t, err)
	testutil.Recv(t, sub.Chan(), "seed config")

	require.NoError(t, stop(context.Background()))

	// The loop is already joined, so a close would be visible on this receive right
	// now — no window to wait out.
	select {
	case _, ok := <-sub.Chan():
		assert.True(t, ok, "stop must not close the subscription")
	default:
	}

	require.NoError(t, w.Close())
	testutil.WaitClosed(t, sub.Chan(), "subscription after Close")
}

func TestCloseWithoutStart(t *testing.T) {
	// New is called in clustersvc.New and Close in service.Close, with Start only in
	// between — a failure between the two must not deadlock on a loop never launched.
	w := New(filepath.Join(t.TempDir(), "config"))
	assert.NoError(t, w.Close())
}

func TestStopJoinsPollLoop(t *testing.T) {
	// A stop that returned while the loop still ran would let a poll publish after
	// the owner considers it drained. The interval is far longer than the test, so
	// the claim is that stop selects on the stop channel rather than waiting a tick.
	w := newTestConfigWatcher(t, time.Hour)
	stop, err := w.Start(context.Background())
	require.NoError(t, err)

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "stop to return")

	assert.NoError(t, w.Close())
}
