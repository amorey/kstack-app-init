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

package clustersvc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/testutil"
)

// newTestService builds a service over a temp dir, closed on cleanup.
func newTestService(t *testing.T) Service {
	t.Helper()
	svc, err := New(filepath.Join(t.TempDir(), "data"), filepath.Join(t.TempDir(), "config"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, svc.Close()) })
	return svc
}

// New is the first thing the composition root builds, so nothing else has made
// dataDir yet — it has to create it rather than assume it.
func TestNewCreatesDataDir(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "nested", "data")
	svc, err := New(dataDir, "", nil)
	require.NoError(t, err)
	defer func() { assert.NoError(t, svc.Close()) }()

	assert.FileExists(t, filepath.Join(dataDir, "beehive.db"))
}

// A dataDir that cannot be created is a startup failure, not a service that limps
// along writing nowhere.
func TestNewRejectsAnUnusableDataDir(t *testing.T) {
	// A regular file where the directory should go: MkdirAll fails on it.
	blocked := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(blocked, nil, 0o600))

	_, err := New(filepath.Join(blocked, "data"), "", nil)
	assert.Error(t, err)
}

// The accessors are stateless views onto the one service, so each returns something
// usable and none of them is the nil interface.
func TestFamilyAccessors(t *testing.T) {
	svc := newTestService(t)

	assert.NotNil(t, svc.Clusters())
	assert.NotNil(t, svc.Caches())
	assert.NotNil(t, svc.CachedCatalogs())
	assert.NotNil(t, svc.CachedResources())
	assert.NotNil(t, svc.CachedData())
}

// The full lifecycle the composition root drives: Start, the stop func it returns,
// then Close. Anything left running here would outlive the process's drain.
func TestStartStopClose(t *testing.T) {
	svc, err := New(filepath.Join(t.TempDir(), "data"), filepath.Join(t.TempDir(), "config"), nil)
	require.NoError(t, err)

	stop, err := svc.Start(context.Background())
	require.NoError(t, err)

	testutil.WaitReturn(t, func() { assert.NoError(t, stop(context.Background())) }, "stop to return")

	assert.NoError(t, svc.Close())
}

// stubStartCloser records each phase it goes through into a shared log, and refuses to
// start when startErr is set.
type stubStartCloser struct {
	name     string
	startErr error
	log      *[]string
}

func (s *stubStartCloser) Start(context.Context) (func(context.Context) error, error) {
	if s.startErr != nil {
		return nil, s.startErr
	}
	*s.log = append(*s.log, "start:"+s.name)
	return func(context.Context) error {
		*s.log = append(*s.log, "stop:"+s.name)
		return nil
	}, nil
}

func (s *stubStartCloser) Close() error {
	*s.log = append(*s.log, "close:"+s.name)
	return nil
}

func TestStartAllStartsInOrderAndStopsInReverse(t *testing.T) {
	var log []string
	ls := []startCloser{
		&stubStartCloser{name: "a", log: &log},
		&stubStartCloser{name: "b", log: &log},
	}

	stop, err := startAll(context.Background(), ls)
	require.NoError(t, err)
	require.NoError(t, stop(context.Background()))

	assert.Equal(t, []string{"start:a", "start:b", "stop:b", "stop:a"}, log)
}

// A failed start leaves the caller no stop func, so whatever already runs is
// unreachable — startAll has to take it down itself, and must not start what comes
// after.
func TestStartAllUnwindsWhenOneFails(t *testing.T) {
	boom := errors.New("boom")
	var log []string
	ls := []startCloser{
		&stubStartCloser{name: "a", log: &log},
		&stubStartCloser{name: "b", startErr: boom, log: &log},
		&stubStartCloser{name: "c", log: &log},
	}

	stop, err := startAll(context.Background(), ls)

	assert.Nil(t, stop)
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, []string{"start:a", "stop:a"}, log)
}

// stopWaiter's stop blocks until release is closed, so a test can tell a stop that
// really drained from one that gave up on a dead context.
type stopWaiter struct {
	entered chan struct{}
	release chan struct{}
}

func (s *stopWaiter) Start(context.Context) (func(context.Context) error, error) {
	return func(ctx context.Context) error {
		close(s.entered)
		return drain.WithContext(ctx, func() { <-s.release })
	}, nil
}

func (s *stopWaiter) Close() error { return nil }

// A start most often fails because the startup context died. Unwinding on that same
// context would signal the already-started parts and report them drained without
// waiting, leaving them running while the caller is free to close what they use.
func TestStartAllUnwindDoesNotInheritADeadContext(t *testing.T) {
	boom := errors.New("boom")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	waiter := &stopWaiter{entered: make(chan struct{}), release: make(chan struct{})}
	var log []string
	ls := []startCloser{waiter, &stubStartCloser{name: "b", startErr: boom, log: &log}}

	errs := make(chan error, 1)
	go func() {
		_, err := startAll(ctx, ls)
		errs <- err
	}()

	testutil.Wait(t, waiter.entered, "the unwind to reach the started part's stop")
	close(waiter.release)

	err := testutil.Recv(t, errs, "startAll to return")
	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, context.Canceled, "the unwind must wait, not adopt the dead startup context")
}

func TestCloseAllRunsInReverse(t *testing.T) {
	var log []string
	ls := []startCloser{
		&stubStartCloser{name: "a", log: &log},
		&stubStartCloser{name: "b", log: &log},
	}

	require.NoError(t, closeAll(ls))
	assert.Equal(t, []string{"close:b", "close:a"}, log)
}

// Stopping in reverse of registration order is what takes a child's machinery down
// before its owner's.
func TestStopAllRunsInReverse(t *testing.T) {
	var order []int
	stops := make([]func(context.Context) error, 3)
	for i := range stops {
		stops[i] = func(context.Context) error {
			order = append(order, i)
			return nil
		}
	}

	require.NoError(t, stopAll(context.Background(), stops))
	assert.Equal(t, []int{2, 1, 0}, order)
}

// One failing stop must not skip the rest: a controller left running because an
// earlier one errored would outlive the service that owns it.
func TestStopAllRunsEveryStopAndJoinsErrors(t *testing.T) {
	first := errors.New("first")
	last := errors.New("last")

	var ran int
	stops := []func(context.Context) error{
		func(context.Context) error { ran++; return first },
		func(context.Context) error { ran++; return nil },
		func(context.Context) error { ran++; return last },
	}

	err := stopAll(context.Background(), stops)
	assert.Equal(t, 3, ran)
	assert.ErrorIs(t, err, first)
	assert.ErrorIs(t, err, last)
}

func TestStopAllWithNoStops(t *testing.T) {
	assert.NoError(t, stopAll(context.Background(), nil))
}

// A dataDir that exists but cannot hold the store is a startup failure, not a
// service running against a database it never opened.
func TestNewRejectsAnUnopenableStore(t *testing.T) {
	dataDir := t.TempDir()
	// A directory where the database file goes: sqlite cannot open it.
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, "beehive.db"), 0o700))

	_, err := New(dataDir, "", nil)
	assert.ErrorContains(t, err, "open beehive store")
}

// beehive rejects a second controller for a kind, and that error has to reach New's
// caller rather than leaving a kind silently unreconciled.
func TestRegisterControllersRejectsADuplicateKind(t *testing.T) {
	bh := newTestBeehive(t)
	path := filepath.Join(t.TempDir(), "config")

	_, _, err := registerControllers(bh, deps{}, path)
	require.NoError(t, err)

	_, _, err = registerControllers(bh, deps{}, path)
	assert.Error(t, err)
}

// Start is not re-entrant: beehive refuses a second start, and the failure must
// surface rather than half-starting the controllers behind it.
func TestStartRejectsASecondStart(t *testing.T) {
	svc := newTestService(t)
	stop, err := svc.Start(context.Background())
	require.NoError(t, err)
	defer func() { assert.NoError(t, stop(context.Background())) }()

	_, err = svc.Start(context.Background())
	assert.ErrorContains(t, err, "start beehive")
}

// beehive starts before the controllers, so a controller that fails to start leaves
// it running with no stop func to reach it. Start has to drain it on the way out.
func TestStartDrainsBeehiveWhenAControllerFails(t *testing.T) {
	boom := errors.New("boom")
	svc := newTestService(t).(*service)
	var log []string
	// Keep beehive at the head, where New puts it, and fail the part after it.
	svc.parts = []startCloser{svc.parts[0], &stubStartCloser{name: "a", startErr: boom, log: &log}}

	_, err := svc.Start(context.Background())
	require.ErrorIs(t, err, boom)

	// beehive refuses to restart once stopped, which is how a drained instance is
	// told apart from one still running.
	_, err = svc.Start(context.Background())
	assert.ErrorContains(t, err, "already stopped")
}

// The rebuild's remaining surface, called through the boundary the resolvers use.
// Every entry must be deleted as its method lands: a stub that stops panicking fails
// here, which is what keeps this list honest about what is left.
func TestUnimplementedBoundaryPanics(t *testing.T) {
	svc := newTestService(t)
	ctx := context.Background()
	var id ObjectID = 1

	calls := map[string]func(){
		"Clusters().WatchSchedule":       func() { svc.Clusters().WatchSchedule(ctx, id) },
		"Caches().WatchByCluster":        func() { svc.Caches().WatchByCluster(ctx, id) },
		"Caches().WatchStats":            func() { svc.Caches().WatchStats(ctx, id, id) },
		"Caches().WatchHealth":           func() { svc.Caches().WatchHealth(ctx) },
		"Caches().Clear":                 func() { svc.Caches().Clear(ctx, id) },
		"CachedCatalogs().Get":           func() { svc.CachedCatalogs().Get(ctx, id) },
		"CachedCatalogs().List":          func() { svc.CachedCatalogs().List(ctx) },
		"CachedCatalogs().Watch":         func() { svc.CachedCatalogs().Watch(ctx, id) },
		"CachedCatalogs().WatchList":     func() { svc.CachedCatalogs().WatchList(ctx) },
		"CachedCatalogs().ListByCache":   func() { svc.CachedCatalogs().ListByCache(ctx, id) },
		"CachedCatalogs().WatchByCache":  func() { svc.CachedCatalogs().WatchByCache(ctx, id) },
		"CachedResources().Get":          func() { svc.CachedResources().Get(ctx, id) },
		"CachedResources().List":         func() { svc.CachedResources().List(ctx) },
		"CachedResources().Watch":        func() { svc.CachedResources().Watch(ctx, id) },
		"CachedResources().WatchList":    func() { svc.CachedResources().WatchList(ctx) },
		"CachedResources().ListByCache":  func() { svc.CachedResources().ListByCache(ctx, id) },
		"CachedResources().WatchByCache": func() { svc.CachedResources().WatchByCache(ctx, id) },
		"CachedResources().Clear":        func() { svc.CachedResources().Clear(ctx, id) },
		"CachedData().ListKinds":         func() { svc.CachedData().ListKinds(ctx, id, id) },
		"CachedData().WatchKinds":        func() { svc.CachedData().WatchKinds(ctx, id, id) },
		"CachedData().WatchObjects":      func() { svc.CachedData().WatchObjects(ctx, id, id, "v1", "pods") },
		"CachedData().WatchEvents":       func() { svc.CachedData().WatchEvents(ctx, id, id) },
		"GetConnection":                  func() { svc.GetConnection(id) },
		"RetryConnection":                func() { svc.RetryConnection(ctx, id) },
		"ListEvents":                     func() { svc.ListEvents(ctx, id, nil, nil) },
		"WatchEvents":                    func() { svc.WatchEvents(ctx, id, nil) },
	}

	for name, call := range calls {
		t.Run(name, func(t *testing.T) { assert.Panics(t, call) })
	}
}
