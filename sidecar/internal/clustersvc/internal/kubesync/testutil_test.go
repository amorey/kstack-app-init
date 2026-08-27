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

// Shared fixtures: the pool and the store directory a Service is built over, and the
// bodies a test substitutes for the sweep and the mirror.
package kubesync

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/amorey/gobus/watch"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
)

// fakePool stands in for the connection pool: one lease per context, handed back to
// every claim so a test can vouch for an identity before or after a session takes one.
type fakePool struct {
	mu     sync.Mutex
	leases map[string]*fakeLease
	hub    *watch.Hub[string, kubeconn.State]
}

func newFakePool() *fakePool {
	return &fakePool{leases: map[string]*fakeLease{}, hub: watch.New[string, kubeconn.State]()}
}

func (p *fakePool) Acquire(contextName string) kubeconn.Lease {
	l := p.lease(contextName)
	l.mu.Lock()
	l.claims++
	l.mu.Unlock()
	return l
}

// lease returns the claim handed out for a context, building it if nothing has claimed
// one yet — so a test can vouch for an identity before the session arms.
func (p *fakePool) lease(contextName string) *fakeLease {
	p.mu.Lock()
	defer p.mu.Unlock()
	l, ok := p.leases[contextName]
	if !ok {
		l = &fakeLease{contextName: contextName, hub: p.hub}
		p.leases[contextName] = l
	}
	return l
}

// fakeLease answers ConnFor from whatever the test vouched for, and publishes on the
// same hub kubeconn does, so ReadyFor's attach-then-check ordering is exercised for real.
type fakeLease struct {
	contextName string
	hub         *watch.Hub[string, kubeconn.State]

	mu       sync.Mutex
	claims   int
	released int
	uid      string
	conn     *kubeconn.Connection
}

// vouch makes this lease answer for serverUID, and wakes whoever is waiting on it.
func (l *fakeLease) vouch(serverUID string) {
	l.mu.Lock()
	l.uid, l.conn = serverUID, &kubeconn.Connection{}
	l.mu.Unlock()
	_ = l.hub.Sender().Send(l.contextName, kubeconn.State{})
}

func (l *fakeLease) held() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.claims - l.released
}

func (l *fakeLease) Conn(context.Context) (*kubeconn.Connection, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil {
		return nil, kubeconn.ErrNoConnection
	}
	return l.conn, nil
}

func (l *fakeLease) ConnFor(_ context.Context, serverUID string) (*kubeconn.Connection, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn == nil || l.uid != serverUID {
		return nil, kubeconn.ErrIdentityMismatch
	}
	return l.conn, nil
}

func (l *fakeLease) State() kubeconn.State { return kubeconn.State{} }

func (l *fakeLease) WatchState() kubeconn.StateSubscription { return l.hub.Watch(l.contextName) }

func (l *fakeLease) Departed() bool { return false }

func (l *fakeLease) Release() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released++
}

// newTestService builds a Service over a fresh pool and a store directory under t, with
// bodies that park until their run ends — the shape a real body has, so nothing here
// re-enters and the tests below drive arming rather than sync.
func newTestService(t *testing.T, opts ...option) (*Service, *fakePool) {
	t.Helper()
	pool := newFakePool()
	mgr := kubestore.NewManager(t.TempDir())
	t.Cleanup(func() { _ = mgr.Close() })

	base := []option{
		withDiscoveryBody(func(ctx context.Context, _ discoveryRun) { <-ctx.Done() }),
		withKindBody(func(ctx context.Context, _ kindRun) { <-ctx.Done() }),
	}
	svc := New(pool, mgr, append(base, opts...)...)
	t.Cleanup(func() { _ = svc.Close() })
	return svc, pool
}

// quietWindow bounds a negative assertion — "no news arrived" — which has no event to
// wait for and so needs a window of its own rather than the failsafe every positive wait
// uses. Short: what it is watching for would already have happened.
const quietWindow = 50 * time.Millisecond

// testKind is one kind's identity, spelled the way the store writes rows by.
func testKind(apiVersion, kind, resource string) kubestore.Kind {
	return kubestore.Kind{APIVersion: apiVersion, Kind: kind, Resource: resource}
}
