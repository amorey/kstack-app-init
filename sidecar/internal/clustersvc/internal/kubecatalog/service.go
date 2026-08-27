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

// Package kubecatalog discovers the kinds each tracked cluster serves — the sweep behind
// every ClusterCachedCatalog — on its own probe engine, off every reconcile goroutine.
//
// Subjects are opaque ids the caller supplies (clustersvc uses the catalog's beehive
// name, so the change signal doubles as the requeue), each bound at Track to the
// kube-context whose connection the sweep borrows. Arming is the caller's policy, not
// interest — Track and Forget mirror the catalog record's own state — which is why this
// is not a refcounted lease pool like kubeconn: a reader must never re-arm a sweep the
// user paused. Only a run commits an answer; the pull cadence is the correctness bound,
// and the wake layers (the connection bridge, and a watch on the CRDs and APIServices that
// change what a cluster serves) only make it prompt.
// → docs/adr/2026-08-26-kubecatalog-watch.md.
package kubecatalog

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/amorey/gobus/conflate"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// connService is the pool the sweeps borrow connections from: a lease per tracked
// subject, and the fleet feed that says a context's connection moved.
type connService interface {
	Acquire(contextName string) kubeconn.Lease
	Subscribe() kubeconn.Subscription
}

// storeManager is the cache directory as a sweep reaches it: one claim per run, given
// back when it ends. Leaf-to-leaf, the way the kubeconn import is — the sweep writes
// native rows and knows nothing of the records above.
type storeManager interface {
	OpenOrCreate(cacheID int64) (*kubestore.Store, error)
}

// Subscription reports the ids whose news changed — the answer or the verdict, never
// timing — for the trigger that requeues each one's record. A keyed, coalescing bus, so
// a fleet sweeping at once neither loses an id behind a busier one nor overflows a
// buffer. The value carries nothing; the key is the news.
type Subscription = *conflate.Receiver[string, struct{}]

// Observation is the sweep's standing answer for one id, beside the attempts that
// account for it.
type Observation = probe.Observation[Catalog]

// Service runs the sweep over the tracked ids.
type Service struct {
	conns  connService
	stores storeManager
	engine *probe.Engine
	// signalHub names the ids whose news changed, fed by publish.
	signalHub *conflate.Hub[string, struct{}]

	// mu guards tracked, byContext, and published together: what publish announces is
	// measured against who is still tracked, and the bridge fans a context out to the
	// ids over it.
	mu      sync.Mutex
	tracked map[string]*subject
	// byContext is tracked reversed — the ids sweeping over each context — for the
	// bridge's fan-out.
	byContext map[string]map[string]struct{}
	// published is the news each id's last signal carried, compared against so a pass
	// that moved only timing wakes nobody.
	published map[string]news
	// watchers holds one standing watch per tracked id. Beside tracked, under the same mutex,
	// because establishing one has to be measured against whether the id is still tracked and
	// the two must not be read separately — see ensureWatcher.
	watchers map[string]*watcher
	// watcherCtx bounds every watcher and is cancelled by Close. Not Start's context, which
	// bounds startup, and not a run's, which ends with the pass that established the watcher.
	watcherCtx   context.Context
	stopWatchers context.CancelFunc
	// open is how a watcher reaches the API server, a seam a test substitutes.
	open opener

	wg sync.WaitGroup
}

// Params is what one subject sweeps: over which context, as which server, into which
// cache's store.
//
// **A context is not an identity.** It can be re-pointed at another cluster, and the
// pool hands out whatever now answers, so the server the caller armed this subject for
// is the thing that makes a sweep's answer belong to it.
type Params struct {
	// CacheID names the store the sweep writes. The subject id cannot carry it: the
	// record's name embeds the catalog's object id, not the cache's.
	CacheID     int64
	ContextName string
	ServerUID   string
}

// subject is one tracked id: what it sweeps, and this service's own claim on the
// connection — refcounted in the pool beside every other holder's, so releasing it never
// stops the cluster being probed.
type subject struct {
	params Params
	lease  kubeconn.Lease
}

// New returns a Service sweeping over conns' connections, into stores' files.
func New(conns connService, stores storeManager) *Service {
	return newWithOptions(conns, stores)
}

// option is a test seam, reachable only from white-box tests.
type option func(*Service, *catalogProbe)

// withSweep substitutes the API server behind every sweep.
func withSweep(f func(context.Context, *kubeconn.Connection) (sweep, error)) option {
	return func(_ *Service, p *catalogProbe) { p.sweep = f }
}

// withOpener substitutes the API server behind every watch.
func withOpener(f opener) option {
	return func(s *Service, _ *catalogProbe) { s.open = f }
}

// newWithOptions is New plus the seams.
func newWithOptions(conns connService, stores storeManager, opts ...option) *Service {
	watcherCtx, stopWatchers := context.WithCancel(context.Background())
	s := &Service{
		conns:     conns,
		stores:    stores,
		engine:    probe.New(),
		signalHub: conflate.New[string, struct{}](),
		tracked:   map[string]*subject{},
		byContext: map[string]map[string]struct{}{},
		published: map[string]news{},
		watchers:  map[string]*watcher{},

		watcherCtx:   watcherCtx,
		stopWatchers: stopWatchers,
		open:         openCollectionWatch,
	}

	p := &catalogProbe{
		conn:    s.connFor,
		sweep:   discoverServedKinds,
		mirror:  s.mirror,
		watch:   s.ensureWatcher,
		unwatch: s.stopWatcher,
	}
	for _, opt := range opts {
		opt(s, p)
	}
	probe.Register(s.engine, nameCatalog, p,
		probe.WithInterval(sweepInterval),
		probe.WithTimeout(sweepTimeout),
		probe.WithBackoff(sweepRetryBase, 2, sweepIntervalDegraded))
	s.engine.OnPass(s.publish)
	return s
}

// Track arms the sweep for id with p. Idempotent, and every field of p is fixed for the
// id's life — the caller derives them from one record, whose identity does not change —
// so a repeat is a no-op, never a re-bind.
func (s *Service) Track(id string, p Params) {
	s.mu.Lock()
	if _, held := s.tracked[id]; held {
		s.mu.Unlock()
		return
	}
	// Acquire never fails and never waits, so holding mu across it costs nothing.
	sub := &subject{params: p, lease: s.conns.Acquire(p.ContextName)}
	s.tracked[id] = sub
	ids := s.byContext[p.ContextName]
	if ids == nil {
		ids = map[string]struct{}{}
		s.byContext[p.ContextName] = ids
	}
	ids[id] = struct{}{}
	s.mu.Unlock()

	s.engine.Add(id)
}

// Forget disarms id's sweep and releases everything Track took. Idempotent.
//
// **The subject and its watcher go in one critical section**, and the stop happens after. A run
// is on a worker, so a sweep can finish inside this teardown — and stopping a watcher waits for
// its streams, which is as long as the API server takes to hang up. Dropping the two separately
// leaves a window where the id still reads as tracked with no watcher against it, which is
// exactly what ensureWatcher establishes into.
func (s *Service) Forget(id string) {
	s.mu.Lock()
	sub := s.tracked[id]
	if sub == nil {
		s.mu.Unlock()
		return
	}
	delete(s.tracked, id)
	delete(s.published, id)
	w := s.watchers[id]
	delete(s.watchers, id)
	ids := s.byContext[sub.params.ContextName]
	delete(ids, id)
	if len(ids) == 0 {
		delete(s.byContext, sub.params.ContextName)
	}
	// Under the lock, so a Forget racing a fresh Track of the same id cannot remove the
	// subject the new call just added.
	s.engine.Remove(id)
	s.mu.Unlock()

	// Outside the lock: stop waits for both streams, and nothing about that needs the map.
	if w != nil {
		w.stop()
	}
	sub.lease.Release()
}

// Read is the sweep's standing answer for id, beside its attempts. ok is false for an
// id nothing tracks.
func (s *Service) Read(id string) (Observation, bool) {
	v, tracked := s.engine.Read(id)
	if !tracked {
		return Observation{}, false
	}
	return keyCatalog.From(v), true
}

// Subscribe reports every id whose news changed, for the trigger that requeues each
// one's record.
func (s *Service) Subscribe() Subscription { return s.signalHub.Receiver() }

// Start runs the engine and the connection bridge. The pool subscription is taken
// before Start returns, so nothing it says in between is dropped.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	stopEngine := s.engine.Start(ctx)

	// Not Start's context, which bounds startup: this one bounds the bridge, so it
	// lives until the stop func cancels it.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	moved := s.conns.Subscribe()
	s.wg.Go(func() {
		defer moved.Close()
		s.watchConnections(loopCtx, moved)
	})

	return func(ctx context.Context) error {
		stopLoop()
		return errors.Join(drain.WithContext(ctx, s.wg.Wait), stopEngine(ctx))
	}, nil
}

// Close drops every subject and gives the pool its claims back. Sweep values own
// nothing, so the engine's copies need no retiring of their own.
func (s *Service) Close() error {
	// Every watcher at once, rather than one stop per id: they share the context, and each
	// one's own stop would wait for its streams in turn.
	s.stopWatchers()

	s.mu.Lock()
	leases := make([]kubeconn.Lease, 0, len(s.tracked))
	for _, sub := range s.tracked {
		leases = append(leases, sub.lease)
	}
	watchers := slices.Collect(maps.Values(s.watchers))
	clear(s.tracked)
	clear(s.byContext)
	clear(s.published)
	clear(s.watchers)
	s.mu.Unlock()

	for _, w := range watchers {
		w.stop()
	}

	err := s.engine.Close()
	for _, lease := range leases {
		lease.Release()
	}
	return err
}

// watchConnections wakes every subject over a context whose news changed, until
// stopped — what re-arms a sweep suspended on ReasonNoConnection the moment the pool
// reaches the server again. Cancellation ends the wait: the pool is not required to
// close its channel when released.
func (s *Service) watchConnections(ctx context.Context, moved kubeconn.Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-moved.Chan():
			if !ok {
				return
			}
			s.mu.Lock()
			ids := slices.Collect(maps.Keys(s.byContext[ev.Key]))
			s.mu.Unlock()
			for _, id := range ids {
				s.engine.Wake(id, nameCatalog)
			}
		}
	}
}

// connFor is the probe's connection for its subject: the one the pool holds for that
// context, and only while it is the server the subject was armed for. An id forgotten
// mid-run reads as no connection; the engine refuses that run's commit anyway.
//
// **This is what keeps one cluster's kinds out of another's catalog.** A context re-pointed
// at a second cluster wakes every subject over it — identity is in the pool's news — so the
// superseded cache's sweep is the first thing to run against the new server, well before
// the record learns it is superseded and disarms. The pool answers from the identity
// stamped on the connection itself, so a connection built but not yet identified is
// refused rather than paired with the previous one's answer.
func (s *Service) connFor(ctx context.Context, id string) (*kubeconn.Connection, error) {
	s.mu.Lock()
	sub := s.tracked[id]
	s.mu.Unlock()
	if sub == nil {
		return nil, fmt.Errorf("%w: subject %q", kubeconn.ErrNoConnection, id)
	}
	return sub.lease.ConnFor(ctx, sub.params.ServerUID)
}

// Wake asks for id's sweep now rather than at the interval — what a wiper calls so the
// rows it emptied are rewritten in seconds. A no-op for an id nothing tracks.
func (s *Service) Wake(id string) { s.engine.Wake(id, nameCatalog) }

// mirror writes one sweep's answer into its cache's store, claiming the file for the
// write alone: nothing holds one open for a subject that is not running, which is what
// lets a cache's teardown delete it.
func (s *Service) mirror(ctx context.Context, id string, sw sweep, fingerprint uint64) error {
	s.mu.Lock()
	sub := s.tracked[id]
	s.mu.Unlock()
	if sub == nil {
		return fmt.Errorf("%w: subject %q", kubestore.ErrRemoved, id)
	}

	store, err := s.stores.OpenOrCreate(sub.params.CacheID)
	if err != nil {
		return err
	}
	defer store.Release()

	// Prune only on a complete answer, the same rule the children follow: a group that
	// went quiet has not stopped being served.
	return store.SyncKinds(ctx, kindRows(sw.Kinds), !sw.Partial, fingerprint)
}

// kindRows translates the sweep's answer into the table's own vocabulary.
func kindRows(kinds []Kind) []kubestore.KindRow {
	rows := make([]kubestore.KindRow, 0, len(kinds))
	for _, k := range kinds {
		scope := kubestore.ScopeCluster
		if k.Namespaced {
			scope = kubestore.ScopeNamespaced
		}
		rows = append(rows, kubestore.KindRow{
			APIVersion: k.GroupVersion,
			Kind:       k.Kind,
			Resource:   k.Resource,
			Scope:      scope,
			IsCRD:      k.IsCRD,
		})
	}
	return rows
}

// publish is the engine's OnPass: signal the id when its news moved. Every pass rather
// than every change is the engine's contract — attempts always churn — so the
// projection to news is what keeps a timing-only pass from waking the fold.
func (s *Service) publish(id string, v probe.Snapshot) {
	o := keyCatalog.From(v)
	// A pass that finished no run carries no news — and the arming pass would otherwise
	// signal the very fold that just armed it, before the first sweep has run.
	if !o.LastAttempt.Done() {
		return
	}
	n := newsOf(o)

	s.mu.Lock()
	_, held := s.tracked[id]
	changed := held && s.published[id] != n
	if held {
		s.published[id] = n
	}
	s.mu.Unlock()

	if changed {
		s.signalHub.Sender().Send(id, struct{}{})
	}
}

// news is the part of a pass the fold reacts to: the answer and the verdict, never the
// attempt bookkeeping. WatchLive is in it because the fold reports it, and it moves only
// inside a run — so a flapping watch costs one wake per sweep, not one per pass.
type news struct {
	fingerprint uint64
	partial     bool
	watchLive   bool
	known       bool
	ok          bool
	reason      probe.Reason
}

func newsOf(o Observation) news {
	return news{
		fingerprint: o.Value.Fingerprint,
		partial:     o.Value.Partial,
		watchLive:   o.Value.WatchLive,
		known:       o.Known(),
		ok:          o.OK(),
		reason:      o.LastAttempt.Reason,
	}
}

// ensureWatcher stands a watch up for id over conn, unless the one already standing is live
// and over that same connection, and returns once the watch is open. What it stands over is the
// connection the run just resolved, so the watcher inherits that connection's identity scoping
// rather than checking anything itself. Every run calls it, so a connection replaced under an
// unchanged server takes the watch with it whatever the sweep then goes on to find.
//
// **Nothing else re-establishes**, so the two cases a standing watcher must not survive are
// both here. A spent one has already woken this sweep on its way out and would otherwise be
// read as still watching. A live one over a superseded connection is worse than none: it holds
// an HTTP watch over retired credentials, and the streams do not read conn.Done() — which
// would not cover it anyway, since a conflicted connection is never retired.
//
// **It stores only while id is still tracked**, checked and written in one critical section.
// A run is on a worker, so Forget can land under it: the sweep finishes and establishes for an
// id nothing tracks, and the goroutine and its two streams then stand until the connection
// retires — indefinitely, for a healthy cluster. The engine's commit refusal does not cover
// this, because establishment is not a commit. The same check publish makes against the entry
// and connFor makes against the subject.
func (s *Service) ensureWatcher(ctx context.Context, id string, conn *kubeconn.Connection) bool {
	s.mu.Lock()
	if _, tracked := s.tracked[id]; !tracked {
		s.mu.Unlock()
		return false
	}
	standing := s.watchers[id]
	if standing != nil && standing.conn == conn && !standing.spent() {
		s.mu.Unlock()
		standing.awaitOpen(ctx)
		// Re-read: a stream can end while the wait is out.
		return !standing.spent()
	}
	// Started under the lock so a Forget racing this cannot miss it: what Forget stops is
	// whatever the map holds, and this is in the map before the lock is dropped.
	w := startWatcher(s.watcherCtx, conn, s.open, reopenDelay, func() { s.engine.Wake(id, nameCatalog) })
	s.watchers[id] = w
	s.mu.Unlock()

	// Outside the lock, since neither of these needs the map: stop waits for both streams, and
	// the caller's sweep waits on awaitOpen. The replacement is already the one in the map, so
	// the overlap costs a moment of two watchers and never a lost wake.
	if standing != nil {
		standing.stop()
	}
	w.awaitOpen(ctx)
	// Read after the wait, which is what makes it meaningful: a stream marks itself spent
	// before it reports its first open, so a refusal is already in by the time this returns.
	return !w.spent()
}

// stopWatcher ends id's watch if one stands — the probe's unwatch, called by every run that
// could not use its connection. conn.Done() does not cover that, since a connection that goes
// conflicted is never retired. Teardown is Forget's, which drops the watcher with the subject.
func (s *Service) stopWatcher(id string) {
	s.mu.Lock()
	w := s.watchers[id]
	delete(s.watchers, id)
	s.mu.Unlock()

	// Outside the lock: stop waits for both streams, and nothing about that needs the map.
	if w != nil {
		w.stop()
	}
}
