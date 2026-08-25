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
// and the wake layers (the connection bridge today, the CRD/APIService watch to come)
// only make it prompt. → docs/specs/kubecatalog-discovery.md.
package kubecatalog

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"sync"

	"github.com/amorey/gobus/conflate"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// connService is the pool the sweeps borrow connections from: a lease per tracked
// subject, and the fleet feed that says a context's connection moved.
type connService interface {
	Acquire(contextName string) kubeconn.Lease
	Subscribe() kubeconn.Subscription
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

	wg sync.WaitGroup
}

// subject is one tracked id: the context it sweeps over, the server that context has to
// answer as, and this service's own claim on the connection — refcounted in the pool
// beside every other holder's, so releasing it never stops the cluster being probed.
//
// **A context is not an identity.** It can be re-pointed at another cluster, and the
// pool hands out whatever now answers, so the server the caller armed this subject for
// is the thing that makes a sweep's answer belong to it.
type subject struct {
	contextName string
	serverUID   string
	lease       kubeconn.Lease
}

// New returns a Service sweeping over conns' connections.
func New(conns connService) *Service {
	return newWithOptions(conns)
}

// option is a test seam on the probe, reachable only from white-box tests.
type option func(*catalogProbe)

// withSweep substitutes the API server behind every sweep.
func withSweep(f func(*kubeconn.Connection) (Catalog, error)) option {
	return func(p *catalogProbe) { p.sweep = f }
}

// newWithOptions is New plus the seams.
func newWithOptions(conns connService, opts ...option) *Service {
	s := &Service{
		conns:     conns,
		engine:    probe.New(),
		signalHub: conflate.New[string, struct{}](),
		tracked:   map[string]*subject{},
		byContext: map[string]map[string]struct{}{},
		published: map[string]news{},
	}

	p := &catalogProbe{conn: s.connFor, sweep: discoverServedKinds}
	for _, opt := range opts {
		opt(p)
	}
	probe.Register(s.engine, nameCatalog, p, probe.WithInterval(sweepInterval), probe.WithTimeout(sweepTimeout))
	s.engine.OnPass(s.publish)
	return s
}

// Track arms the sweep for id over contextName's connection, for as long as that context
// answers as serverUID. Idempotent, and all three are fixed for the id's life — the
// caller derives them from one record, whose identity does not change — so a repeat is a
// no-op, never a re-bind.
func (s *Service) Track(id, contextName, serverUID string) {
	s.mu.Lock()
	if _, held := s.tracked[id]; held {
		s.mu.Unlock()
		return
	}
	// Acquire never fails and never waits, so holding mu across it costs nothing.
	sub := &subject{contextName: contextName, serverUID: serverUID, lease: s.conns.Acquire(contextName)}
	s.tracked[id] = sub
	ids := s.byContext[contextName]
	if ids == nil {
		ids = map[string]struct{}{}
		s.byContext[contextName] = ids
	}
	ids[id] = struct{}{}
	s.mu.Unlock()

	s.engine.Add(id)
}

// Forget disarms id's sweep and releases everything Track took. Idempotent.
func (s *Service) Forget(id string) {
	s.mu.Lock()
	sub := s.tracked[id]
	if sub == nil {
		s.mu.Unlock()
		return
	}
	delete(s.tracked, id)
	delete(s.published, id)
	ids := s.byContext[sub.contextName]
	delete(ids, id)
	if len(ids) == 0 {
		delete(s.byContext, sub.contextName)
	}
	// Under the lock, so a Forget racing a fresh Track of the same id cannot remove the
	// subject the new call just added.
	s.engine.Remove(id)
	s.mu.Unlock()

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
	s.mu.Lock()
	leases := make([]kubeconn.Lease, 0, len(s.tracked))
	for _, sub := range s.tracked {
		leases = append(leases, sub.lease)
	}
	clear(s.tracked)
	clear(s.byContext)
	clear(s.published)
	s.mu.Unlock()

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
	return sub.lease.ConnFor(ctx, sub.serverUID)
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

// news is the part of a pass the fold reacts to: the answer and the verdict, never
// timing.
type news struct {
	kinds   uint64
	partial bool
	known   bool
	ok      bool
	reason  probe.Reason
}

func newsOf(o Observation) news {
	return news{
		kinds:   kindsFingerprint(o.Value.Kinds),
		partial: o.Value.Partial,
		known:   o.Known(),
		ok:      o.OK(),
		reason:  o.LastAttempt.Reason,
	}
}

// kindsFingerprint folds the kind list into one comparable word, so news stays a value
// a map compare reads with ==.
func kindsFingerprint(kinds []Kind) uint64 {
	h := fnv.New64a()
	for _, k := range kinds {
		fmt.Fprintf(h, "%s|%s|%s|%t\n", k.GroupVersion, k.Resource, k.Kind, k.Namespaced)
	}
	return h.Sum64()
}
