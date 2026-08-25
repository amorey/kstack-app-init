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

// Package kubeconn hands out leases on kube-contexts and reports what probing the server behind
// one found.
//
// The probes live in probe.go and the scheduling around them is the probe engine's
// (internal/probe). This file is the rest: leases, publishing what the engine observes, and
// retiring the connections its runs build.
//
// Three rules shape it:
//
//   - One context, one entry. Contexts resolving to the same credentials are not merged, so
//     what is learned about one belongs to exactly one context.
//   - A claim outlives what it is a claim on. The kubeconfig can stop naming a context while a
//     holder still holds it, and the entry stays: the file may name it again, and the claim is
//     how the holder hears about that. Only releasing drops an entry.
//   - It never learns what a cluster is. Callers name kube-contexts; whether the server behind
//     one is the cluster the caller meant is the caller's to decide.
package kubeconn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// kubeconfigService is the reader this package asks whether a context still resolves, and which
// tells it when the file changed. The fingerprint RESTConfig returns beside the config describes
// the credentials, and is what tells a rotation from a write that changed nothing.
type kubeconfigService interface {
	RESTConfig(contextName string) (*rest.Config, string, error)
	Subscribe() kubeconfig.Subscription
}

// Subscription reports the contexts whose news changed, one event per context, for a reader
// whose reaction to any of them is the same: re-read that claim. A keyed, coalescing bus, so a
// fleet changing at once neither loses a context behind a busier one nor overflows a buffer.
// The value carries nothing — the key is the news.
type Subscription = *conflate.Receiver[string, struct{}]

// StateSubscription carries each State published for one claim. Close it when done — an
// abandoned one keeps its slot.
//
// Keyed by context, not by the credentials behind it: a receiver is bound to its key for life,
// and credentials move under a context that never does.
//
// Nothing is delivered on attach — a watcher reads Lease.State for what is known now and this
// for what comes after. Every value is a level, never an edge: the hub keeps only the latest
// per key, so a reader that falls behind skips what came between.
type StateSubscription = *watch.Receiver[string, State]

// Lease is a claim on one kube-context: what keeps the pool tracking it, and how a holder hears
// about it. An interface because it crosses out of this package, where a caller's test has to
// stand in for a live cluster.
type Lease interface {
	// Conn is the connection to the context's server, or ErrNoConnection while the context
	// resolves to nothing. It never dials: what it hands back is what the connection probe
	// built, failing last probe included.
	Conn(ctx context.Context) (*Connection, error)
	// State is what the probes last found, probe by probe. It never dials.
	State() State
	// ConnFor is Conn, refused with ErrIdentityMismatch unless the server behind it is
	// serverUID — for a caller whose work belongs to one cluster rather than to a context.
	//
	// It reads the identity off the connection itself, never off State: State.ServerUID is
	// its own probe's observable, queued by a committed connection rather than applied by
	// it, so a fresh connection sits beside the previous one's UID until that probe re-runs.
	// A connection nobody has identified yet is refused for the same reason.
	ConnFor(ctx context.Context, serverUID string) (*Connection, error)
	// WatchState carries every State published for this claim. It delivers nothing on attach,
	// so a watcher pairs it with State for what is known now.
	//
	// **The caller closes it**, as with Subscribe. Release does not: the receiver is keyed by
	// context, so one that outlives its claim keeps a hub slot and goes on reporting whatever
	// claims that name next.
	WatchState() StateSubscription
	// Departed reports that the kubeconfig no longer names this claim's context.
	//
	// Not an error and not final: the file may name it again, and this claim is how the
	// holder hears about that. Subscribe and WatchState both fire when this flips.
	Departed() bool
	// Release drops the claim, and the entry with it once nothing else holds the context.
	// Idempotent, so it is safe to defer.
	Release()
}

// Service is the pool the cluster service leases contexts from. The probe engine tracks one
// subject per claimed context; this type owns who holds the claims and what the fleet is told.
type Service struct {
	kubecfgSvc kubeconfigService
	// engine runs the five probes of probe.go over the claimed contexts; probes is what
	// registering them returned.
	engine *probe.Engine
	// signalHub names the contexts whose news changed; stateHub carries what the probes read.
	// Both keyed by context, both fed by publish.
	signalHub *conflate.Hub[string, struct{}]
	stateHub  *watch.Hub[string, State]

	// mu guards claimed and published together: what publish announces is measured against
	// who still holds the claim, and nothing may see one without the other.
	mu sync.Mutex
	// claimed holds one entry per claimed context — a key is here exactly while someone holds
	// that context, mirrored by a subject in the engine — and is also the key both hubs
	// publish under.
	claimed map[string]*entry
	// published is the news the fleet was last told per context, compared against so a pass
	// that moved only timing wakes nobody.
	published map[string]news

	wg sync.WaitGroup
}

// entry is a claimed context's holder count and the connection its probe built. A pointer per
// context on purpose: a claim carries the entry it was given, which is what stops a release that
// outlived Close from touching whatever claims the name next.
//
// The connection is here as well as in the engine's observable because the two answer different
// questions: the engine's copy is what a run reads and what Conn hands out, and this one is who
// to retire when the entry goes — an engine a Remove has already emptied can name nobody.
type entry struct {
	holders int
	conn    *Connection
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfgSvc kubeconfigService) *Service {
	// Here rather than in the composition root because the vars are read when a transport is
	// built, and this package builds them — a call the root has to remember is one that goes
	// missing.
	configureHTTP2Keepalive()

	s := &Service{
		kubecfgSvc: kubecfgSvc,
		engine:     probe.New(),
		signalHub:  conflate.New[string, struct{}](),
		stateHub:   watch.New[string, State](),
		claimed:    map[string]*entry{},
		published:  map[string]news{},
	}
	registerProbes(s.engine, kubecfgSvc)
	s.engine.OnPass(s.publish)
	return s
}

// Acquire claims contextName and hands back the lease on it. It never fails and never waits: a
// context the kubeconfig does not name yet is claimable, because the file may name it later and
// the claim is how the holder finds out.
//
// The first holder adds the context to the engine, whose first pass dispatches the connection
// probe — not work to do on the caller's thread. A later holder joins what the probes found.
func (s *Service) Acquire(contextName string) Lease {
	s.mu.Lock()
	e, held := s.claimed[contextName]
	if !held {
		e = &entry{}
		s.claimed[contextName] = e
	}
	e.holders++
	s.mu.Unlock()

	if !held {
		s.engine.Add(contextName)
	}
	return &claim{svc: s, contextName: contextName, entry: e}
}

// Retry runs every probe on contextName now, whatever it was next due at and whatever suspended
// it. For a user who fixed something no probe can observe — a VPN dialed, a credential rotated —
// and should not have to wait out the cadence.
//
// All five rather than the connection alone: a connection that is already up commits nothing, so
// waking it would leave a probe that failed on its own — a forbidden kube-system read — suspended
// on the answer the user just fixed.
//
// A context nobody claims is untracked and this does nothing. Nothing to report either way: what
// the re-probe finds reaches watchers the way every other pass does.
func (s *Service) Retry(contextName string) {
	s.engine.Wake(contextName, probeNames[:]...)
}

// Subscribe reports every context whose news changed, for a reader whose reaction to any of them
// is the same. A holder that cares about one claim watches that claim instead.
func (s *Service) Subscribe() Subscription { return s.signalHub.Receiver() }

// Start runs the engine and the kubeconfig watch. The kubeconfig subscription is taken before
// Start returns, so nothing it says in between is dropped; the engine's queues need no such
// care, since they hold what a claim taken before Start asked for until its workers run.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	stopEngine := s.engine.Start(ctx)

	// Not Start's context, which bounds startup: this one bounds the watch, so it lives until
	// the stop func cancels it.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	cfgs := s.kubecfgSvc.Subscribe()
	s.wg.Go(func() {
		defer cfgs.Close()
		s.watchKubeconfig(loopCtx, cfgs)
	})

	return func(ctx context.Context) error {
		stopLoop()
		return errors.Join(drain.WithContext(ctx, s.wg.Wait), stopEngine(ctx))
	}, nil
}

// Close drops what the pool holds, the engine's subjects included. Claims are not released for
// their holders: a claim outliving the pool is the holder's bug, and reading as nothing known is
// what a dropped entry already does.
func (s *Service) Close() error {
	s.mu.Lock()
	dropped := make([]*Connection, 0, len(s.claimed))
	for contextName, e := range s.claimed {
		if e.conn != nil {
			dropped = append(dropped, e.conn)
		}
		// The engine's copy too, and before it is closed: a pass whose commit landed while
		// the pass worker was already stopped left the connection it built there and nowhere
		// else. Retiring is idempotent, so the usual case retires the same one twice.
		if v, tracked := s.engine.Read(contextName); tracked {
			if conn := keyConnection.From(v).Value.conn; conn != nil {
				dropped = append(dropped, conn)
			}
		}
	}
	clear(s.claimed)
	clear(s.published)
	s.mu.Unlock()

	err := s.engine.Close()
	for _, conn := range dropped {
		conn.retire()
	}
	return err
}

// watchKubeconfig wakes every claimed context's connection probe on every change, until stopped.
// Every context rather than the ones that moved, because the feed carries a whole config and
// working out which contexts changed is what the probe does anyway. Only the connection probe:
// resolving owns the context's lifecycle, and the four behind it hang off its answer.
//
// Cancellation ends the wait: the service behind the feed is the app's and is not required to
// close its channel when released.
func (s *Service) watchKubeconfig(ctx context.Context, cfgs kubeconfig.Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-cfgs.Chan():
			if !ok {
				return
			}
			s.engine.WakeAll(nameConnection)
		}
	}
}

// publish is the engine's OnPass: project the pass into State, tell the claim watchers, and
// signal the fleet when the news moved. The engine serializes it per context, and the order
// holds — a reader the signal wakes finds the state already published.
func (s *Service) publish(contextName string, v probe.Snapshot) {
	st := s.stateOf(v)
	n := s.newsOf(v, st)

	stale, held, changed := s.record(contextName, keyConnection.From(v).Value.conn, n)
	if stale != nil {
		// Outside the lock: retiring closes sockets, and nothing about it needs the pool.
		stale.retire()
	}
	if !held {
		// The pass raced the last release; watchers on this name hear about whatever claims
		// it next.
		return
	}

	s.stateHub.Sender().Send(contextName, st)
	if changed {
		s.signalHub.Sender().Send(contextName, struct{}{})
	}
}

// record files what a pass concluded against the entry that must still be there to receive it,
// and hands back the connection nothing holds any more for the caller to retire.
//
// One critical section, because the entry check is what makes the rest of it true: a release
// landing between the two would leave this writing published for a context nobody holds — which
// both announces a claim that is gone and leaves a stale baseline, so the first pass of whatever
// claims the name next compares equal and tells the fleet nothing.
//
// held is not the same question as stale: what a release retired is the connection the *entry*
// held, so a pass landing after one carries a connection nothing else can reach — that one is
// the stale one.
func (s *Service) record(contextName string, conn *Connection, n news) (stale *Connection, held, changed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.claimed[contextName]
	if e == nil {
		return conn, false, false
	}
	if e.conn != conn {
		stale, e.conn = e.conn, conn
	}
	changed = s.published[contextName] != n
	s.published[contextName] = n
	return stale, true, changed
}

// news is the part of a pass a fleet reader reacts to: what the probes concluded, never when.
// The timing moves every run, so signalling on it would wake every cluster's reconcile on every
// cadence to find nothing changed.
type news struct {
	departed bool
	phase    Phase
	identity Identity
	// vouchedFor is the cluster the CURRENT connection vouches for, empty while it vouches
	// for none. Distinct from identity, which is what the probes last read over whatever
	// connection was up at the time: a rebuild empties this and a stamp refills it, and
	// neither moves any other field when the cluster did not change.
	//
	// Without it a credential rotation for an unchanged cluster is silent — the connection is
	// replaced, an identity-scoped holder is refused until the stamp lands, and the stamp
	// commits nothing because the uid it read equals the one already recorded. Nothing would
	// ever tell that holder to try again.
	vouchedFor string
	ok         [len(probeNames)]bool
}

func (s *Service) newsOf(v probe.Snapshot, st State) news {
	ci := keyConnection.From(v).Value
	n := news{
		departed: ci.departed,
		phase:    st.Phase(),
		identity: st.Identity(),
	}
	if ci.conn != nil {
		n.vouchedFor, _ = ci.conn.ServerUID()
	}
	for i, name := range probeNames {
		n.ok[i] = v.Attempts(name).OK()
	}
	return n
}

// stateOf projects the engine's observables into State. The connection's observable bundles the
// context's standing with the endpoint; only the endpoint is the answer State carries.
func (s *Service) stateOf(v probe.Snapshot) State {
	ci := keyConnection.From(v)
	return State{
		Connection:    Observation[string]{Value: ci.Value.endpoint, LastSeen: ci.LastSeen, Attempts: ci.Attempts},
		Readiness:     probe.Get[ComponentStatus](v, nameReadiness),
		ServerUID:     probe.Get[string](v, nameServerUID),
		ServerVersion: probe.Get[VersionInfo](v, nameServerVersion),
		Principal:     probe.Get[Principal](v, namePrincipal),
	}
}

// claim is one holder's claim on the context it named, and on the entry it was given. It carries
// both because a context outlives its entries: what a claim may read and release is the one it
// was made for, never whatever has the name now.
type claim struct {
	svc         *Service
	contextName string
	entry       *entry

	released atomic.Bool
}

// Conn never dials: the connection is whatever the last run of the connection probe built.
// A holder that wants to wait for one pairs State with WatchState, as it does for everything
// else — a connection whose last probe failed is still handed out, since only the holder can
// tell a revoked credential from a control plane mid-restart.
func (c *claim) Conn(context.Context) (*Connection, error) {
	v, ok := c.svc.read(c.contextName, c.entry)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrNoConnection, c.contextName)
	}
	conn := keyConnection.From(v).Value.conn
	if conn == nil {
		return nil, fmt.Errorf("%w: %q", ErrNoConnection, c.contextName)
	}
	return conn, nil
}

func (c *claim) State() State {
	v, ok := c.svc.read(c.contextName, c.entry)
	if !ok {
		return State{}
	}
	return c.svc.stateOf(v)
}

// ConnFor asks the connection who it reached, so there is nothing to correlate: the probe
// that made the request is the one that stamped it. The connection is resolved first, so a
// cluster nothing has reached reports the outage rather than an identity it could not have
// read either way.
func (c *claim) ConnFor(ctx context.Context, serverUID string) (*Connection, error) {
	conn, err := c.Conn(ctx)
	if err != nil {
		return nil, err
	}

	if err := conn.IdentityFor(serverUID); err != nil {
		return nil, fmt.Errorf("context %q: %w", c.contextName, err)
	}
	return conn, nil
}

func (c *claim) Departed() bool {
	v, ok := c.svc.read(c.contextName, c.entry)
	if !ok {
		return true
	}
	return keyConnection.From(v).Value.departed
}

// read answers a claim from the engine, for the entry the claim was made for. Once the pool no
// longer holds that entry — released, or dropped by Close — nothing is known, which a claim
// reads as departed with no connection.
//
// The identity check comes after the engine read: the last holder can release and another
// caller re-claim the name mid-read, and the state that came back is then the new claim's — the
// check catches it, where one taken before the read would not.
func (s *Service) read(contextName string, held *entry) (probe.Snapshot, bool) {
	v, tracked := s.engine.Read(contextName)

	s.mu.Lock()
	valid := s.claimed[contextName] == held
	s.mu.Unlock()

	return v, valid && tracked
}

// WatchState takes no baseline: the hub compares against one only through an Accept, and this
// one has none, so every value is delivered. What a baseline is for — reading and registering in
// one critical section, so a publish landing between the two is not skipped — is worth having
// once a probe can land at all.
//
// Not tracked for the caller. The receiver is the caller's to close, and closing it here on
// Release would take one out from under a caller that is still reading it.
func (c *claim) WatchState() StateSubscription {
	return c.svc.stateHub.Watch(c.contextName)
}

// Release gives the claim back, dropping the entry — and the engine's subject with it — once
// the last holder goes: an entry nobody holds is one nothing probes. The CAS makes it
// idempotent while other holders remain, when the entry check below cannot tell a second
// release from a first.
func (c *claim) Release() {
	if !c.released.CompareAndSwap(false, true) {
		return
	}

	s := c.svc
	s.mu.Lock()
	// Only the entry the claim was made for: Close drops entries out from under the leases
	// still holding them, so a stale release must not decrement — or delete — whatever has
	// claimed the name since.
	if s.claimed[c.contextName] != c.entry {
		s.mu.Unlock()
		return
	}
	var dropped *Connection
	c.entry.holders--
	if c.entry.holders == 0 {
		dropped = c.entry.conn
		delete(s.claimed, c.contextName)
		delete(s.published, c.contextName)
		// Under the lock, so a release racing a fresh Acquire of the same name cannot remove
		// the subject the new claim just added.
		s.engine.Remove(c.contextName)
	}
	s.mu.Unlock()

	// An entry that goes takes its connection with it: nothing probes the context now, so
	// nothing else would ever close these sockets.
	if dropped != nil {
		dropped.retire()
	}
}
