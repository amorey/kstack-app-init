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

// Package kubeconn hands out leases on kube-contexts and reports what checking the server behind
// one found.
//
// **Nothing dials yet**, so Lease.Conn reports ErrNoConnection and the only answers State carries
// are the ones a check can reach without a server: a context that left the kubeconfig, and one
// that will not resolve. The scheduling around them is real: what is checked lives in check.go,
// and when each check runs is Service.due.
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
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/workqueue"
)

// kubeconfigService is the reader this package asks whether a context still resolves, and which
// tells it when the file changed. The fingerprint RESTConfig returns beside the config is unused
// until connections come back.
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
	// Conn is the connection to the context's server.
	//
	// Nothing builds one yet, so this always reports ErrNoConnection.
	Conn(ctx context.Context) (*Connection, error)
	// State is what the checks last found, check by check. It never dials.
	//
	// Nothing dials yet, so only the checks that need no server have answered.
	State() State
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

// Service is the pool the cluster service leases contexts from.
type Service struct {
	kubecfgSvc kubeconfigService
	// signalHub names the contexts whose news changed; stateHub carries what a probe read.
	// Both keyed by context, both fed by one publish.
	signalHub *conflate.Hub[string, struct{}]
	stateHub  *watch.Hub[string, State]
	// checkQ queues the checks that are due, one key per check per context. A queue and not a
	// bus: what it holds outlives the gap before Start runs the workers, so a claim taken in
	// that window is still checked.
	checkQ *workqueue.Queue[checkKey]
	// intervals paces the checks, one entry per checkID.
	intervals [numChecks]time.Duration

	// mu guards claimed and the entries in it together: a holder count and a departed flag
	// are read against each other, and nothing may see one without the other.
	mu sync.Mutex
	// claimed holds one entry per claimed context — a key is here exactly while someone
	// holds that context — and is also the key both hubs publish under.
	claimed map[string]*entry
	// kubecfgGen counts the changes the kubeconfig watch has seen. Starts at 1, so a fresh
	// entry's zero differs and its connection check is due on the first reconcile.
	kubecfgGen uint64

	wg sync.WaitGroup
}

// entry is what the pool holds for one claimed context.
type entry struct {
	holders int
	// departed is what the last read of the kubeconfig said: false while the file names this
	// context, true once it stops. Flipping it is news the holders are told.
	departed bool
	// state is what the checks found, each owning one observation in it.
	state State
	// conn is the connection the checks run over. Nothing builds one yet.
	conn *Connection
	// kubecfgGen is the kubeconfig generation the connection check last read at. Differing
	// from the service's is what makes that check due, so a file change needs to name no
	// check — it bumps the generation and reconciles.
	kubecfgGen uint64
	// timer brings reconcile back when the soonest thing due comes round. One, because it
	// wakes the pass that decides per check rather than pacing any of them.
	timer *time.Timer
	// published is the news the fleet was last told. Compared against rather than against the
	// start of a pass, since several commits can land behind one reconcile and only what
	// survived them is news.
	published news
}

// stopTimer gives up an entry's scheduled wake. An entry nobody holds is one nothing checks.
func (e *entry) stopTimer() {
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
}

// news is the part of an entry a fleet reader reacts to: what the checks concluded, never when.
// The timing moves every run, so signalling on it would wake every cluster's reconcile on every
// cadence to find nothing changed.
type news struct {
	departed bool
	phase    Phase
	identity Identity
	ok       [numChecks]bool
}

func newsOf(e *entry) news {
	n := news{departed: e.departed, phase: e.state.Phase(), identity: e.state.Identity()}
	for id, a := range observations(&e.state) {
		n.ok[id] = a.OK()
	}
	return n
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfgSvc kubeconfigService) *Service { return newWithOptions(kubecfgSvc) }

// option is a test seam. Production calls New.
type option func(*Service)

// withIntervals paces the checks, so a test picks its own timescale rather than outwaiting the
// production cadence.
func withIntervals(intervals [numChecks]time.Duration) option {
	return func(s *Service) { s.intervals = intervals }
}

func newWithOptions(kubecfgSvc kubeconfigService, opts ...option) *Service {
	s := &Service{
		kubecfgSvc: kubecfgSvc,
		signalHub:  conflate.New[string, struct{}](),
		stateHub:   watch.New[string, State](),
		checkQ:     workqueue.New[checkKey](),
		intervals:  defaultIntervals,
		claimed:    map[string]*entry{},
		kubecfgGen: 1,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Acquire claims contextName and hands back the lease on it. It never fails and never waits: a
// context the kubeconfig does not name yet is claimable, because the file may name it later and
// the claim is how the holder finds out.
//
// The first holder derives the new entry's schedule here, which queues its checks rather than
// running them — the kubeconfig is not read on the caller's thread. A later holder joins what
// the first one's checks found.
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
		// A fresh entry has read no kubeconfig, so this pass queues its connection check and
		// nothing else — the four behind it wait on a connection.
		s.reconcile(contextName)
	}

	return &claim{svc: s, contextName: contextName, entry: e}
}

// Subscribe reports every context whose news changed, for a reader whose reaction to any of them
// is the same. A holder that cares about one claim watches that claim instead.
func (s *Service) Subscribe() Subscription { return s.signalHub.Receiver() }

// Start runs the check workers and the watch that asks them to run again as the kubeconfig
// moves. The kubeconfig subscription is taken before Start returns, so nothing it says in between
// is dropped; the check queue needs no such care, since it holds what a claim taken before Start
// asked for until a worker arrives.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	// Not Start's context, which bounds startup: this one bounds the loops, so it lives until
	// the stop func cancels them.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	for range checkWorkers {
		s.wg.Go(func() { s.checkLoop(loopCtx) })
	}

	cfgs := s.kubecfgSvc.Subscribe()
	s.wg.Go(func() {
		defer cfgs.Close()
		s.watchKubeconfig(loopCtx, cfgs)
	})

	return func(ctx context.Context) error {
		stopLoop()
		return drain.WithContext(ctx, s.wg.Wait)
	}, nil
}

// Close drops what the pool holds. Claims are not released for their holders: a claim outliving
// the pool is the holder's bug, and reading as nothing known is what a dropped entry already
// does.
//
// Closing the queue is what stops it accumulating: past here nothing works it off, and a held
// claim still adds on every kubeconfig change.
func (s *Service) Close() error {
	s.checkQ.Close()

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range s.claimed {
		e.stopTimer()
	}
	clear(s.claimed)
	return nil
}

// watchKubeconfig re-derives every claimed context's schedule on every change, until stopped.
// Every context rather than the ones that moved, because the feed carries a whole config and
// working out which contexts changed is what the connection check does anyway.
//
// Bumping the generation is what makes those checks due. The watch names no check: a context
// whose connection check has not read the current file is due, and reconcile is what knows that.
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
			s.bumpKubeconfig()
		}
	}
}

// bumpKubeconfig records that the file moved and re-derives every claimed context's schedule.
// One generation for the whole file, since that is what the feed carries.
//
// The names are collected before any pass runs, because reconcile takes the pool's lock itself.
// A context released in between is dropped by the pass that finds it gone.
func (s *Service) bumpKubeconfig() {
	s.mu.Lock()
	s.kubecfgGen++
	contextNames := slices.Collect(maps.Keys(s.claimed))
	s.mu.Unlock()

	for _, contextName := range contextNames {
		s.reconcile(contextName)
	}
}

// checkWorkers is how many checks may be in flight at once, across the whole fleet. The budget
// that stops a first install running a credential helper per cluster in the same second.
const checkWorkers = 8

// checkLoop runs checks until stopped. Several workers, because a check is bounded by a remote
// server and a fleet would otherwise be probed one server at a time.
//
// Nothing is serialized per context, and nothing needs to be: the queue keys by check, so one
// check never runs twice at once, and a check writes only the observation it owns. Done is what
// re-arms a check an ask reached mid-run, rather than folding it into a run that had already
// passed the thing it asked about.
func (s *Service) checkLoop(ctx context.Context) {
	for {
		key, ok := s.checkQ.Next(ctx)
		if !ok {
			return
		}
		s.runCheck(ctx, key)
		s.checkQ.Done(key)
	}
}

// runCheck runs one check and commits what it found.
//
// The entry is read first and re-checked at the commit: the last holder can release mid-run and
// another caller re-claim the name, and the entry it gets is a different one — committing a run
// that predates it would answer about a claim nobody asked about. Keying the queue by check does
// not cover this; what raced is a release, not another run of the same check.
func (s *Service) runCheck(ctx context.Context, key checkKey) {
	s.mu.Lock()
	held := s.claimed[key.contextName]
	if held == nil || !wanted(held, key.check) {
		// Dropped before the run is marked, or an early return here would leave the check
		// reading as in flight — which due takes as a run that will reconcile when it
		// commits, so nothing would ever schedule it again.
		s.mu.Unlock()
		return
	}
	a := checkArgs{svc: s, contextName: key.contextName, conn: held.conn, kubecfgGen: s.kubecfgGen}
	connOK := held.state.Connection.OK()
	// Marked before the lock is dropped, so InFlight is true for as long as the request is out
	// and a reconcile landing meanwhile leaves the run alone.
	observations(&held.state)[key.check].begin(time.Now())
	s.mu.Unlock()

	c := checks[key.check]
	var apply commit
	if c.needsConnection && !connOK {
		// Recorded rather than run: a server the connection check could not reach would cost
		// this worker the full timeout to learn nothing.
		apply = dependencyFailed(key.check)
	} else {
		apply = c.run(ctx, a)
	}
	s.commitCheck(key, held, apply)
}

// wanted reports whether the entry holding the name now asked for this check.
//
// A queued key names a context, not the claim that queued it — so the last holder can release and
// another caller re-claim the name before a worker gets there. The replacement has reached no
// server yet, so it never scheduled anything behind reachability, and recording a dependency
// failure against it would show one for a connection nobody has tried. Same rule due applies when
// it schedules, which is what makes the two agree.
//
// It fires on one entry too: a connection that comes back forgets its failure, and the checks
// queued while it was down have nothing left to report.
func wanted(e *entry, id checkID) bool {
	return !checks[id].needsConnection || e.state.Phase() != PhasePending
}

// commitCheck files what a check found and asks for a reconcile. Called for every run, including
// one that concluded nothing: ending the run is what lets reconcile schedule this check again.
//
// The entry is re-checked under the lock, so a release landing mid-run cannot let a run that
// predates a re-claim be committed against whatever holds the name now.
func (s *Service) commitCheck(key checkKey, held *entry, apply commit) {
	s.mu.Lock()
	if s.claimed[key.contextName] != held {
		s.mu.Unlock()
		return
	}
	if apply != nil {
		apply(held)
	}
	// Ending the run and deriving what follows it happen together: end leaves the check with
	// nothing scheduled, which reads as suspended, and the pass is what replaces that. A reader
	// between the two would see a check that had quietly stopped.
	observations(&held.state)[key.check].end()
	s.reconcileLocked(key.contextName)
	s.mu.Unlock()
}

// reconcile works out what each of a context's checks should do and records it: whatever is due
// goes on the check queue, and the soonest thing due later gets the entry's one timer. **That
// timer is a wake, not a cadence** — it only brings this pass back, which decides again per check.
//
// **The schedule is derived, never armed.** Every path that changes an entry ends by running a
// pass rather than working out which checks its change affected — affordable, because this is
// state and arithmetic — so no path can leave a check un-armed by forgetting it. It is also the
// only thing that builds a checkKey, so no caller names a check, only the context that moved.
//
// And the only thing that publishes: deriving first is what makes every State a reader sees carry
// a schedule that matches the answers beside it.
func (s *Service) reconcile(contextName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.reconcileLocked(contextName)
}

// reconcileLocked is the pass itself, for a caller already holding the lock because the change it
// is reconciling has to land in the same critical section — see commitCheck.
func (s *Service) reconcileLocked(contextName string) {
	e := s.claimed[contextName]
	if e == nil {
		return
	}

	now := time.Now()
	obs := observations(&e.state)

	var soonest time.Time
	for id := range numChecks {
		if obs[id].InFlight() {
			// NextAttempt is that run. Writing a schedule over it would erase both the mark
			// saying it is still out — leaving a later pass free to ask for a second one —
			// and the schedule it was dispatched on. Its commit reconciles.
			continue
		}

		at := s.due(e, id, obs[id], now)
		obs[id].schedule(at)

		switch {
		case at.IsZero():
			// Suspended: nothing is due, and the last answer stands.
		case at.After(now):
			if soonest.IsZero() || at.Before(soonest) {
				soonest = at
			}
		default:
			s.checkQ.Add(checkKey{contextName: contextName, check: id})
		}
	}

	e.stopTimer()
	if !soonest.IsZero() {
		e.timer = time.AfterFunc(soonest.Sub(now), func() { s.reconcile(contextName) })
	}

	// State first, so a reader the signal wakes finds the value already published rather than
	// the one it replaced. It carries the timing too, which is why it goes every pass.
	//
	// The signal is measured against what the fleet was last told, not against this pass's own
	// starting point: several commits can land behind one reconcile, and only the difference
	// that survived them is news. Both sends are safe under the lock — each takes only its own
	// hub's lock, fans out into receiver slots, and calls no code of ours.
	s.stateHub.Sender().Send(contextName, e.state)
	if n := newsOf(e); n != e.published {
		e.published = n
		s.signalHub.Sender().Send(contextName, struct{}{})
	}
}

// due is when a check should next run, zero for one with nothing scheduled. The whole scheduling
// policy, in the order the cases have to be read. Asked only of a check that is not running —
// reconcile leaves one that is alone, since its NextAttempt is the run.
func (s *Service) due(e *entry, id checkID, a *Attempts, now time.Time) time.Time {
	last := a.LastAttempt

	switch {
	case id == checkConnection:
		switch {
		case e.kubecfgGen != s.kubecfgGen:
			// The file moved since this check last read it.
			return now
		case last.Reason == ReasonContextNotFound:
			// The file is the whole truth about a context's presence, and the watch
			// reports it moving. Polling would ask a question already answered.
			return time.Time{}
		case !last.Done():
			// It resolved, and nothing dials yet, so there is nothing to poll for. The
			// kubeconfig moving is what brings it back.
			return time.Time{}
		default:
			// It failed. Keep asking: the file can be fixed in a way the kubeconfig
			// service cannot see, such as a CA path that now opens.
			return last.FinishedAt.Add(s.intervals[id])
		}

	case e.state.Phase() == PhasePending:
		// Everything below reachability, before reachability has answered. Nothing to report
		// about a server nobody has tried yet, so these stay untouched rather than recording
		// a dependency that has not failed.
		return time.Time{}

	case !e.state.Connection.OK():
		// Reachability answered and the server is not there. One run records DependencyFailed
		// and the rest of the outage costs nothing, which is what keeps a dead cluster at one
		// timeout per cycle.
		if last.Reason == ReasonDependencyFailed {
			return time.Time{}
		}
		return now

	case last.Reason == ReasonUnsupported:
		// The endpoint is absent rather than failing, so it stays suspended for the life of
		// the connection.
		return time.Time{}

	case last.Reason == ReasonDependencyFailed:
		// Its dependency came back. This is the whole re-arm — nothing has to notice the
		// recovery and go looking for what suspended on it.
		return now

	case !last.Done():
		return now

	default:
		return last.FinishedAt.Add(s.intervals[id])
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

func (c *claim) Conn(context.Context) (*Connection, error) {
	return nil, fmt.Errorf("%w: %q", ErrNoConnection, c.contextName)
}

func (c *claim) State() State {
	state, _ := c.svc.read(c.contextName, c.entry)
	return state
}

func (c *claim) Departed() bool {
	_, departed := c.svc.read(c.contextName, c.entry)
	return departed
}

// read answers a claim from the entry it was made for. Once the pool no longer holds that entry
// — released, or dropped by Close — the claim reads as departed with nothing known.
func (s *Service) read(contextName string, held *entry) (State, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.claimed[contextName] == held {
		return held.state, held.departed
	}
	return State{}, true
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

// Release gives the claim back, dropping the entry once the last holder goes: an entry nobody
// holds is one nothing tracks. The CAS makes it idempotent while other holders remain, when the
// entry check below cannot tell a second release from a first.
func (c *claim) Release() {
	if !c.released.CompareAndSwap(false, true) {
		return
	}

	s := c.svc
	s.mu.Lock()
	defer s.mu.Unlock()

	// Only the entry the claim was made for: Close drops entries out from under the leases
	// still holding them, so a stale release must not decrement — or delete — whatever has
	// claimed the name since.
	if s.claimed[c.contextName] != c.entry {
		return
	}
	c.entry.holders--
	if c.entry.holders == 0 {
		delete(s.claimed, c.contextName)
		c.entry.stopTimer()
	}
}
