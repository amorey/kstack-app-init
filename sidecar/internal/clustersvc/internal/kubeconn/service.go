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

// Package kubeconn hands out leases on kube-contexts, and tells a lease's holders when the
// context it names leaves the kubeconfig.
//
// That is the whole of it today. The connection a lease will eventually hand out, and the probe
// that will validate it, are not here — every claim reads as nothing known, and Lease.Conn
// reports ErrNoConnection. What is here is the bookkeeping those need: who holds what, and what
// the kubeconfig still says about it.
//
// **One context, one entry.** Contexts that resolve to the same credentials are not merged: two
// aimed at one server as one user get an entry each, which costs a socket and a probe cycle once
// those exist, and buys a store that is one map, measurements that need no apportioning, and a
// failure that belongs to one caller.
//
// **A claim outlives what it is a claim on.** The kubeconfig can stop naming a context while a
// holder still holds it — the user deleted it, or is mid-edit — and the entry stays, because the
// file may name it again and the claim is how the holder hears about that. Only releasing drops
// an entry.
//
// **It never learns what a cluster is.** The caller names a kube-context; this package speaks
// contexts and whatever the kubeconfig says about them. Whether the server behind one is the
// cluster the caller meant is the caller's to decide.
package kubeconn

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// kubeconfigService is the reader this package asks whether a context still resolves, and which
// tells it when the file changed. The fingerprint it returns beside the config names the
// connection those credentials describe; nothing here uses it until connections come back.
type kubeconfigService interface {
	RESTConfig(contextName string) (*rest.Config, string, error)
	Subscribe() kubeconfig.Subscription
}

// Subscription reports the contexts whose news changed, one event per context, for a reader
// whose reaction to any of them is the same: go re-read that claim.
//
// A keyed bus, not a fan-out ring: it holds a slot per context, so a fleet changing at once
// neither loses a context behind a busier one nor bounds what is remembered by a buffer length.
// The value carries nothing — the key is the news.
type Subscription = *conflate.Receiver[string, struct{}]

// StateSubscription carries each State a probe publishes for one claim. Close it when done — an
// abandoned one keeps its slot.
//
// **Nothing is delivered on attach.** A watcher reads Lease.State for what is known now and this
// for what comes after; the bus hands back no current value.
//
// Keyed by context, not by the credentials behind it. A receiver is bound to its key for life,
// and credentials move under a context that never does.
//
// **Every value is a level, never an edge.** The hub keeps the latest per key, so a reader that
// falls behind skips what came between — a gap means it missed some, not that nothing happened.
type StateSubscription = *watch.Receiver[string, State]

// Lease is a claim on one kube-context: what keeps the pool tracking it, and how a holder hears
// about it. An interface because it crosses out of this package, where a caller's test has to
// stand in for a live cluster.
type Lease interface {
	// Conn is the connection to the context's server.
	//
	// Nothing builds one yet, so this always reports ErrNoConnection.
	Conn(ctx context.Context) (*Connection, error)
	// State is what the last probe read, check by check. It never dials.
	//
	// Nothing probes yet, so this always reads as nothing known.
	State() State
	// WatchState carries every State a probe publishes for this claim. It delivers nothing on
	// attach, so a watcher pairs it with State for what is known now. Release ends it.
	WatchState() StateSubscription
	// Departed reports that the kubeconfig no longer names this claim's context.
	//
	// Not an error and not final: the file may name it again, and this claim is how the holder
	// hears about that. A holder that wants to be told rather than to ask watches Subscribe or
	// WatchState — both fire when this flips.
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
	// presenceHub is the work queue: a context whose presence in the kubeconfig has to be
	// re-read. Keyed by context and coalescing, so a burst asks once.
	//
	// presenceWork is its receiver, taken in New rather than in Start: a send with no receiver
	// is dropped, so a claim taken before the loop runs would never be checked.
	presenceHub  *conflate.Hub[string, struct{}]
	presenceWork Subscription

	// mu guards the map and the entries in it together: a holder count and a presence flag are
	// read against each other, and nothing may see one without the other.
	mu sync.Mutex
	// claimed is one entry per claimed context — a key is here exactly while someone holds
	// that context — and is also the key both hubs publish under.
	claimed map[string]*entry

	wg sync.WaitGroup
}

// entry is what the pool holds for one claimed context.
type entry struct {
	holders int
	// departed is what the last read of the kubeconfig said: false while the file names this
	// context, true once it stops. Flipping it is the news this package exists to deliver.
	departed bool
	// state is what a probe read. Nothing probes, so it stays zero.
	state State
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfgSvc kubeconfigService) *Service {
	presenceHub := conflate.New[string, struct{}]()
	return &Service{
		kubecfgSvc:   kubecfgSvc,
		signalHub:    conflate.New[string, struct{}](),
		stateHub:     watch.New[string, State](),
		presenceHub:  presenceHub,
		presenceWork: presenceHub.Receiver(),
		claimed:      map[string]*entry{},
	}
}

// Acquire claims contextName and hands back the lease on it. It never fails and never waits: a
// context the kubeconfig does not name yet is claimable, because the file may name it later and
// the claim is how the holder finds out.
//
// A new context's presence is asked for, never read here: reading the kubeconfig is not work to
// do on the caller's thread or under this lock. A later holder joins what the first one's check
// found and asks for nothing.
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
		s.presenceHub.Sender().Send(contextName, struct{}{})
	}
	return &claim{svc: s, contextName: contextName, entry: e}
}

// Subscribe reports every context whose news changed, for a reader whose reaction to any of them
// is the same. A holder that cares about one claim watches that claim instead.
func (s *Service) Subscribe() Subscription { return s.signalHub.Receiver() }

// releaseContext gives back one holder's claim on contextName, dropping the entry once the last
// one goes. An entry nobody holds is one nothing tracks.
//
// **Only the entry the claim was made for.** Close drops entries out from under the leases still
// holding them, so a lease released afterwards would otherwise decrement whatever has claimed
// its name since — and could delete an entry belonging to a claim that has nothing to do with it.
//
// The decrement and the drop are one critical section: a claim taken between them would be on an
// entry this call then deletes, and would read as nothing known for as long as it is held.
func (s *Service) releaseContext(contextName string, held *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.claimed[contextName]
	if e != held {
		return
	}
	e.holders--
	if e.holders == 0 {
		delete(s.claimed, contextName)
	}
}

// checkPresence re-reads whether the kubeconfig still names contextName, and tells its holders
// if that changed. **The one thing this service does.**
//
// Read outside the lock: RESTConfig reads the shared config, and holding this pool's lock across
// it would order every reader behind that read.
//
// **Only a missing context is a departure.** ErrContextNotFound is the one error that means the
// file stopped naming it; every other resolve failure — a cluster entry it points at that is
// gone, credentials that will not load — is a context the file still names and a file the user
// has to fix. Reporting those as departures would say the context left when it did not.
//
// An unread kubeconfig names nothing, and is not a departure either. Reporting one would tell
// every holder its context is gone for as long as the first read takes.
func (s *Service) checkPresence(contextName string) {
	// Taken before the read so the commit can tell whether it is still answering about this
	// entry, and skipped entirely for a context nobody claims.
	held := s.entryFor(contextName)
	if held == nil {
		return
	}

	_, _, err := s.kubecfgSvc.RESTConfig(contextName)
	if errors.Is(err, kubeconfig.ErrNotRead) {
		return
	}
	s.setDeparted(contextName, held, errors.Is(err, kubeconfig.ErrContextNotFound))
}

func (s *Service) entryFor(contextName string) *entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.claimed[contextName]
}

// setDeparted records what the read found and, when that was news, tells contextName's holders.
//
// **Only onto the entry the read was taken for.** The last holder can release while a read is in
// flight and another caller re-claim the same name, and the entry it gets is a different one —
// answering it from a read that predates it would depart a claim over a context nobody asked
// about. Serializing the queue does not cover this: what raced is a release, not another read.
//
// **Committed and announced under one lock**, for the same reason: releasing it between the two
// would publish the old read as news for whatever claimed the name next. Both sends are safe
// here — each takes only its own hub's lock, fans out into receiver slots, and calls no code of
// ours, so neither blocks on a consumer.
//
// A context already in this state is unchanged: every kubeconfig write would otherwise
// re-announce every departed context for as long as its claim is held.
func (s *Service) setDeparted(contextName string, held *entry, departed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.claimed[contextName]
	if e != held || e.departed == departed {
		return
	}
	e.departed = departed

	// State first, so a reader the poke wakes finds the value already published rather than the
	// one it replaced.
	s.stateHub.Sender().Send(contextName, e.state)
	s.signalHub.Sender().Send(contextName, struct{}{})
}

// stateOf is what the claim on contextName reads, and departedContext whether its context is
// still named. Both answer for an entry the pool no longer holds — released, or dropped by
// Close — which is what a claim outliving its entry reads.
func (s *Service) stateOf(contextName string, held *entry) State {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.claimed[contextName] == held {
		return held.state
	}
	return State{}
}

func (s *Service) departedContext(contextName string, held *entry) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.claimed[contextName] == held {
		return held.departed
	}
	return true
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

func (c *claim) State() State { return c.svc.stateOf(c.contextName, c.entry) }

func (c *claim) Departed() bool { return c.svc.departedContext(c.contextName, c.entry) }

// WatchState takes no baseline: the hub compares against one only through an Accept, and this
// one has none, so every value is delivered. What a baseline is for — reading and registering in
// one critical section, so a publish landing between the two is not skipped — is worth having
// once a probe can land at all.
func (c *claim) WatchState() StateSubscription {
	return c.svc.stateHub.Watch(c.contextName)
}

func (c *claim) Release() {
	if c.released.CompareAndSwap(false, true) {
		c.svc.releaseContext(c.contextName, c.entry)
	}
}

// Start runs the two loops: the one that reads presence, and the watch that asks it to read again
// as the kubeconfig moves. The kubeconfig subscription is taken before Start returns, so nothing
// it says in between is dropped.
//
// **A claim taken before Start is served once it runs.** The queue's receiver is taken in New,
// because a send with no receiver is dropped — a claim made before Start would otherwise never be
// checked, and a context that had already gone would read as present until some later kubeconfig
// change happened to ask again.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	// Not Start's context, which bounds startup: this one bounds the loops, so it lives until
	// the stop func cancels them.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	s.wg.Go(func() {
		defer s.presenceWork.Close()
		s.presenceLoop(loopCtx, s.presenceWork)
	})

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

// presenceLoop reads one context at a time until stopped. **Serial on purpose**: two reads of one
// context racing could record the older answer over the newer, leaving a claim reporting a
// context the file has since named again.
func (s *Service) presenceLoop(ctx context.Context, work Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-work.Chan():
			if !ok {
				return
			}
			s.checkPresence(ev.Key)
		}
	}
}

// watchKubeconfig asks for every claimed context to be re-read on every change, until stopped.
// Cancellation ends the wait: the service behind the feed is the app's and is not required to
// close its channel when released.
//
// Every context rather than the ones that moved, because the feed carries a whole config and
// working out which contexts changed is what checkPresence does anyway.
func (s *Service) watchKubeconfig(ctx context.Context, cfgs kubeconfig.Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-cfgs.Chan():
			if !ok {
				return
			}
			for _, contextName := range s.claimedContexts() {
				s.presenceHub.Sender().Send(contextName, struct{}{})
			}
		}
	}
}

// claimedContexts is the contexts to re-read, snapshotted so the loop above holds no lock while
// it asks. Unordered: each read is independent, and the queue coalesces per context anyway.
//
// A slice rather than the iterator maps.Keys hands back, which reads the live map as it goes:
// ranging that after this returns would walk the map with the lock already released.
func (s *Service) claimedContexts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Collect(maps.Keys(s.claimed))
}

// Close drops what the pool holds. Claims are not released for their holders: a claim outliving
// the pool is the holder's bug, and reading as nothing known is what a dropped entry already
// does.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.claimed)
	return nil
}
