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

// Package kubeconn hands out leases on kube-contexts and tells a lease's holders when the
// context it names leaves the kubeconfig.
//
// That is the whole of it today: nothing builds a connection or probes one yet, so Lease.Conn
// reports ErrNoConnection and State reads as nothing known. What is here is the bookkeeping
// those will need — who holds which context, and whether the kubeconfig still names it.
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
	// State is what the last probe read, check by check. It never dials.
	//
	// Nothing probes yet, so this always reads as nothing known.
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
	// presenceHub is the work queue: a context whose presence in the kubeconfig has to be
	// re-read. Keyed and coalescing, so a burst asks once.
	//
	// presenceWork is its receiver, taken in New rather than in Start: a send with no
	// receiver is dropped, so a claim taken before the loop runs would never be checked.
	presenceHub  *conflate.Hub[string, struct{}]
	presenceWork Subscription

	// mu guards claimed and the entries in it together: a holder count and a departed flag
	// are read against each other, and nothing may see one without the other.
	mu sync.Mutex
	// claimed holds one entry per claimed context — a key is here exactly while someone
	// holds that context — and is also the key both hubs publish under.
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
// The first holder of a context queues a presence check rather than reading the kubeconfig here
// — not work to do on the caller's thread or under the lock. A later holder joins what the first
// one's check found.
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

// Start runs the two loops: the one that reads presence, and the watch that asks it to read
// again as the kubeconfig moves. The kubeconfig subscription is taken before Start returns, so
// nothing it says in between is dropped. The presence queue's receiver was taken in New, so a
// claim taken before Start is served once the loop runs.
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

// Close drops what the pool holds. Claims are not released for their holders: a claim outliving
// the pool is the holder's bug, and reading as nothing known is what a dropped entry already
// does.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.claimed)
	return nil
}

// presenceLoop reads one context at a time until stopped. Serial on purpose: two reads of one
// context racing could commit the older answer over the newer, leaving a claim reporting a
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

// watchKubeconfig queues a presence check for every claimed context on every change, until
// stopped. Every context rather than the ones that moved, because the feed carries a whole
// config and working out which contexts changed is what checkPresence does anyway.
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
			for _, contextName := range s.claimedContexts() {
				s.presenceHub.Sender().Send(contextName, struct{}{})
			}
		}
	}
}

// claimedContexts snapshots the claimed contexts so watchKubeconfig holds no lock while it
// queues them. A slice rather than the maps.Keys iterator, which would walk the live map after
// the lock is released.
func (s *Service) claimedContexts() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Collect(maps.Keys(s.claimed))
}

// checkPresence re-reads whether the kubeconfig still names contextName and, when that changed,
// tells the holders. The one thing this service does.
//
// The read runs outside the lock — RESTConfig reads the shared config, and holding the pool's
// lock across it would order every other caller behind that read. The entry is captured first so
// the commit can tell whether it is still answering about the same claim: the last holder can
// release mid-read and another caller re-claim the name, and the entry it gets is a different
// one — committing a read that predates it would depart a claim nobody asked about. Serializing
// the queue does not cover this: what raced is a release, not another read.
//
// Only a missing context is a departure. ErrContextNotFound is the one error that means the file
// stopped naming it; every other resolve failure — a cluster entry that is gone, credentials
// that will not load — is a context the file still names and a file the user has to fix. And an
// unread kubeconfig names nothing: reporting it as a departure would tell every holder its
// context is gone for as long as the first read takes.
//
// An unchanged answer publishes nothing, or every kubeconfig write would re-announce every
// departed context for as long as its claim is held. Commit and announce happen under one lock,
// so a release landing between the two cannot let the old read become news for whatever claims
// the name next. Both sends are safe under it — each takes only its own hub's lock, fans out
// into receiver slots, and calls no code of ours.
func (s *Service) checkPresence(contextName string) {
	s.mu.Lock()
	held := s.claimed[contextName]
	s.mu.Unlock()
	if held == nil {
		return
	}

	_, _, err := s.kubecfgSvc.RESTConfig(contextName)
	if errors.Is(err, kubeconfig.ErrNotRead) {
		return
	}
	departed := errors.Is(err, kubeconfig.ErrContextNotFound)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.claimed[contextName] != held || held.departed == departed {
		return
	}
	held.departed = departed

	// State first, so a reader the poke wakes finds the value already published rather than
	// the one it replaced.
	s.stateHub.Sender().Send(contextName, held.state)
	s.signalHub.Sender().Send(contextName, struct{}{})
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
	}
}
