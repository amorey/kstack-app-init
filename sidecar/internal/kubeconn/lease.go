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

// The claim a caller holds on a set of credentials, and what it reads through: whether a
// probe has validated them, and the connection once one has. The loop behind it is
// loop.go's.
package kubeconn

import (
	"context"
	"sync"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"

	"k8s.io/client-go/rest"
)

// Result is one probe's outcome for a set of credentials. It names no credentials of its
// own: it is stored under the key it ran against, so a rotation cannot be told the wrong
// cluster's news — the new credentials are a different key with its own result.
type Result struct {
	Identity Identity
	Err      error
}

// Failed reports whether the cluster would not answer.
func (r Result) Failed() bool { return r.Err != nil }

// sameAs reports whether this result says what prev already said. The error compares by
// text, as Identity's does: connection-refused becoming a 401 is a different answer, and a
// caller reporting the old reason indefinitely is the failure that hides.
func (r Result) sameAs(prev *Result) bool {
	if prev == nil {
		return false
	}
	return r.Identity.sameAs(prev.Identity) && errText(r.Err) == errText(prev.Err)
}

// State is what this package knows about a set of credentials: the last probe's outcome,
// and whether one is asked for and unanswered. Read together, since a caller deciding
// what to report must not have a probe finish between two reads and conclude none ever
// ran.
type State struct {
	// Last is the last probe's outcome, or nil when none has landed. Nil is "no news",
	// never "no identity" — credentials nobody has probed read the same as ones nobody
	// has asked about, and a caller folds both as "keep what you have".
	Last *Result
	// Probing with a nil Last is the one state worth reporting on its own: these
	// credentials are being tried for the first time since anything asked.
	Probing bool
}

// Subscribe reports that these credentials moved: a probe started with nothing known yet,
// or one landed saying something new. Close the receiver when done — an abandoned one keeps
// its slot.
//
// The value is empty. It says that State is worth re-reading, not what State now says, so a
// reader acts on what is current rather than on a snapshot taken when the probe landed.
func (s *Service) Subscribe(key string) *conflate.Receiver[string, struct{}] {
	return s.newsHub.Receiver(s.newsHub.WithKeyFilter(func(k string) bool { return k == key }))
}

// Lease is a claim on the connection for one set of credentials. While one is held they
// re-probe on the cadence, so what Conn hands back has been validated recently; with none
// held they are probed only when something asks.
//
// An interface for the one reason a single implementation earns one: it crosses out of
// this package, where a caller's test has to stand in for a live cluster.
type Lease interface {
	// Conn is the connection, once a probe has validated it.
	Conn(ctx context.Context) (*Connection, error)
	// Release drops the claim. Idempotent, so it is safe to defer.
	Release()
}

// lease is the claim a live Service hands out.
type lease struct {
	svc *Service
	key string
	// e is the entry this claim was counted on, held so Release finds it without the
	// map — Close empties that.
	e *entry
	// once makes Release idempotent, which is what lets a caller defer it and still
	// release early on an error path.
	once sync.Once
}

// Acquire claims the connection for one set of credentials and arms their cadence,
// building the connection if this is the first caller. It does not wait for a probe —
// Conn is what waits — so a caller can hold a claim across a cluster being down without
// running a retry loop of its own.
func (s *Service) Acquire(cfg *rest.Config, key string) (Lease, error) {
	e, err := s.entryFor(cfg, key)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Asked for before the claim is counted, so a refusal leaves no phantom holder behind.
	// The count may have just been zero, in which case there is no loop to arm.
	if err := s.demand(key, e); err != nil {
		return nil, err
	}
	e.leases++
	return &lease{svc: s, key: key, e: e}, nil
}

// Release drops the claim. Dropping the last one stops the cadence: the loop ends after
// the probe it is on, so credentials nobody is using cost nothing. The connection stays
// pooled — Close is what drops those.
func (l *lease) Release() {
	l.once.Do(func() {
		l.svc.mu.Lock()
		defer l.svc.mu.Unlock()

		l.e.leases--
		if l.e.leases == 0 {
			// Or the loop waits out the cadence first and runs a probe nobody is waiting
			// for — which, for credentials that dial through a helper, is a subprocess and
			// possibly a prompt.
			l.e.nudge()
		}
	})
}

// Conn is the connection, once a probe has validated it.
//
// Credentials whose last probe succeeded answer immediately. Anything else waits for the
// probe now running — its own outcome, not a successful one, so a down cluster answers
// rather than hanging, and the caller decides whether to ask again. It does not kick: the
// claim already has the loop probing, on the backoff ladder while the cluster is down,
// and kicking per call would flatten it.
func (l *lease) Conn(ctx context.Context) (*Connection, error) {
	// Read and register in one critical section, so a probe landing in between is
	// delivered rather than missed. watch.Watch calls no caller code, which is what
	// makes it safe here.
	s := l.svc
	s.mu.Lock()
	res := l.e.result
	var next *watch.Receiver[string, *Result]
	if res == nil || res.Failed() {
		next = s.resultsHub.Watch(l.key)
	}
	s.mu.Unlock()

	if next != nil {
		defer next.Close()

		ev, err := next.RecvContext(ctx)
		if err != nil {
			return nil, err
		}
		res = ev.Value
	}
	if res.Err != nil {
		return nil, res.Err
	}
	// The entry's own connection, not one the result carries: an entry is one set of
	// credentials for its whole life, so what was probed and what is handed back cannot
	// come apart.
	return l.e.conn, nil
}

// ProbeNow asks for one probe of these credentials and does not wait for the outcome,
// which reaches a Subscribe reader. It claims nothing, so credentials nobody holds are
// probed once and the loop ends again.
func (s *Service) ProbeNow(cfg *rest.Config, key string) error {
	e, err := s.entryFor(cfg, key)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return s.demand(key, e)
}

// State reports what is known about a set of credentials. Ones the pool does not hold
// read as the zero value, which says the same as ones nobody has probed: no news.
func (s *Service) State(key string) State {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.conns[key]
	if !ok {
		return State{}
	}
	return State{Last: e.result, Probing: e.probing}
}
