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

// What is checked about a server. Each of State's five observations has one check behind it, and
// each check runs on its own schedule: a cluster's UID never moves and its readiness moves
// constantly, so nothing is gained by asking about them together. **A check writes the observation
// it owns and no other**, which is what lets the five run at once against one entry without
// coordinating. When each one runs is the pool's to decide — see Service.due.
//
// **Nothing dials yet.** The connection check resolves the kubeconfig and reports a context that
// left it; the four behind it record DependencyFailed and suspend, which is what they are
// supposed to do while the connection is down, and stays their behavior once one is built.
package kubeconn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// checkID names the observation a check owns. **A check writes that observation and no other**,
// which is what lets the five run at once against one entry without coordinating.
type checkID int

const (
	checkConnection checkID = iota
	checkReadiness
	checkServerUID
	checkServerVersion
	checkPrincipal
	// numChecks counts them; not a check itself. Every per-check table is sized by it, so a
	// sixth check is a compile error in any table that forgets it.
	numChecks
)

// checkNames is its own array rather than a field on check: checks refers to String through the
// unimplemented bodies, so a name in that table would be an initialization cycle.
var checkNames = [numChecks]string{
	checkConnection:    "connection",
	checkReadiness:     "readiness",
	checkServerUID:     "serverUID",
	checkServerVersion: "serverVersion",
	checkPrincipal:     "principal",
}

func (c checkID) String() string {
	if c < 0 || c >= numChecks {
		return fmt.Sprintf("check(%d)", int(c))
	}
	return checkNames[c]
}

// checkKey is one check of one context — the unit the work queue keys on, so a check is never in
// two workers at once and an ask arriving mid-run earns a fresh run rather than joining it. Only
// reconcile builds one.
type checkKey struct {
	contextName string
	check       checkID
}

// commit is what a check concluded, applied to the entry under the pool's lock. It records what
// the run found and nothing about when to run again — that is derived afterwards, from this.
//
// The request happens before the lock is taken and only the write happens under it, so a check
// never orders other callers behind a remote server.
type commit func(e *entry)

// checkArgs is what a check is handed: the context it is checking, and what the pool knew about
// it when the run was dispatched.
type checkArgs struct {
	svc         *Service
	contextName string
	conn        *Connection
	// kubecfgGen is the kubeconfig generation this run sees. The connection check stamps it on
	// the entry, which is how the next reconcile knows the file has been read since it moved.
	kubecfgGen uint64
}

// check is one observation's owner.
type check struct {
	// needsConnection marks the four checks behind reachability: a server nothing reached
	// cannot answer them, so they are recorded rather than dispatched while it is down. The
	// dependency is declared here rather than tested inside each body, which would be four
	// copies of one rule.
	needsConnection bool
	run             func(ctx context.Context, a checkArgs) commit
}

var checks = [numChecks]check{
	checkConnection:    {run: runConnectionCheck},
	checkReadiness:     {needsConnection: true, run: unimplemented(checkReadiness)},
	checkServerUID:     {needsConnection: true, run: unimplemented(checkServerUID)},
	checkServerVersion: {needsConnection: true, run: unimplemented(checkServerVersion)},
	checkPrincipal:     {needsConnection: true, run: unimplemented(checkPrincipal)},
}

// defaultIntervals is how long after a run each check is due again. A cluster's UID outlives
// everything else here; its readiness outlives nothing.
var defaultIntervals = [numChecks]time.Duration{
	checkConnection:    30 * time.Second,
	checkReadiness:     30 * time.Second,
	checkServerUID:     10 * time.Minute,
	checkServerVersion: 5 * time.Minute,
	checkPrincipal:     5 * time.Minute,
}

// runConnectionCheck answers whether the server behind contextName can be reached, and owns the
// context's lifecycle on the way there: resolving the kubeconfig is the first step of reaching a
// server, so a context that left the file is this check's to find.
//
// **Every branch stamps the generation it read at**, including the one that found nothing to
// read. That stamp is what stops the next reconcile asking again for a file this run has seen.
//
// Nothing dials yet, so a context that resolves records no attempt — resolving is the
// precondition, not the answer, and Phase stays PhasePending. A context that cannot resolve is a
// definitive answer, and does record one.
func runConnectionCheck(_ context.Context, a checkArgs) commit {
	startedAt := time.Now()
	_, _, err := a.svc.kubecfgSvc.RESTConfig(a.contextName)
	att := Attempt{StartedAt: startedAt, FinishedAt: time.Now(), Err: err}

	switch {
	case errors.Is(err, kubeconfig.ErrNotRead):
		// An unread kubeconfig names nothing, and saying so would report every context gone
		// for as long as the first read takes. Nothing was concluded, so nothing may be left
		// behind to conclude from: a retained failure would still be deriving a retry, and
		// this run would leave it due again — a spin for as long as the file stays unread.
		return func(e *entry) {
			e.kubecfgGen = a.kubecfgGen
			e.state.Connection.forget()
		}

	case errors.Is(err, kubeconfig.ErrContextNotFound):
		att.Reason, att.Message = ReasonContextNotFound, "kubeconfig no longer names this context"
		return func(e *entry) {
			e.kubecfgGen = a.kubecfgGen
			e.departed = true
			e.state.Connection.record(att)
		}

	case err != nil:
		// The file still names it, so this is not a departure — it is a file the user has to
		// fix, and no backoff can fix a file.
		att.Reason, att.Message = ReasonResolveFailed, err.Error()
		return func(e *entry) {
			e.kubecfgGen = a.kubecfgGen
			e.departed = false
			e.state.Connection.record(att)
		}

	default:
		return func(e *entry) {
			e.kubecfgGen = a.kubecfgGen
			e.departed = false
			// A context that resolves again outlives whatever the last failure said, and
			// nothing dials yet to replace it — so the check goes back to unattempted
			// rather than leaving the phase at a server it never reached.
			observations(&e.state)[checkConnection].forget()
		}
	}
}

// unimplemented stands in for a check with no request behind it yet. Unreachable while nothing
// dials — every one of these depends on a connection that never comes up — and it records rather
// than going quiet, so a run that does reach it says so instead of looking suspended for a reason
// nobody wrote down.
func unimplemented(id checkID) func(context.Context, checkArgs) commit {
	return func(context.Context, checkArgs) commit {
		now := time.Now()
		att := Attempt{
			StartedAt:  now,
			FinishedAt: now,
			Reason:     ReasonInternal,
			Message:    id.String() + " check is not implemented",
		}
		return func(e *entry) { observations(&e.state)[id].record(att) }
	}
}

// dependencyFailed is what a check behind the connection records instead of dialing a server
// nothing reached. Recorded rather than attempted — which is why it has no StartedAt — and it has
// to be written rather than skipped, or nothing says why the check is suspended.
func dependencyFailed(id checkID) commit {
	att := Attempt{
		FinishedAt: time.Now(),
		Reason:     ReasonDependencyFailed,
		Message:    "no connection to the server",
	}
	return func(e *entry) { observations(&e.state)[id].record(att) }
}
