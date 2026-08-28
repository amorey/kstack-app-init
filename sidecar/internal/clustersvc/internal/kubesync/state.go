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

// What a sweep and a mirror stand behind, and this leaf's reason vocabulary. The two
// states differ in shape because the two workers differ in kind: a sweep is a set of
// passes, and a mirror is a live stream.
package kubesync

import (
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/supervisor"
)

// The supervisor's bookkeeping, aliased rather than copied so an Observation carries exactly
// what it recorded — the same aliases kubeconn declares, for the same reason. Reason
// stays this package's vocabulary; the supervisor treats it as opaque.
type (
	Observation[T any] = supervisor.Observation[T]
	Attempt            = supervisor.Attempt
	Attempts           = supervisor.Attempts
	Verdict            = supervisor.Verdict
	Reason             = supervisor.Reason
)

// The discovery vocabulary. There is no Watching or Stale here: nothing behind a sweep
// can prove itself live, so Discovering/Discovered are what Syncing/Watching are for a
// collection that can only be listed.
const (
	// ReasonNoConnection: nothing has reached the server, so the session is suspended.
	ReasonNoConnection = "NoConnection"
	// ReasonIdentityMismatch: the context's connection does not answer as ServerUID.
	ReasonIdentityMismatch = "IdentityMismatch"
	// ReasonDiscovering: a sweep is running and none has answered yet.
	ReasonDiscovering = "Discovering"
	// ReasonDiscovered: a sweep has answered; the kind set is current.
	ReasonDiscovered = "Discovered"
	// ReasonPartial: the fan-out committed what it read, and some group-versions did not
	// answer. Its own verdict because neither neighbour is honest about a sweep where
	// twelve of fourteen legs worked — Discovered would reset the backoff ladder over an
	// aggregated API that is down, and DiscoveryFailed would climb it while most of the
	// catalog refreshed fine.
	ReasonPartial = "Partial"
	// ReasonDiscoveryFailed: the sweep failed and is retrying.
	ReasonDiscoveryFailed = "DiscoveryFailed"
)

// The per-kind vocabulary. A kind reports NoConnection and IdentityMismatch too: the
// session suspends every worker under it, and each says so for itself.
const (
	// ReasonResyncing: the cache holds this kind's rows, but the position they were current
	// at is one the server no longer serves from, so they are being reconciled against a
	// fresh list. Its own reason because the rows are still served throughout — unlike
	// Syncing, where there is nothing to serve.
	ReasonResyncing = "Resyncing"
	// ReasonSyncing: cold-listing a kind with nothing cached.
	ReasonSyncing = "Syncing"
	// ReasonResuming: re-establishing from a cookie, and slow enough to be worth saying.
	ReasonResuming = "Resuming"
	// ReasonWatching: caught up and streaming deltas, proven live.
	ReasonWatching = "Watching"
	// ReasonStale: caught up, but the watch has stopped proving itself alive.
	ReasonStale = "Stale"
	// ReasonSyncFailed: the run failed and is retrying.
	ReasonSyncFailed = "SyncFailed"
)

// DiscoveryState is the sweep's standing answer for one cache: a verdict, and the three
// reads behind it.
//
// **It carries nothing a sweep found.** The kinds go to kind_catalog and are read back
// from there, and which group-versions a cluster serves is that table's business too.
// What is left is how discovery is DOING.
//
// Each read is accounted for on its own, so one failing says so without dragging the
// others' verdict with it, and Observation carries the supervisor's own record — the last
// attempt's verdict, reason and message, the next attempt, the failure streak — so
// nothing here restates it.
type DiscoveryState struct {
	// Reason and Message are the whole sweep's verdict, and the one thing here not
	// readable off a probe: which of them decides is a PRECEDENCE rule — a suspended
	// session over a failing read, a failing read over one that has yet to answer. It is
	// made here because the news feed has to gate on it either way, and a boundary
	// folding its own would fold it differently.
	Reason  string
	Message string

	// What each read commits is what the next one needs, and no more. Resources commits
	// the FINGERPRINT it wrote to kind_catalog rather than the catalog itself: the rows
	// belong on disk, and a fingerprint is all "the answer moved" requires.
	APIVersions Observation[[]string] // GET /api — the core group's versions
	APIGroups   Observation[[]string] // GET /apis — the group-versions served, the fan-out's input
	Resources   Observation[uint64]   // the fan-out — the catalog fingerprint it committed
}

// KindState is one mirror's standing answer — a live stream's, where DiscoveryState is a
// set of passes, and the difference decides every field here. It carries no identity: the
// caller named the kind to read it.
//
// A pass is judged by its last attempt. A stream is judged by whether it is established
// now and has recently proven itself alive, which is a different question and needs
// different evidence:
//
//   - **Silence is ambiguous.** A watch sending nothing is either a quiet collection or a
//     wedged connection. LastLiveAt is what separates them — a delta OR a bookmark, since
//     bookmarks exist to make an idle watch prove itself — and Stale is what it reads as
//     once that proof ages past the threshold.
//   - **A healthy stream has no next attempt.** NextRetryAt is set only while a run is
//     down, where a pass always has one scheduled.
//   - **Flapping hides at an instant.** A watch that reconnects every thirty seconds reads
//     Watching whenever it is read; Restarts is the only field that says otherwise.
type KindState struct {
	Reason  string
	Message string
	// SinceAt is when Reason last moved — "watching since 10:02", which is what a stream
	// has instead of a last-attempt stamp.
	SinceAt time.Time

	// LastUpdateAt is when data last arrived; LastLiveAt the last proof the stream is
	// live, which is the later of the two and the only one that distinguishes idle from
	// wedged.
	//
	// No row count: kind_counts is trigger-maintained, so one here would be a stale copy
	// of a number the database keeps authoritatively, bought with a query per commit. A
	// consumer reads Store.Kinds, which joins every kind's count in one go.
	LastUpdateAt time.Time
	LastLiveAt   time.Time

	// Restarts counts runs this worker has begun without settling; NextRetryAt is when a
	// run that is down will be tried again, and zero while one is up.
	Restarts    int
	NextRetryAt time.Time
}

// setReason moves the reason, and SinceAt with it only when it changed: SinceAt is when the
// reason last moved, which is what "watching since 10:02" reads off.
func (st *KindState) setReason(reason Reason, message string) {
	if st.Reason != string(reason) {
		st.SinceAt = time.Now()
	}
	st.Reason, st.Message = string(reason), message
}
