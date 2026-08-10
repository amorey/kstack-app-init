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

package cluster

import (
	"slices"
	"time"
)

// syncHealthAcc folds one cache's per-kind records into a single verdict.
type syncHealthAcc struct {
	total int
	// worstRank/worstReason track the most severe reason seen; offender names the
	// kinds behind it (the reason is carried along, not mapped back from the rank).
	worstRank   int
	worstReason string
	offender    []SyncedKindRef
	// paused names (not counts) the kinds observed Paused: a pause relays to a
	// cache's children one at a time, so a partly-paused cache is a state a UI sees.
	paused []SyncedKindRef
	// notWatching counts every kind not observed Watching — ranked AND paused; the
	// offender list can't answer this (it resets to one name when a worse rank appears).
	notWatching int
	// unconfirmed counts kinds nobody in THIS process has observed — neither healthy
	// nor broken, and the verdict must not round them to either.
	unconfirmed int

	lastUpdateAt *time.Time
	lastLiveAt   *time.Time
}

// syncReasonRank orders per-kind reasons by how much they dominate a cache's
// verdict: failure > stall > wait > catch-up. A pause is not in the ladder — it is
// the mildest reading, reached only when nothing ranked or unobserved is left.
func syncReasonRank(reason string) int {
	switch reason {
	case ReasonSyncFailed:
		return 4
	case ReasonStale:
		return 3
	case ReasonNoConnection:
		return 2
	case ReasonSyncing:
		return 1
	case ReasonWatching, ReasonPaused:
		// Not faults; never reached (add handles both first). Named so default
		// means "unrecognized".
		return 0
	default:
		// An unfamiliar spelling is DEGRADED, not healthy (per the schema); ranked
		// at the bottom so it registers without masking a known reason.
		return 1
	}
}

func (a *syncHealthAcc) add(rec gvrSyncRec, st ClusterCacheGVRSyncStats) {
	a.total++
	{
		// Newest write anywhere in the cache; oldest proof among the kinds that have one.
		if st.LastUpdateAt != nil && (a.lastUpdateAt == nil || st.LastUpdateAt.After(*a.lastUpdateAt)) {
			a.lastUpdateAt = st.LastUpdateAt
		}
		if st.LastLiveAt != nil && (a.lastLiveAt == nil || st.LastLiveAt.Before(*a.lastLiveAt)) {
			a.lastLiveAt = st.LastLiveAt
		}
	}

	cond := FindCondition(rec.conditions, ConditionSynced)
	// Only an OBSERVED Watching is health: an unconfirmed or absent condition
	// counts toward notWatching, or UnhealthyKinds would disagree with an Unknown
	// verdict right after a restart. See docs/adr/2026-08-09-liveness-conditions.md.
	if cond != nil && !cond.Unconfirmed && cond.Reason == ReasonWatching {
		return
	}
	a.notWatching++

	// Neither healthy nor broken — never assert a pre-restart verdict, nor health
	// on its absence. See verdict.
	if cond == nil || cond.Unconfirmed {
		a.unconfirmed++
		return
	}
	if cond.Reason == ReasonPaused {
		a.paused = append(a.paused, rec.kindRef())
		return
	}
	rank := syncReasonRank(cond.Reason)
	switch {
	case rank > a.worstRank:
		a.worstRank, a.worstReason, a.offender = rank, cond.Reason, []SyncedKindRef{rec.kindRef()}
	case rank == a.worstRank && rank > 0:
		a.offender = append(a.offender, rec.kindRef())
	}
}

// verdict renders the fold. It reads like a per-kind condition on purpose, so a consumer
// renders a cache exactly as it renders one kind.
func (a *syncHealthAcc) verdict(cacheID ClusterCacheID) ClusterCacheSyncHealth {
	h := ClusterCacheSyncHealth{
		CacheID:        cacheID,
		TotalKinds:     a.total,
		UnhealthyKinds: a.notWatching,
		LastUpdateAt:   a.lastUpdateAt,
		LastLiveAt:     a.lastLiveAt,
	}
	switch {
	case a.total == 0:
		// No kinds yet (discovery hasn't landed) — neither fault nor health.
		h.Status, h.Reason = ConditionUnknown, ReasonSyncing
	case a.worstRank > 0:
		h.Status = ConditionFalse
		h.Reason = a.worstReason
		// Sorted so "the first offender" stays stable while kinds stream in.
		slices.SortFunc(a.offender, compareKindRefs)
		h.UnhealthyKindRefs = a.offender
	case a.unconfirmed > 0:
		// Some kind is still unobserved in this process; Healthy is a claim about
		// EVERY kind, so it can't be made yet. Ranked below a real failure — an
		// observed fault is worth surfacing while others report in.
		h.Status, h.Reason = ConditionUnknown, ReasonSyncing
	case len(a.paused) > 0:
		// Partial pause counts: calling in-between frames Watching would publish a
		// healthy status beside a non-zero unhealthyKinds with no names behind it.
		h.Status, h.Reason = ConditionFalse, ReasonPaused
		slices.SortFunc(a.paused, compareKindRefs)
		h.UnhealthyKindRefs = a.paused
	default:
		h.Status, h.Reason = ConditionTrue, ReasonWatching
	}
	return h
}
