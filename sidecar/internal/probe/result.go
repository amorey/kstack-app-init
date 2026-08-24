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

// What a run concludes and what the engine records of it. Pure data — nothing here reaches the
// queues or the subjects.
package probe

import "time"

// Reason classifies a run in the caller's vocabulary, opaque to the engine but for the two
// constants below. A caller aliases this type and defines its own set.
type Reason string

const (
	// ReasonSucceeded is stamped by Succeeded; a caller never passes it.
	ReasonSucceeded Reason = "Succeeded"
	// ReasonDependencyFailed is recorded by the engine when a probe's dependency is failing,
	// in place of a run. See the dependency lifecycle in docs/specs/probe-engine.md.
	ReasonDependencyFailed Reason = "DependencyFailed"
)

// Verdict is how a finished run was classified, recorded on the Attempt because the schedule is
// derived from it: the engine never interprets a caller's Reason.
type Verdict int

const (
	// VerdictNone is the zero value: no run has finished.
	VerdictNone Verdict = iota
	VerdictSucceeded
	VerdictFailed
	VerdictSuspended
)

// Result is what a run concluded: the record it leaves and the schedule it earns. Built by
// Succeeded, Fail, Suspend, or Skip — the zero Result is invalid, and the engine panics on one.
type Result struct {
	kind    resultKind
	verdict Verdict
	reason  Reason
	message string
	err     error
}

type resultKind int

const (
	resultInvalid resultKind = iota
	resultRecord             // record an attempt carrying verdict/reason
	resultSkip               // record nothing
)

// Succeeded records success; the probe is due again after its interval.
func Succeeded() Result {
	return Result{kind: resultRecord, verdict: VerdictSucceeded, reason: ReasonSucceeded}
}

// Fail records a failure; the probe is due again up the backoff ladder. Message defaults to the
// error's text.
func Fail(reason Reason, err error) Result {
	r := Result{kind: resultRecord, verdict: VerdictFailed, reason: reason, err: err}
	if err != nil {
		r.message = err.Error()
	}
	return r
}

// Suspend records why and schedules nothing; a Wake is what brings the probe back. It ends a
// failure streak — a suspension parks a question rather than failing at one.
func Suspend(reason Reason, message string) Result {
	return Result{kind: resultRecord, verdict: VerdictSuspended, reason: reason, message: message}
}

// Skip records nothing and schedules nothing; a Wake is what brings the probe back. For a run
// that learned nothing usable — an unreadable source, a shutdown cancellation.
func Skip() Result {
	return Result{kind: resultSkip}
}

// Attempt is one run of one probe, from scheduled through finished. Its fields fill in that
// order, so the same value describes a run at every stage of its life.
type Attempt struct {
	// ScheduledAt is when this run should start — the backoff ladder made visible, since
	// successive values show the interval widening. StartedAt is when it did start, FinishedAt
	// when it ended; both zero until they happen, and separate from ScheduledAt because a
	// saturated engine lets a scheduled time slip into the past, which would otherwise read
	// as running.
	ScheduledAt time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	// Verdict, Reason, and Message classify the outcome; Err is the raw error, nil on success.
	// All unset until the run finishes.
	Verdict Verdict
	Reason  Reason
	Message string
	Err     error
}

// Running reports whether this run has started and not finished.
func (a Attempt) Running() bool { return !a.StartedAt.IsZero() && a.FinishedAt.IsZero() }

// Done reports whether this run has finished, which is what makes Verdict readable.
func (a Attempt) Done() bool { return !a.FinishedAt.IsZero() }

// Latency is how long the run took: zero until it finishes, and zero for one recorded without
// being dispatched — a DependencyFailed record has no duration to report.
func (a Attempt) Latency() time.Duration {
	if !a.Done() || a.StartedAt.IsZero() {
		return 0
	}
	return a.FinishedAt.Sub(a.StartedAt)
}

// Attempts is one probe's bookkeeping for one subject. The engine owns every write; a caller
// reads it out of Read or OnChange, and embeds it beside the value the probe observes.
type Attempts struct {
	// LastSeen is when the value the caller keeps beside this was read. Advances only on a
	// succeeded run, stamped by the engine — a commit writes the value and nothing else.
	LastSeen time.Time

	// LastAttempt is the most recent run that finished; NextAttempt is the one that has not,
	// scheduled or already running. A run moves from one to the other as it completes.
	LastAttempt Attempt
	NextAttempt Attempt

	// Failures counts consecutive failed runs and FailingSince is when the streak began, which
	// the count cannot give: the ladder widens, so failures do not map to elapsed time.
	Failures     int
	FailingSince time.Time
}

// OK reports whether the last finished run succeeded. False while nothing has finished.
func (a Attempts) OK() bool { return a.LastAttempt.Verdict == VerdictSucceeded }

// InFlight reports whether a run is under way.
func (a Attempts) InFlight() bool { return a.NextAttempt.Running() }

// Scheduled reports whether another run is due. False for a suspended probe.
func (a Attempts) Scheduled() bool { return !a.NextAttempt.ScheduledAt.IsZero() }

// begin marks a run dispatched. InFlight reads true from here until the commit, which is what
// stops a pass scheduling over a run already out.
func (a *Attempts) begin(at time.Time) { a.NextAttempt.StartedAt = at }

// schedule sets when the next run is due, zero for a probe with nothing scheduled.
func (a *Attempts) schedule(at time.Time) { a.NextAttempt = Attempt{ScheduledAt: at} }

// record files a finished run. A success stamps LastSeen; a suspension ends a failure streak
// the same way a success does, since it parks the question rather than failing at it. It writes
// nothing about the next run — that is derived from this, not decided here.
func (a *Attempts) record(att Attempt) {
	a.LastAttempt = att

	if att.Verdict == VerdictFailed {
		a.Failures++
		if a.FailingSince.IsZero() {
			a.FailingSince = att.FinishedAt
		}
		return
	}
	if att.Verdict == VerdictSucceeded {
		a.LastSeen = att.FinishedAt
	}
	a.Failures, a.FailingSince = 0, time.Time{}
}
