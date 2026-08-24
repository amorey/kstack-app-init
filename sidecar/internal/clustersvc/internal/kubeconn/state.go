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

// What a probe read about the server behind one context: the checks, their outcomes, and the
// identity they add up to. Pure data — nothing here reaches the pool or the kubeconfig.
package kubeconn

import "time"

// Reason classifies the most recent attempt at one check. Ours, in the style of a Kubernetes
// condition reason: CamelCase, stable, safe to switch on. Free-form detail belongs in Message.
//
// **It spans layers on purpose.** A check dies at the transport, in an API response, or on a
// rule of ours, and a caller asks why once — so a TLS failure and a 403 and a skipped check are
// one vocabulary. Some names match metav1.StatusReason because that is the same word for the
// same thing, not because this is that set: it has no word for a certificate we rejected.
//
// Assigned when the attempt completes, because the classification is not recoverable later:
// Err arrives wrapped, and does not survive the trip to a caller that only sees a copy.
//
// **Classify from the typed error where there is one.** Dynamic returns *apierrors.StatusError,
// which carries the API's own reason; only the raw endpoints (/readyz, /version) leave a status
// code as the sole evidence. One switch over status codes for both would discard what the typed
// half already knows.
type Reason string

const (
	// ReasonUnknown is the zero value: no attempt has completed.
	ReasonUnknown Reason = ""
	// ReasonSucceeded means the attempt returned usable data.
	ReasonSucceeded Reason = "Succeeded"

	// ReasonUnreachable is DNS failure, connection refused, or no route — nothing answered.
	ReasonUnreachable Reason = "Unreachable"
	// ReasonTLSInvalid means the server answered and its certificate was rejected.
	ReasonTLSInvalid Reason = "TLSInvalid"
	// ReasonTimeout means the check's own deadline expired. Told apart from ReasonCanceled
	// because the deadline was ours: the caller is still waiting and this is news about the
	// cluster.
	ReasonTimeout Reason = "Timeout"
	// ReasonCanceled means the caller went away mid-flight. It says nothing about the
	// cluster, so it does not count toward Failures.
	ReasonCanceled Reason = "Canceled"

	// ReasonUnauthorized is a 401: credentials absent, malformed, or expired.
	ReasonUnauthorized Reason = "Unauthorized"
	// ReasonForbidden is a 403: authenticated, and not permitted. A grant to fix rather than
	// an outage to wait out, which is why callers render it differently.
	ReasonForbidden Reason = "Forbidden"

	// ReasonNotFound means the object a check asked for is absent, such as kube-system.
	// News about the cluster, and a check that keeps running.
	ReasonNotFound Reason = "NotFound"
	// ReasonUnsupported means the endpoint itself is absent — SelfSubjectReview before v1.27,
	// or a managed distribution withholding /readyz. Terminal: the check suspends.
	//
	// The trap: both arrive as a 404, and only the check knows which it asked for. Classifying
	// on the status code alone suspends a check that should have kept running.
	ReasonUnsupported Reason = "Unsupported"
	// ReasonInternalError is a 500: the API server hit a fault serving the request.
	ReasonInternalError Reason = "InternalError"
	// ReasonServiceUnavailable is a 503, the routine one — a control plane restarting or
	// mid-upgrade. Told apart from ReasonInternalError because it is expected and passes on
	// its own, which is what a caller renders differently and what a backoff waits out.
	ReasonServiceUnavailable Reason = "ServiceUnavailable"
	// ReasonThrottled is a 429, or a client-side rate limiter delay past the check timeout.
	ReasonThrottled Reason = "Throttled"
	// ReasonMalformed means a response arrived and would not parse, which usually means a
	// proxy or captive portal between us and the API server.
	ReasonMalformed Reason = "Malformed"

	// ReasonComponentsFailing means /readyz answered and named components not ok. The check
	// worked and the cluster is not fit to use; ComponentStatus.Failing says which.
	ReasonComponentsFailing Reason = "ComponentsFailing"
	// ReasonDependencyFailed means the check was recorded rather than attempted, because the
	// connection it needs had already failed this cycle. It marks the cycle a check went from
	// running to suspended; the cycles after it schedule nothing at all, which is what keeps a
	// dead cluster costing one timeout per cycle instead of one per check.
	ReasonDependencyFailed Reason = "DependencyFailed"
	// ReasonInternal is a bug here.
	ReasonInternal Reason = "Internal"
)

// Attempt is one run of one check, from scheduled through finished. Its fields fill in that
// order, so the same value describes a run at every stage of its life and Observation needs no
// second type for the one that has not finished.
//
// Immutable once FinishedAt is set, which is what makes Latency safe to derive.
type Attempt struct {
	// ScheduledAt is when this run should start. It is the backoff ladder made visible: a
	// caller renders the wait, and successive values show the interval widening.
	ScheduledAt time.Time
	// StartedAt is when it did start, FinishedAt when it ended. Both zero until they happen,
	// and told apart from ScheduledAt on purpose — a saturated prober lets a scheduled time
	// slip into the past, which would otherwise read as running.
	StartedAt  time.Time
	FinishedAt time.Time
	// Reason and Message classify the outcome, success included; Err is the raw error, nil on
	// success. Reason and Message are the durable record — Err arrives wrapped and is for a
	// caller close enough to unwrap it. All three are unset until the run finishes.
	Reason  Reason
	Message string
	Err     error
}

// Running reports whether this run has started and not finished.
func (a Attempt) Running() bool { return !a.StartedAt.IsZero() && a.FinishedAt.IsZero() }

// Done reports whether this run has finished, which is what makes Reason readable.
func (a Attempt) Done() bool { return !a.FinishedAt.IsZero() }

// Latency is how long the run took. Zero until it finishes, and zero for one that was
// recorded without being dispatched — a DependencyFailed run has no duration to report.
func (a Attempt) Latency() time.Duration {
	if !a.Done() || a.StartedAt.IsZero() {
		return 0
	}
	return a.FinishedAt.Sub(a.StartedAt)
}

// Observation is one check's last answer and the provenance to judge it.
//
// **Value outlives the failure that follows it.** A read that stops being permitted does not
// mean the fact changed, so Value is what was last seen and LastSeen is when — which is what
// makes it readable: "identified, as of 10:00" is usable where "ready, as of 10:00" is not.
//
// **A check that has never run is the zero value**, which needs no sentinel: a zero LastAttempt
// is not Done, so every question below already answers for it.
//
// **A zero NextAttempt means the check is suspended** — nothing is due, and the last answer
// stands. The four checks behind the connection are suspended while it is down, since a server
// nothing reached cannot answer them, and re-armed when it recovers; a check whose last attempt
// was ReasonUnsupported stays suspended for the life of the connection, because the endpoint is
// absent rather than failing. So "ready, as of 10:00, nothing due" is a state a reader renders,
// not one it treats as stalled.
//
// **Why it is suspended is LastAttempt.Reason**, which needs no field of its own because a check
// suspends over what its last attempt found. Suspending therefore has to write one — the cycle
// that stops scheduling records DependencyFailed rather than going quiet, or the reason is lost.
type Observation[T any] struct {
	// Value is the last answer. Meaningless until Known.
	Value T
	// LastSeen is when Value was read. Advances only on success.
	LastSeen time.Time

	// LastAttempt is the most recent run that finished; NextAttempt is the one that has not,
	// scheduled or already running. A run moves from one to the other as it completes.
	LastAttempt Attempt
	NextAttempt Attempt

	// Failures counts consecutive failures and FailingSince is when the run of them began,
	// which the count cannot give: the ladder widens, so failures do not map to elapsed time.
	// ReasonCanceled says nothing about the cluster and counts toward neither.
	Failures     int
	FailingSince time.Time
}

// Known reports whether this check has ever answered, which is what makes Value readable.
func (o Observation[T]) Known() bool { return !o.LastSeen.IsZero() }

// OK reports whether the last finished attempt answered. False while nothing has finished.
func (o Observation[T]) OK() bool { return o.LastAttempt.Reason == ReasonSucceeded }

// InFlight reports whether a run is under way.
func (o Observation[T]) InFlight() bool { return o.NextAttempt.Running() }

// Scheduled reports whether another run is due. False for a suspended check.
func (o Observation[T]) Scheduled() bool { return !o.NextAttempt.ScheduledAt.IsZero() }

// ComponentStatus is what /readyz named when it answered. Empty on a server that is ready.
type ComponentStatus struct {
	Failing []string // components not reporting ok
}

// VersionInfo is the API server's reported version.
type VersionInfo struct {
	GitVersion string // "v1.31.4"
	Major      string
	Minor      string // carries a "+" suffix on some managed distributions
}

// Principal is who these credentials authenticate as, per SelfSubjectReview.
type Principal struct {
	Username string
	Groups   []string
}

// Identity is the three scalars a connection is scoped to. Comparable on purpose: retiring a
// connection is a == against what the last probe read, and errors are what would break it — so
// why a part is missing stays on the Observation that could not read it.
type Identity struct {
	ServerUID     string
	ServerVersion string
	Username      string
}

// State is what the last probe read about the server behind one entry: five checks that
// succeed, fail, and go stale independently. Reachability is the only one the others depend on.
//
// It says nothing about the connection's own life — whether one is built, retiring, or how many
// claims hold it. Connection.Done is where that surfaces.
//
// A value copy, but a shallow one — the slices inside are the prober's and must not be
// mutated, since every watcher holds the same backing array.
type State struct {
	Connection    Observation[string] // the API server URL that answered
	Readiness     Observation[ComponentStatus]
	ServerUID     Observation[string] // kube-system's UID, conventionally the cluster's
	ServerVersion Observation[VersionInfo]
	Principal     Observation[Principal]
}

// Phase is how far the last attempt to reach the server got. The other four checks report
// themselves; this one is separate because they are only worth reading once it is PhaseProbed.
type Phase int

const (
	// PhasePending means nothing has completed yet.
	PhasePending Phase = iota
	// PhaseUnreached means the last attempt did not reach the server.
	PhaseUnreached
	// PhaseProbed means it did.
	PhaseProbed
)

// Phase reports how far the last attempt got.
func (s State) Phase() Phase {
	switch {
	case !s.Connection.LastAttempt.Done():
		return PhasePending
	case s.Connection.OK():
		return PhaseProbed
	default:
		return PhaseUnreached
	}
}

// Identity is the scope built from the checks that have answered. A part no probe could read
// is empty, which is how it compares equal to another connection missing the same part.
func (s State) Identity() Identity {
	return Identity{
		ServerUID:     s.ServerUID.Value,
		ServerVersion: s.ServerVersion.Value.GitVersion,
		Username:      s.Principal.Value.Username,
	}
}
