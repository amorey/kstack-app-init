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

// Package kubeconn holds the connections everything in this service talks to a cluster over,
// and the probe that keeps them validated.
//
// **Credentials are the unit, not clusters.** What is pooled is one connection per credential
// key, so two kube-contexts aimed at one server as one user are one socket and one probe.
// Nothing in the vocabulary of a cluster record says so: a reader may not assume one entry per
// cluster.
//
// **A connection is scoped to one identity.** Credentials that move arrive under a new key and
// build a new one; an unchanged key answering as a different server, version, or user retires
// the connection and builds another. So Identity is a field rather than a question a holder has
// to keep re-asking.
//
// **It never learns what a cluster is.** The caller names a kube-context; this package speaks
// contexts, credentials, and whatever a probe read back. Whether the server that answered is
// the one the caller meant is the caller's to decide, off Connection.Identity.
//
// **Everything a holder learns comes through its Lease** — which is why the pool needs no index
// from a credential key back out to the contexts sharing it.
//
// **Nothing dials yet.** Acquire resolves the context and hands back a claim, but no connection
// is built and no probe runs behind it, so every claim reads as nothing known. The pool and the
// probe are what land next.
package kubeconn

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// kubeconfigService resolves one context to credentials and the key naming them, and reports
// when the file behind them changed. The key excludes the context name, so two contexts aimed
// at one server as one user share an entry.
//
// One RESTConfig call is one snapshot. Resolving a context twice can key one snapshot's proxy
// URL onto another's credentials, so a re-key reads each context once and lets the next signal
// correct a straggler.
type kubeconfigService interface {
	RESTConfig(contextName string) (*rest.Config, string, error)
	Subscribe() kubeconfig.Subscription
}

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

// Subscription reports the contexts whose State moved, one event per context. Claims are
// per context, so this is the fleet view of what WatchState reports per claim — same send,
// both channels.
//
// A keyed bus, not a fan-out ring: it holds a slot per context, so a fleet answering at
// once neither loses a context behind a busier one nor bounds what is remembered by a
// buffer length. The value carries nothing — the key is the news, and the reader re-reads
// the claim for what it now says.
type Subscription = *conflate.Receiver[string, struct{}]

// StateSubscription carries a claim's State: the current one on attach, then each one that says
// something new. Close it when done — an abandoned one keeps its slot. Its key is the
// credentials the probe ran against, which a holder of one claim has no use for.
//
// Current-on-attach is what makes it race-free: a watcher that read State separately would owe an
// ordering nothing enforces, and a probe landing between the two calls is the one it never hears
// about.
//
// **Every value is a level, never an edge.** The hub keeps the latest per key, so a reader that
// falls behind skips what came between — a gap means it missed some, not that nothing happened.
// Transitions are derived from the record's conditions and event timeline, never counted here.
type StateSubscription = *watch.Receiver[string, State]

// Lease is a claim on one connection, and what keeps its probe running: credentials nobody
// holds are not probed at all. An interface because it crosses out of this package, where a
// caller's test has to stand in for a live cluster.
type Lease interface {
	// Conn is the connection, once a probe has validated it.
	//
	// Credentials whose last probe succeeded answer immediately. Anything else waits for the
	// probe now running and returns *its* outcome, not a successful one, so a down cluster
	// answers rather than hanging and the caller decides whether to ask again. It does not
	// kick: the claim already has the loop probing, on the backoff ladder while the cluster
	// is down, and kicking per call would flatten it.
	Conn(ctx context.Context) (*Connection, error)
	// State is what the last probe read, check by check. It never dials.
	//
	// What the prober reads: it wants the answers themselves rather than a connection, and a
	// failed check carries a reason no connection could.
	State() State
	// WatchState carries this claim's State, current on attach and again whenever a probe says
	// something new — including a failure whose reason changed. Release ends it.
	//
	// The same hub Conn parks on, so a watcher and a parked caller cannot disagree. Per claim
	// rather than per pool, which is what spares the pool an index from a credential key back
	// out to the contexts sharing it.
	WatchState() StateSubscription
	// Release drops the claim, and with it the probe cadence once nothing else holds these
	// credentials. Idempotent, so it is safe to defer.
	Release()
}

// Connection is one identity and the clients built over the credentials reaching it. The clients
// share one http.Client, so they share one connection pool — with HTTP/2 that is a single TCP
// connection carrying every concurrent request to that API server.
//
// Read-only, and it never changes after it is built. A caller that mutates Config is editing
// what every other holder is using.
type Connection struct {
	// Identity is what the probe that validated this connection read, stable for its life: a
	// probe reading anything different retires it. So a holder that knows which cluster it
	// means compares before it reads. The pool cannot compare — its key is credentials, which
	// do not move when a cluster is rebuilt, upgraded, or re-issues a token for another user.
	Identity Identity
	// Config is what the clients below were built from. Exposed for the client-go
	// constructors that take one and build their own transport (exec, port-forward);
	// anything that can use Dynamic or HTTPClient should.
	Config *rest.Config
	// BaseURL is Config.Host resolved to an absolute URL, carrying the scheme and any path
	// prefix; APIPath is the versioned path beside it. Raw paths join onto them.
	BaseURL *url.URL
	APIPath string
	// HTTPClient is authenticated and pooled. Raw API paths go through it directly, and
	// unthrottled: the QPS bucket lives in rest.RESTClient, so it reaches Dynamic alone.
	HTTPClient *http.Client
	Dynamic    dynamic.Interface

	// done closes when this connection is retired. Nil in a connection nobody built, which
	// reads as never retired.
	done chan struct{}
}

// Done closes when a probe reads a different Identity and this connection is retired.
//
// For the holder nothing else reaches: a long-lived stream is blocked in a read, so a field it
// is not re-reading cannot tell it that what it derived here no longer holds. A caller that asks
// per operation re-calls Lease.Conn instead.
func (c *Connection) Done() <-chan struct{} { return c.done }

// Service is the pool the cluster service dials through.
type Service struct {
	kubecfgSvc kubeconfigService
	hub        *conflate.Hub[string, struct{}]

	wg sync.WaitGroup
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfgSvc kubeconfigService) *Service {
	return &Service{
		kubecfgSvc: kubecfgSvc,
		hub:        conflate.New[string, struct{}](),
	}
}

// Subscribe reports every context whose State moves, for a reader whose reaction to any of
// them is the same. A holder that cares about one claim watches that claim instead.
//
// Nothing probes yet, so nothing is ever sent.
func (s *Service) Subscribe() Subscription { return s.hub.Receiver() }

// Acquire claims the connection for contextName's credentials and arms their probe cadence,
// building the connection if this is the first caller. It does not dial — Lease.Conn is what
// waits — so a caller may hold a claim across a cluster being down without a retry loop of
// its own. Release the claim.
//
// Resolving here is what makes a claim answerable: a context the kubeconfig does not name, or
// whose entries do not resolve, is refused with the reader's own error rather than becoming a
// claim on credentials nobody could build.
//
// A claim is on the context, not on the key it resolved to. Credentials rotate under a context
// that never moves, so a claim pinned to one key would report the server it used to reach for
// as long as it is held — re-resolving is this package's, since it is the one that resolves.
//
// A kubeconfig that has not been read yet is not a refusal: the pre-read config is empty, so
// every context looks departed, and refusing would report a live cluster's credentials broken.
// The claim is taken and resolves when the file lands.
func (s *Service) Acquire(contextName string) (Lease, error) {
	if _, _, err := s.kubecfgSvc.RESTConfig(contextName); err != nil && !errors.Is(err, kubeconfig.ErrNotRead) {
		return nil, err
	}
	return pendingLease{}, nil
}

// rekey points every claim at the entry for the credentials its context resolves to now, and
// is what makes a claim outlive the credentials it was taken against. A claim whose key moved
// leaves its old entry; an entry nothing claims stops probing, and the connection it built
// retires so a stream on it learns rather than reading from a server the caller no longer means.
//
// The signal and not the probe cycle, because a claim on a down cluster is deep in the backoff
// ladder: a user who just fixed their kubeconfig would otherwise wait it out, which is the one
// moment they want the retry now. Landing on a different entry starts a fresh ladder.
//
// The config is the signal, never the source: resolving goes back through RESTConfig, which
// is what computes a key at all.
//
// Nothing is keyed yet, so there is nothing to move.
func (s *Service) rekey() {}

// pendingLease is a claim on credentials nothing probes yet: it reports nothing known, which is
// what a claim whose first probe is still owed reports once there is one.
type pendingLease struct{}

func (pendingLease) Conn(context.Context) (*Connection, error) { panic("nothing dials yet") }

func (pendingLease) State() State { return State{} }

// WatchState has nothing to hand back: nothing probes, so this claim never publishes.
func (pendingLease) WatchState() StateSubscription { return nil }

func (pendingLease) Release() {}

// Start watches the kubeconfig until stopped, re-keying claims as the file moves under them.
// The subscription is established before Start returns, so a config already read is applied
// once rather than waited for.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	// Not Start's context, which bounds startup: this one bounds the loop, so it lives until
	// the stop func cancels it.
	loopCtx, stopLoop := context.WithCancel(context.Background())

	sub := s.kubecfgSvc.Subscribe()
	s.wg.Go(func() {
		defer sub.Close()
		s.watchKubeconfig(loopCtx, sub)
	})

	return func(ctx context.Context) error {
		stopLoop()
		return drain.WithContext(ctx, s.wg.Wait)
	}, nil
}

// watchKubeconfig re-keys on every config until stopped. Cancellation ends the wait: the
// service behind the feed is the app's and is not required to close its channel when released.
func (s *Service) watchKubeconfig(ctx context.Context, sub kubeconfig.Subscription) {
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-sub.Chan():
			if !ok {
				return
			}
			s.rekey()
		}
	}
}

// Close releases the pool. Nothing is held yet.
func (s *Service) Close() error { return nil }
