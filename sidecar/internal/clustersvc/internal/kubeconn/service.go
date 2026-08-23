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
// **One context, one connection, one probe.** Contexts that resolve alike are not merged: two
// aimed at one server as one user get a socket each, which costs a handshake and a probe cycle
// and buys a store that is one map, attribution that needs no apportioning, and a failure that
// belongs to one caller. The credential helper is not duplicated with them — client-go caches
// exec authenticators process-wide by exec config, below anything here.
//
// **A connection is scoped to one identity.** Credentials that move build a new connection; an
// unchanged fingerprint answering as a different server, version, or user retires the
// connection and builds another. So Identity is a field rather than a question a holder has to
// keep re-asking.
//
// **It never learns what a cluster is.** The caller names a kube-context; this package speaks
// contexts, connections, and whatever a probe read back. One address can front two clusters and
// nothing here can tell them apart — whether the server that answered is the one the caller
// meant is the caller's to decide, off Connection.Identity.
//
// **Everything a holder learns comes through its Lease**, which is the context it named. Both
// hubs publish under that same context.
//
// **Nothing dials yet.** Acquire hands back a claim, but no connection is built and no probe
// runs behind it, so every claim reads as nothing known. The connections and the probe are what
// land next.
package kubeconn

import (
	"context"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amorey/gobus/conflate"
	"github.com/amorey/gobus/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// kubeconfigService resolves one context to a connection and the fingerprint naming it, and
// reports when the file behind it changed. The fingerprint covers everything that decides what
// a socket is — server, TLS, auth, proxy — so an unchanged one means the connection this
// context holds is still the one its credentials name.
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

// StateSubscription carries each State a probe publishes for one claim. Close it when done —
// an abandoned one keeps its slot.
//
// **Nothing is delivered on attach.** A watcher reads Lease.State for what is known now and
// this for what comes after; the bus hands back no current value, so a watcher that only
// attaches learns nothing until the next probe lands.
//
// Keyed by context, not by the credentials behind it. A receiver is bound to its key for life,
// and credentials move under a context that never does — so keying on the connection would
// strand a watcher on the one its claim just left.
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
	// WatchState carries every State a probe publishes for this claim — including a failure
	// whose reason changed. It delivers nothing on attach, so a watcher pairs it with State
	// for what is known now. Release ends it.
	//
	// The same hub Conn parks on, so a watcher and a parked caller cannot disagree, keyed by
	// the context this claim named.
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
	// means compares before it reads. Nothing here can compare — a context resolving to the
	// same credentials says nothing about which cluster is behind them.
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
	// signalHub names the contexts whose state moved and carries nothing else; stateHub
	// carries what they now say. Both keyed by context, both fed by one send.
	signalHub *conflate.Hub[string, struct{}]
	stateHub  *watch.Hub[string, State]

	// mu guards the map, and an entry's fields with it: a rotation replaces the fingerprint
	// and the state together, and nothing may read one against the other.
	mu sync.Mutex
	// claimed is one entry per claimed context — a key is here exactly while someone holds
	// that context — and is also the key both hubs publish under.
	claimed map[string]*entry

	wg sync.WaitGroup
}

// entry is what the pool holds for one claimed context: how many hold it, and what the probe
// last read. holders and the state are one record because a context has exactly one of each,
// which is what keeps the pool a single map.
type entry struct {
	holders int
	state   State
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfgSvc kubeconfigService) *Service {
	return &Service{
		kubecfgSvc: kubecfgSvc,
		signalHub:  conflate.New[string, struct{}](),
		stateHub:   watch.New[string, State](),
		claimed:    map[string]*entry{},
	}
}

// Subscribe reports every context whose State moves, for a reader whose reaction to any of
// them is the same. A holder that cares about one claim watches that claim instead.
//
// Nothing probes yet, so the only thing that moves is the kubeconfig: a context whose
// credentials rotated, or that stopped resolving.
func (s *Service) Subscribe() Subscription { return s.signalHub.Receiver() }

// Acquire claims contextName's connection and arms its probe cadence. It does not dial, and it
// does not resolve — Lease.Conn is what waits, so a caller may hold a claim across a cluster
// being down, a kubeconfig that has not been read, or a context that does not exist yet.
// Release the claim.
//
// **A claim is answerable before it is answerable-with-anything.** Whether the context resolves,
// and to what, is the pool's to find out on its own schedule and report through Lease.State —
// never a reason to refuse the claim, since every one of those answers can change without the
// holder doing anything.
//
// A claim is on the context, not on the credentials it resolved to. Credentials rotate under a
// context that never moves, so a claim pinned to them would report the server it used to reach
// for as long as it is held.
func (s *Service) Acquire(contextName string) Lease {
	s.claimContext(contextName)
	return &claim{svc: s, contextName: contextName}
}

// claimContext counts a holder on contextName, building its entry if this is the first. The
// only thing that creates one.
func (s *Service) claimContext(contextName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.claimed[contextName]
	if !ok {
		e = &entry{}
		s.claimed[contextName] = e
	}
	e.holders++
}

// stateOf is what the claim on contextName reads. A context with no connection has nothing read
// about it, which is what a claim awaiting its first resolve reports.
func (s *Service) stateOf(contextName string) State {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e := s.claimed[contextName]; e != nil {
		return e.state
	}
	return State{}
}

// releaseContext gives back one holder's claim on contextName, dropping the context once the
// last one goes. An entry nobody holds is one nothing probes.
//
// The decrement and the drop are one critical section: a claim taken between them would be on
// an entry this call then deletes, and would read as nothing known for as long as it is held.
func (s *Service) releaseContext(contextName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	e := s.claimed[contextName]
	if e == nil {
		return
	}
	e.holders--
	if e.holders == 0 {
		delete(s.claimed, contextName)
	}
}

// claim is one holder's claim on the credentials a context names. It holds the context and
// never the entry: re-keying moves the entry underneath, and a claim pinned to one would
// report the server it used to reach for as long as it is held.
type claim struct {
	svc         *Service
	contextName string

	released atomic.Bool
}

func (c *claim) Conn(context.Context) (*Connection, error) { panic("nothing dials yet") }

func (c *claim) State() State { return c.svc.stateOf(c.contextName) }

// WatchState takes no baseline: the hub compares against one only through an Accept, and this
// one has none, so every value is delivered. What a baseline is for — reading and registering
// in one critical section, so a probe landing between the two is not skipped — is worth having
// once a probe can land at all.
func (c *claim) WatchState() StateSubscription {
	return c.svc.stateHub.Watch(c.contextName)
}

func (c *claim) Release() {
	if c.released.CompareAndSwap(false, true) {
		c.svc.releaseContext(c.contextName)
	}
}

// rekey points every claim at the entry for the credentials its context resolves to now, and
// is what makes a claim outlive the credentials it was taken against. A claim whose key moved
// leaves its old entry; an entry nothing claims stops probing, and the connection it built
// retires so a stream on it learns rather than reading from a server the caller no longer means.
//
// The signal and not the probe cycle, because a claim on a down cluster is deep in the backoff
// ladder: a user who just fixed their kubeconfig would otherwise wait it out, which is the one
// moment they want the retry now. A rotation builds a different connection, so its ladder
// starts fresh.
//
// The config is the signal, never the source: resolving goes back through RESTConfig, which
// is what computes a fingerprint at all.
func (s *Service) rekey() {
	// Nothing to record yet. What a context resolves to now is the probe's to read and
	// report; a claim is held whether or not the file still names it, and dropping one here
	// would orphan a holder that is still holding.
}

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

// Close drops the pool. Claims are not released for their holders: a claim outliving the pool
// is the holder's bug, and reporting nothing known is what a dropped entry already does.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, e := range s.claimed {
		e.state = State{}
	}
	return nil
}
