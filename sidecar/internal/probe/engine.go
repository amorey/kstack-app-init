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

// Package probe runs a set of periodic observations over a set of subjects: the controller
// pattern without Kubernetes — a work queue, a level-triggered pass, and a schedule derived from
// recorded state. Nothing here knows what a subject is beyond its name.
//
// A caller registers its probes, says which subjects are tracked, and reads what the runs found:
// Register takes a Probe with its cadence and what it depends on, all addressed by registration
// name — a probe's whole public identity; Add and Remove track subjects, and Wake says a
// recorded answer went stale; Read and OnPass hand back a Snapshot — every probe's
// Observation, value beside attempts, copied under one lock. Get reads one of them by name,
// which is how a Run reads a sibling, and a Key pairs that name with its type once.
//
// A run's own Result is its schedule — Succeeded waits out the interval, Fail climbs the backoff
// ladder, Suspend and Skip wait for a Wake — so no domain rule lives in the scheduler.
//
// See docs/adr/2026-08-24-probe-engine.md.
package probe

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/drain"
	"github.com/kubetail-org/kstack-app/sidecar/internal/workqueue"
)

// probeID is a probe's registration index — engine-internal, since a probe's public identity is
// the name it was registered under.
type probeID int

// Probe is one probe's body: request against the subject, classify, and return the result. What
// the run found is recorded on the pass, wherever the body learns it; a body with no news just
// returns. The engine applies the pass under its lock, so a run must do its waiting before it
// returns.
//
// A probe whose value owns something — a connection, a file — also implements
// `Discard(T)`, and is handed back any value the engine does not apply: a commit refused because
// the subject was removed mid-run, a run that concluded Skip or returned the zero Result, and one
// that panicked. Nothing else can reach such a value to release it.
type Probe[T any] interface {
	Run(ctx context.Context, pass *Pass[T]) Result
}

// Backoff paces a failing probe: Base widened by Factor per consecutive failure, capped at Cap.
type Backoff struct {
	Base   time.Duration
	Factor float64
	Cap    time.Duration
}

// delay is the ladder — a pure function of failures, so the same state derives the same
// ScheduledAt on every pass. That is what lets the frontend render the countdown: no jitter,
// and no stateful rate limiter.
func (b Backoff) delay(failures int) time.Duration {
	d := b.Base
	for range failures - 1 {
		d = time.Duration(float64(d) * b.Factor)
		if d >= b.Cap {
			return b.Cap
		}
	}
	return min(d, b.Cap)
}

// probeCfg is the per-probe knobs, all optional.
type probeCfg struct {
	interval     time.Duration
	backoff      Backoff
	timeout      time.Duration
	dependencies []string
	watches      []string
}

var defaultCfg = probeCfg{
	interval: time.Minute,
	backoff:  Backoff{Base: time.Second, Factor: 2, Cap: 5 * time.Minute},
	timeout:  30 * time.Second,
}

// ProbeOption tunes one registration; a registration states only what deviates from the
// defaults.
type ProbeOption func(*probeCfg)

// WithDependencies declares the probes this one can only run over the success of — the health
// edge, answering "can this run?". While a dependency is failing the probe is recorded as
// DependencyFailed rather than dispatched, and it is due again when the dependency recovers. It
// takes IDs Register already returned, so a dependency exists before whatever depends on it and
// the graph is acyclic by construction.
func WithDependencies(names ...string) ProbeOption {
	return func(c *probeCfg) { c.dependencies = append(c.dependencies, names...) }
}

// WithWatches declares the probes whose values this one reads — the data edge, answering
// "is this answer stale?". When one of them commits a changed value, this probe runs again,
// whatever its schedule said; it takes no other part in scheduling, and it never gates a run.
// Health is the other edge's job: a probe that also cannot run without what it reads declares
// WithDependencies too.
func WithWatches(names ...string) ProbeOption {
	return func(c *probeCfg) { c.watches = append(c.watches, names...) }
}

// WithInterval is how long after a succeeded run the probe is due again.
func WithInterval(d time.Duration) ProbeOption {
	return func(c *probeCfg) { c.interval = d }
}

// WithBackoff paces the probe's failures.
func WithBackoff(base time.Duration, factor float64, cap time.Duration) ProbeOption {
	return func(c *probeCfg) { c.backoff = Backoff{Base: base, Factor: factor, Cap: cap} }
}

// WithTimeout bounds one run, enforced by the engine on the context it hands the run.
func WithTimeout(d time.Duration) ProbeOption {
	return func(c *probeCfg) { c.timeout = d }
}

// spec is one registered probe, its body erased to the value the observable stores. Register's
// wrapper is the only thing that boxes and unboxes, so the types stay checked at the boundary.
type spec struct {
	name string
	cfg  probeCfg
	// dependencies and watches are cfg's names resolved once, at registration.
	dependencies []probeID
	watches      []probeID
	run          func(ctx context.Context, subjectName string, prev any, snap Snapshot) (Result, any, func())
}

// Option configures an Engine.
type Option func(*settings)

// WithWorkers is how many runs may be in flight at once, across every subject. A fleet-wide cap
// rather than a per-subject one, because what it holds back is the first pass over a large
// kubeconfig: without it every cluster's credential helper runs in the same second.
func WithWorkers(n int) Option { return func(s *settings) { s.workers = n } }

// settings is what the options write.
type settings struct {
	workers int
}

// Engine runs the registered probes over the tracked subjects. Build with New, add probes with
// Register, then Start; the zero Engine has no queues to work.
type Engine struct {
	settings settings
	onPass   func(subjectName string, snap Snapshot)

	// runQ carries the runs that are due, one key per probe per subject, so one probe never
	// runs twice at once and an ask arriving mid-run is redelivered on Done rather than folded
	// into a run that could not have seen it. passQ carries the subjects whose schedule has to
	// be re-derived; everything that changes a subject ends with an add there.
	runQ  *workqueue.Queue[key]
	passQ *workqueue.Queue[string]

	// mu guards specs, started, and subjects together with the entries behind them: a value
	// and its attempts are read against each other, and nothing may see one without the other.
	mu      sync.Mutex
	specs   []spec
	started bool
	// watchers is the data edge reversed: for each probe, who watches its value and so runs
	// again when it moves. byName is what Get resolves a read through. Both built by Register,
	// and neither written once a subject exists.
	watchers map[probeID][]probeID
	byName   map[string]probeID
	subjects map[string]*subject

	wg sync.WaitGroup
}

// key is one run of one probe against one subject — the unit runQ keys on.
type key struct {
	subject string
	probe   probeID
}

// observable is what the engine holds for one probe of one subject: the value its runs committed
// beside the bookkeeping for the probe that read it. The untyped half of Observation[T], which is
// what Get projects it into — one engine carries probes of different value types, so the slice
// itself cannot be typed.
type observable struct {
	// value is the last committed answer, nil until a run commits one, and seen is when a run
	// last confirmed it: whenever one is committed, and on a success that found the standing
	// answer unchanged — which still read it.
	value any
	seen  time.Time
	// skipped marks a last run that returned Skip — the one memory a Skip leaves. It records
	// nothing, so without this the pass would read the probe as never-run and re-dispatch it
	// at once. Cleared when a run records; a Wake goes through runQ, so it needs no clearing
	// to force a run.
	skipped bool

	Attempts
}

// subject is what the engine holds for one tracked name. Identity matters: a subject removed
// and re-added is a new *subject, and a run dispatched against the old one commits nothing.
type subject struct {
	// obs is one observable per probe, indexed by probeID.
	obs []observable
	// timer brings the pass back when the soonest scheduled run comes due. One per subject,
	// and it is a wake, not a cadence: the pass decides again per probe.
	timer *time.Timer
}

func (s *subject) stopTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

// snapshotOf copies one subject's observables for a reader. Called under e.mu, so the values and
// the schedule beside them agree. The registration index is shared rather than copied —
// registration closes before the first subject, so nothing writes it from here on.
func (e *Engine) snapshotOf(sub *subject) Snapshot {
	return Snapshot{obs: slices.Clone(sub.obs), byName: e.byName}
}

// New returns an Engine with no probes and no subjects.
func New(opts ...Option) *Engine {
	e := &Engine{
		settings: settings{workers: 8},
		runQ:     workqueue.New[key](),
		passQ:    workqueue.New[string](),
		watchers: map[probeID][]probeID{},
		byName:   map[string]probeID{},
		subjects: map[string]*subject{},
	}
	for _, opt := range opts {
		opt(&e.settings)
	}
	return e
}

// OnPass sets the callback the engine fires after every pass, with the Snapshot that pass
// produced. Called outside the engine's lock but serialized per subject; it must not block.
// Wiring, not state — set it before Start, like Register.
//
// Every pass, not every change: a snapshot carries Attempts, which every run rewrites, so the
// engine cannot tell a new answer from the same one re-confirmed. A caller that wakes something
// expensive projects the snapshot down to what its readers react to and compares that itself.
func (e *Engine) OnPass(fn func(subjectName string, snap Snapshot)) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		panic("probe: OnPass after Start")
	}
	e.onPass = fn
}

// Register adds one probe under name, which is its whole public identity: the edges, Wake, and
// every read address it by that. A package function rather than a method so each probe picks its
// own value type; T is inferred from the instance.
//
// It panics on an edge naming a probe not yet registered, a duplicate name, or a call after
// Start or the first Add — a table wired wrong at boot, not a runtime error. A subject's
// bookkeeping is sized when it is added, which is why the set must be complete before anything
// is tracked.
func Register[T any](e *Engine, name string, p Probe[T], opts ...ProbeOption) {
	if p == nil {
		panic("probe: Register needs a probe")
	}
	run := func(ctx context.Context, subjectName string, prev any, snap Snapshot) (Result, any, func()) {
		var pv T
		if prev != nil {
			pv = prev.(T)
		}
		pass := &Pass[T]{subject: subjectName, prev: pv, known: prev != nil, snap: snap}
		// A run that panics has still committed whatever it committed before it did, and the
		// engine applies none of it — so it is handed back here, on the way out to the
		// recover that turns the panic into a result.
		defer func() {
			if r := recover(); r != nil {
				handBack(p, pass)
				panic(r)
			}
		}()
		res := p.Run(ctx, pass)
		if pass.next == nil {
			return res, nil, nil
		}
		return res, *pass.next, func() { handBack(p, pass) }
	}
	e.register(name, run, opts)
}

// handBack tells a probe that the engine dropped what its run committed, for one that asked to
// be told by implementing Discard. A committed value can own something — a connection, a file —
// and one the engine never applied is one nothing else can reach to release.
func handBack[T any](p Probe[T], pass *Pass[T]) {
	d, ok := p.(interface{ Discard(T) })
	if !ok || pass.next == nil {
		return
	}
	d.Discard(*pass.next)
}

func (e *Engine) register(name string, run func(context.Context, string, any, Snapshot) (Result, any, func()), opts []ProbeOption) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		panic("probe: Register after Start")
	}
	if len(e.subjects) != 0 {
		panic("probe: Register after Add")
	}
	if name == "" {
		panic("probe: Register needs a name")
	}
	for _, s := range e.specs {
		if s.name == name {
			panic(fmt.Sprintf("probe: %q registered twice", name))
		}
	}

	cfg := defaultCfg
	for _, opt := range opts {
		opt(&cfg)
	}
	// Resolved here, against what is registered so far: a forward reference panics, so the
	// registration order stays topological and both graphs are acyclic by construction.
	dependencies := e.resolveLocked(name, "depends on", cfg.dependencies)
	watches := e.resolveLocked(name, "watches", cfg.watches)

	e.specs = append(e.specs, spec{
		name: name, cfg: cfg, dependencies: dependencies, watches: watches, run: run,
	})
	id := probeID(len(e.specs) - 1)
	e.byName[name] = id
	// The edge is walked from the probe that committed, so it is indexed that way here rather
	// than searched for at wake time. Both ends exist already, which is what makes it safe.
	for _, watched := range watches {
		e.watchers[watched] = append(e.watchers[watched], id)
	}
}

// resolveLocked turns an edge's names into indexes. A name nothing was registered under is a
// wiring bug: edge names me a probe that does not exist, or one that comes later.
func (e *Engine) resolveLocked(name, edge string, names []string) []probeID {
	if len(names) == 0 {
		return nil
	}
	ids := make([]probeID, 0, len(names))
	for _, want := range names {
		id, ok := e.byName[want]
		if !ok {
			panic(fmt.Sprintf("probe: %q %s %q, which is not registered yet", name, edge, want))
		}
		ids = append(ids, id)
	}
	return ids
}

// Add tracks subject. Every probe derives from zero, so the pass this queues dispatches
// whatever a fresh subject owes; adding a subject already tracked changes nothing.
func (e *Engine) Add(subjectName string) {
	e.mu.Lock()
	if _, ok := e.subjects[subjectName]; ok {
		e.mu.Unlock()
		return
	}
	e.subjects[subjectName] = &subject{obs: make([]observable, len(e.specs))}
	e.mu.Unlock()

	e.passQ.Add(subjectName)
}

// Remove stops tracking subject: its timer stops, and a run still in flight against it commits
// nothing. Removing a subject not tracked changes nothing.
func (e *Engine) Remove(subjectName string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	sub := e.subjects[subjectName]
	if sub == nil {
		return
	}
	sub.stopTimer()
	delete(e.subjects, subjectName)
}

// Wake says these probes' answers are stale: run them again, suspension notwithstanding. It
// adds straight to the run queue, whose held/dirty machinery redelivers a key that was mid-run
// when its commit lands — a Wake is never lost. A subject not tracked is ignored; a name nothing
// was registered under is a wiring bug and panics.
func (e *Engine) Wake(subjectName string, names ...string) {
	e.mu.Lock()
	tracked := e.subjects[subjectName] != nil
	ids := make([]probeID, 0, len(names))
	for _, name := range names {
		id, ok := e.byName[name]
		if !ok {
			e.mu.Unlock()
			panic(fmt.Sprintf("probe: Wake of %q, which is not registered", name))
		}
		ids = append(ids, id)
	}
	e.mu.Unlock()

	if !tracked {
		return
	}
	for _, id := range ids {
		e.runQ.Add(key{subject: subjectName, probe: id})
	}
}

// WakeAll is Wake over every tracked subject.
func (e *Engine) WakeAll(names ...string) {
	e.mu.Lock()
	subjects := slices.Collect(maps.Keys(e.subjects))
	e.mu.Unlock()

	for _, subjectName := range subjects {
		e.Wake(subjectName, names...)
	}
}

// Read is subject's observables, copied under one lock so the values and the schedule beside
// them agree. ok is false for a subject not tracked.
func (e *Engine) Read(subjectName string) (snap Snapshot, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	sub := e.subjects[subjectName]
	if sub == nil {
		return Snapshot{}, false
	}
	return e.snapshotOf(sub), true
}

// Start runs the pass worker and the run workers, and hands back the stop func that cancels
// them and waits. ctx bounds startup alone, and nothing here can fail — the queues were built in
// New, so work queued before Start (an Add, a Wake) simply waits there for the workers.
func (e *Engine) Start(context.Context) func(context.Context) error {
	e.mu.Lock()
	e.started = true
	e.mu.Unlock()

	// Not Start's context, which bounds startup: this one bounds the loops, so it lives until
	// the stop func cancels them.
	loopCtx, stopLoops := context.WithCancel(context.Background())

	e.wg.Go(func() { e.passLoop(loopCtx) })
	for range e.settings.workers {
		e.wg.Go(func() { e.runLoop(loopCtx) })
	}

	return func(ctx context.Context) error {
		stopLoops()
		return drain.WithContext(ctx, e.wg.Wait)
	}
}

// Close drops every subject and closes the queues. Past here nothing works them off.
func (e *Engine) Close() error {
	e.runQ.Close()
	e.passQ.Close()

	e.mu.Lock()
	defer e.mu.Unlock()

	for _, sub := range e.subjects {
		sub.stopTimer()
	}
	clear(e.subjects)
	return nil
}

// passLoop derives schedules until stopped. One worker: the pass is arithmetic under the
// engine's lock, so more would only contend for it — and one worker is what serializes OnPass
// per subject.
func (e *Engine) passLoop(ctx context.Context) {
	for {
		name, ok := e.passQ.Next(ctx)
		if !ok {
			return
		}
		e.pass(name)
		e.passQ.Done(name)
	}
}

// runLoop runs due probes until stopped. Several workers, because a run is bounded by a remote
// server and a fleet would otherwise be probed one server at a time. Nothing is serialized per
// subject, and nothing needs to be: the queue keys by probe, so one probe never runs twice at
// once, and a probe writes only the observable it owns.
func (e *Engine) runLoop(ctx context.Context) {
	for {
		k, ok := e.runQ.Next(ctx)
		if !ok {
			return
		}
		e.runProbe(ctx, k)
		e.runQ.Done(k)
	}
}

// pass re-derives one subject's schedule and publishes: due runs go on runQ, the soonest future
// time arms the subject's one timer, and OnPass fires after — outside the lock, serialized
// because passLoop is the one caller. It is the only publisher, so every state a reader sees
// carries a schedule that matches the answers beside it.
//
// A run in flight is left alone: NextAttempt is that run, and writing a schedule over it would
// erase both the in-flight mark and the schedule it was dispatched on. Its commit passes again.
func (e *Engine) pass(subjectName string) {
	e.mu.Lock()
	sub := e.subjects[subjectName]
	if sub == nil {
		e.mu.Unlock()
		return
	}

	now := time.Now()
	var soonest time.Time
	for id := range e.specs {
		a := &sub.obs[id].Attempts
		if a.InFlight() {
			continue
		}
		at := e.due(sub, probeID(id), now)
		a.schedule(at)

		switch {
		case at.IsZero():
			// Suspended: nothing is due, and the last answer stands.
		case at.After(now):
			if soonest.IsZero() || at.Before(soonest) {
				soonest = at
			}
		default:
			e.runQ.Add(key{subject: subjectName, probe: probeID(id)})
		}
	}

	sub.stopTimer()
	if !soonest.IsZero() {
		sub.timer = time.AfterFunc(soonest.Sub(now), func() { e.passQ.Add(subjectName) })
	}

	var snap Snapshot
	if e.onPass != nil {
		snap = e.snapshotOf(sub)
	}
	e.mu.Unlock()

	if e.onPass != nil {
		e.onPass(subjectName, snap)
	}
}

// dependenciesState is where a probe's dependencies collectively are; a failing one outranks an
// unanswered one, since it is a definitive reason not to run.
type dependenciesState int

const (
	dependenciesOK dependenciesState = iota
	dependenciesUnanswered
	dependenciesFailing
)

func (e *Engine) dependenciesOf(sub *subject, dependencies []probeID) dependenciesState {
	state := dependenciesOK
	for _, id := range dependencies {
		dep := sub.obs[id]
		switch {
		case dep.LastAttempt.Done() && !dep.OK():
			return dependenciesFailing
		case !dep.LastAttempt.Done():
			state = dependenciesUnanswered
		}
	}
	return state
}

// due is when probe id should next run, zero for nothing scheduled. The whole scheduling
// policy, in the order the cases have to be read. The caller has already set aside a run in
// flight.
func (e *Engine) due(sub *subject, id probeID, now time.Time) time.Time {
	a := sub.obs[id]
	if a.skipped {
		// The last run declined to record; only a Wake brings it back.
		return time.Time{}
	}

	cfg := e.specs[id].cfg

	switch e.dependenciesOf(sub, e.specs[id].dependencies) {
	case dependenciesUnanswered:
		// Nothing to say about a server nobody has tried, so the probe stays untouched
		// rather than recording a dependency that has not failed.
		return time.Time{}
	case dependenciesFailing:
		// One run records DependencyFailed and the rest of the outage costs nothing, which
		// is what keeps a dead cluster at one timeout per cycle instead of one per probe.
		if a.LastAttempt.Reason == ReasonDependencyFailed {
			return time.Time{}
		}
		return now
	}

	switch a.LastAttempt.Verdict {
	case VerdictSucceeded:
		// The interval unless the run asked for less: RequeueAfter accelerates and never
		// lengthens, and an unset one leaves the registration standing rather than
		// scheduling at zero.
		wait := cfg.interval
		if d := a.LastAttempt.requeueAfter; d > 0 && d < wait {
			wait = d
		}
		return a.LastAttempt.FinishedAt.Add(wait)
	case VerdictFailed:
		return a.LastAttempt.FinishedAt.Add(cfg.backoff.delay(a.Failures))
	case VerdictSuspended:
		if a.LastAttempt.Reason == ReasonDependencyFailed {
			// Its dependencies came back. This is the whole re-arm — nothing has to notice
			// the recovery and go looking for what suspended on it.
			return now
		}
		return time.Time{}
	default:
		// Never run, and runnable.
		return now
	}
}

// runProbe runs one due probe and commits what it found. The subject is captured first and
// re-checked at the commit — what can race a run is a Remove, not another run of the same key.
//
// Dependencies are re-checked at dispatch: one that failed since the pass means the run is
// recorded as DependencyFailed, never dialed — a worker must not spend a timeout learning what
// the state already says. One still unanswered means a Wake outran it; the run ends as a no-op
// and the pass owns the question again.
func (e *Engine) runProbe(ctx context.Context, k key) {
	e.mu.Lock()
	sub := e.subjects[k.subject]
	if sub == nil {
		e.mu.Unlock()
		return
	}
	sp := e.specs[k.probe]
	prev := sub.obs[k.probe].value
	snap := e.snapshotOf(sub)
	dependencies := e.dependenciesOf(sub, sp.dependencies)
	// Marked before the lock is dropped, so InFlight is true for as long as the request is
	// out and a pass landing meanwhile leaves the run alone.
	startedAt := time.Now()
	sub.obs[k.probe].begin(startedAt)
	e.mu.Unlock()

	var res Result
	var val any
	var discard func()
	ran := false
	switch dependencies {
	case dependenciesOK:
		res, val, discard = e.dispatch(ctx, sp, k.subject, prev, snap)
		ran = true
	case dependenciesFailing:
		res = Suspend(ReasonDependencyFailed, "a dependency is failing")
		startedAt = time.Time{} // recorded, never dispatched
	default: // dependenciesUnanswered
		res = Skip()
	}
	e.commit(k, sub, startedAt, res, val, ran, discard)
}

// dispatch runs the probe's body, bounded by its timeout, and answers for it when it misbehaves.
// A body that panics or hands back nothing is a bug, and both must still produce a result —
// otherwise the probe reads as in flight forever and its key stays held in the queue. Only here
// does the engine log: the Internal record a caller sees carries no stack, and nothing else in
// the system can report a bug in a body.
func (e *Engine) dispatch(ctx context.Context, sp spec, subjectName string, prev any, snap Snapshot) (res Result, val any, discard func()) {
	if sp.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, sp.cfg.timeout)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Error("probe: run panicked", "probe", sp.name, "subject", subjectName,
				"panic", r, "stack", string(debug.Stack()))
			// The run handed its value back on the way out of the panic, so there is
			// nothing left here to discard.
			res, val, discard = Fail(ReasonInternal, fmt.Errorf("probe %s panicked: %v", sp.name, r)), nil, nil
		}
	}()

	res, val, discard = sp.run(ctx, subjectName, prev, snap)
	if res.kind == resultInvalid {
		slog.Error("probe: run returned the zero Result", "probe", sp.name, "subject", subjectName)
		if discard != nil {
			discard()
		}
		return Fail(ReasonInternal, fmt.Errorf("probe %s returned the zero Result", sp.name)), nil, nil
	}
	return res, val, discard
}

// commit files what a run concluded and asks for a pass. Called for every run, including one
// that concluded nothing: ending the run is what lets the pass schedule this probe again.
//
// The subject is re-checked under the lock, so a Remove landing mid-run cannot let a run that
// predates a re-Add be committed against whatever holds the name now.
func (e *Engine) commit(k key, held *subject, startedAt time.Time, res Result, val any, ran bool, discard func()) {
	e.mu.Lock()
	if e.subjects[k.subject] != held {
		e.mu.Unlock()
		// Nothing will ever see what this run committed, so it goes back to the probe: a
		// value can own something nothing else can now reach to release.
		if discard != nil {
			discard()
		}
		return
	}

	a := &held.obs[k.probe]
	now := time.Now()
	switch res.kind {
	case resultRecord:
		a.record(Attempt{
			StartedAt:  startedAt,
			FinishedAt: now,
			Verdict:    res.verdict,
			Reason:     res.reason,
			Message:    res.message,
			Err:        res.err,

			requeueAfter: res.requeueAfter,
		})
		a.skipped = false
		switch {
		case val != nil:
			// seen dates the value, and a value is confirmed when it is committed —
			// whatever the verdict, since a run that failed can still have read something
			// (which components are down). Dating it by the last success instead would
			// leave this answer undated, and would date a replaced one by a read of what
			// it replaced.
			a.value, a.seen = val, now
			// A committed value is one that moved — the body says so by handing one back at
			// all — and whoever reads it is owed a run against the new one. Queued in the same
			// critical section as the write, so no reader sees the value without the runs it
			// earned. Health is not consulted: a watch is a data edge, and a probe that also
			// cannot run without this declares WithDependencies for that.
			for _, watcher := range e.watchers[k.probe] {
				e.runQ.Add(key{subject: k.subject, probe: watcher})
			}
		case res.verdict == VerdictSucceeded && a.value != nil:
			// Nothing new, and the run confirmed what stands. A success with nothing to
			// date leaves seen alone: a reader's Known guard would otherwise pass and hand
			// it the zero value.
			a.seen = now
		}
	case resultSkip:
		if ran {
			a.skipped = true
		}
		// A Skip records nothing, its value included.
		if discard != nil {
			defer discard()
		}
	}
	// Due now rather than suspended: the pass this asks for is a queue hop away, and a zero
	// here would read as suspended for as long as that takes.
	a.schedule(now)
	e.mu.Unlock()

	e.passQ.Add(k.subject)
}
