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
// Register names one probe with its cadence and the probes it Needs; Add and Remove track
// subjects, and Wake says a recorded answer went stale; Read and OnChange hand back the caller's
// snapshot beside the Attempts for each probe.
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

// ID is a probe's registration index, returned by Register. Read and OnChange hand back
// attempts indexed by it.
type ID int

// Run is one probe's body: request against the subject named, classify, and hand back the
// result plus a commit — nil to write nothing. The commit is applied by the engine under its
// lock, so a run must do its waiting before it returns.
type Run[S any] func(ctx context.Context, subject string, snap S) (Result, func(*S))

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

// probeCfg is the per-probe knobs, all optional. ProbeOption stays non-generic by writing here
// rather than into the spec.
type probeCfg struct {
	interval time.Duration
	backoff  Backoff
	timeout  time.Duration
	needs    []ID
}

var defaultCfg = probeCfg{
	interval: time.Minute,
	backoff:  Backoff{Base: time.Second, Factor: 2, Cap: 5 * time.Minute},
	timeout:  30 * time.Second,
}

// ProbeOption tunes one registration; a registration states only what deviates from the
// defaults.
type ProbeOption func(*probeCfg)

// Needs declares the probes this one can only run over the success of. It takes IDs Register
// already returned, so a dependency exists before its dependent and the graph is acyclic by
// construction.
func Needs(ids ...ID) ProbeOption {
	return func(c *probeCfg) { c.needs = append(c.needs, ids...) }
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

// spec is one registered probe.
type spec[S any] struct {
	name string
	cfg  probeCfg
	run  Run[S]
}

// Option configures an Engine. Non-generic on purpose — the one hook that needs S is set with
// Engine.OnChange instead.
type Option func(*settings)

// WithWorkers is how many runs may be in flight at once, across every subject.
func WithWorkers(n int) Option { return func(s *settings) { s.workers = n } }

// settings is what the options write.
type settings struct {
	workers int
}

// Engine runs the registered probes over the tracked subjects. Build with New, add probes with
// Register, then Start; the zero Engine has no queues to work.
type Engine[S any] struct {
	settings settings
	onChange func(subject string, snap S, at []Attempts)

	// runQ carries the runs that are due, one key per probe per subject, so one probe never
	// runs twice at once and an ask arriving mid-run is redelivered on Done rather than folded
	// into a run that could not have seen it. passQ carries the subjects whose schedule has to
	// be re-derived; everything that changes a subject ends with an add there.
	runQ  *workqueue.Queue[key]
	passQ *workqueue.Queue[string]

	// mu guards specs, started, and subjects together with the entries behind them: a
	// snapshot and its attempts are read against each other, and nothing may see one without
	// the other.
	mu       sync.Mutex
	specs    []spec[S]
	started  bool
	subjects map[string]*subject[S]

	wg sync.WaitGroup
}

// key is one run of one probe against one subject — the unit runQ keys on.
type key struct {
	subject string
	probe   ID
}

// subject is what the engine holds for one tracked name. Identity matters: a subject removed
// and re-added is a new *subject, and a run dispatched against the old one commits nothing.
type subject[S any] struct {
	snap     S
	attempts []Attempts // indexed by ID
	// skipped marks probes whose last run returned Skip — the one memory a Skip leaves. It
	// records nothing, so without this the pass would read the probe as never-run and
	// re-dispatch it at once. Cleared when a run records; a Wake goes through runQ, so it
	// needs no clearing to force a run.
	skipped []bool
	// timer brings the pass back when the soonest scheduled run comes due. One per subject,
	// and it is a wake, not a cadence: the pass decides again per probe.
	timer *time.Timer
}

func (s *subject[S]) stopTimer() {
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
}

// New returns an Engine with no probes and no subjects.
func New[S any](opts ...Option) *Engine[S] {
	e := &Engine[S]{
		settings: settings{workers: 8},
		runQ:     workqueue.New[key](),
		passQ:    workqueue.New[string](),
		subjects: map[string]*subject[S]{},
	}
	for _, opt := range opts {
		opt(&e.settings)
	}
	return e
}

// OnChange sets the callback the engine fires after every pass, with the snapshot and attempts
// that pass produced. Called outside the engine's lock but serialized per subject; it must not
// block. Wiring, not state — set it before Start, like Register.
func (e *Engine[S]) OnChange(fn func(subject string, snap S, at []Attempts)) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		panic("probe: OnChange after Start")
	}
	e.onChange = fn
}

// Register adds one probe and returns its ID. It panics on a Needs entry not yet registered, a
// duplicate name, or a call after Start or the first Add — a table wired wrong at boot, not a
// runtime error. A subject's bookkeeping is sized when it is added, which is why the set must
// be complete before anything is tracked.
func (e *Engine[S]) Register(name string, run Run[S], opts ...ProbeOption) ID {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		panic("probe: Register after Start")
	}
	if len(e.subjects) != 0 {
		panic("probe: Register after Add")
	}
	if name == "" || run == nil {
		panic("probe: Register needs a name and a run")
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
	for _, need := range cfg.needs {
		if need < 0 || int(need) >= len(e.specs) {
			panic(fmt.Sprintf("probe: %q needs probe %d, which is not registered yet", name, need))
		}
	}

	e.specs = append(e.specs, spec[S]{name: name, cfg: cfg, run: run})
	return ID(len(e.specs) - 1)
}

// Add tracks subject. Every probe derives from zero, so the pass this queues dispatches
// whatever a fresh subject owes; adding a subject already tracked changes nothing.
func (e *Engine[S]) Add(subjectName string) {
	e.mu.Lock()
	if _, ok := e.subjects[subjectName]; ok {
		e.mu.Unlock()
		return
	}
	e.subjects[subjectName] = &subject[S]{
		attempts: make([]Attempts, len(e.specs)),
		skipped:  make([]bool, len(e.specs)),
	}
	e.mu.Unlock()

	e.passQ.Add(subjectName)
}

// Remove stops tracking subject: its timer stops, and a run still in flight against it commits
// nothing. Removing a subject not tracked changes nothing.
func (e *Engine[S]) Remove(subjectName string) {
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
// when its commit lands — a Wake is never lost. A subject not tracked is ignored; an ID never
// registered is a wiring bug and panics.
func (e *Engine[S]) Wake(subjectName string, ids ...ID) {
	e.mu.Lock()
	tracked := e.subjects[subjectName] != nil
	n := len(e.specs)
	e.mu.Unlock()

	for _, id := range ids {
		if id < 0 || int(id) >= n {
			panic(fmt.Sprintf("probe: Wake of probe %d, which is not registered", id))
		}
	}
	if !tracked {
		return
	}
	for _, id := range ids {
		e.runQ.Add(key{subject: subjectName, probe: id})
	}
}

// WakeAll is Wake over every tracked subject.
func (e *Engine[S]) WakeAll(ids ...ID) {
	e.mu.Lock()
	names := slices.Collect(maps.Keys(e.subjects))
	e.mu.Unlock()

	for _, name := range names {
		e.Wake(name, ids...)
	}
}

// Read is subject's snapshot and attempts (indexed by ID), copied under one lock so the values
// and the schedule beside them agree. ok is false for a subject not tracked.
func (e *Engine[S]) Read(subjectName string) (snap S, at []Attempts, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	sub := e.subjects[subjectName]
	if sub == nil {
		var zero S
		return zero, nil, false
	}
	return sub.snap, slices.Clone(sub.attempts), true
}

// Start runs the pass worker and the run workers; the stop func cancels them and waits. ctx
// bounds startup alone. The queues were built in New, so work queued before Start — an Add, a
// Wake — waits there for the workers.
func (e *Engine[S]) Start(context.Context) (func(context.Context) error, error) {
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
	}, nil
}

// Close drops every subject and closes the queues. Past here nothing works them off.
func (e *Engine[S]) Close() error {
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
// engine's lock, so more would only contend for it — and one worker is what serializes OnChange
// per subject.
func (e *Engine[S]) passLoop(ctx context.Context) {
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
// once, and a probe writes only the attempts it owns.
func (e *Engine[S]) runLoop(ctx context.Context) {
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
// time arms the subject's one timer, and OnChange fires after — outside the lock, serialized
// because passLoop is the one caller. It is the only publisher, so every state a reader sees
// carries a schedule that matches the answers beside it.
//
// A run in flight is left alone: NextAttempt is that run, and writing a schedule over it would
// erase both the in-flight mark and the schedule it was dispatched on. Its commit passes again.
func (e *Engine[S]) pass(subjectName string) {
	e.mu.Lock()
	sub := e.subjects[subjectName]
	if sub == nil {
		e.mu.Unlock()
		return
	}

	now := time.Now()
	var soonest time.Time
	for id := range e.specs {
		a := &sub.attempts[id]
		if a.InFlight() {
			continue
		}
		at := e.due(sub, ID(id), now)
		a.schedule(at)

		switch {
		case at.IsZero():
			// Suspended: nothing is due, and the last answer stands.
		case at.After(now):
			if soonest.IsZero() || at.Before(soonest) {
				soonest = at
			}
		default:
			e.runQ.Add(key{subject: subjectName, probe: ID(id)})
		}
	}

	sub.stopTimer()
	if !soonest.IsZero() {
		sub.timer = time.AfterFunc(soonest.Sub(now), func() { e.passQ.Add(subjectName) })
	}

	var snap S
	var at []Attempts
	if e.onChange != nil {
		snap, at = sub.snap, slices.Clone(sub.attempts)
	}
	e.mu.Unlock()

	if e.onChange != nil {
		e.onChange(subjectName, snap, at)
	}
}

// needsState is where a probe's dependencies collectively are; a failing one outranks an
// unanswered one, since it is a definitive reason not to run.
type needsState int

const (
	needsOK needsState = iota
	needsUnanswered
	needsFailing
)

func (e *Engine[S]) needsOf(sub *subject[S], needs []ID) needsState {
	state := needsOK
	for _, id := range needs {
		dep := sub.attempts[id]
		switch {
		case dep.LastAttempt.Done() && !dep.OK():
			return needsFailing
		case !dep.LastAttempt.Done():
			state = needsUnanswered
		}
	}
	return state
}

// due is when probe id should next run, zero for nothing scheduled. The whole scheduling
// policy, in the order the cases have to be read. The caller has already set aside a run in
// flight.
func (e *Engine[S]) due(sub *subject[S], id ID, now time.Time) time.Time {
	if sub.skipped[id] {
		// The last run declined to record; only a Wake brings it back.
		return time.Time{}
	}

	a := sub.attempts[id]
	cfg := e.specs[id].cfg

	switch e.needsOf(sub, cfg.needs) {
	case needsUnanswered:
		// Nothing to say about a server nobody has tried, so the probe stays untouched
		// rather than recording a dependency that has not failed.
		return time.Time{}
	case needsFailing:
		// One run records DependencyFailed and the rest of the outage costs nothing, which
		// is what keeps a dead cluster at one timeout per cycle instead of one per probe.
		if a.LastAttempt.Reason == ReasonDependencyFailed {
			return time.Time{}
		}
		return now
	}

	switch a.LastAttempt.Verdict {
	case VerdictSucceeded:
		return a.LastAttempt.FinishedAt.Add(cfg.interval)
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
// Needs is re-checked at dispatch: a dependency that failed since the pass means the run is
// recorded as DependencyFailed, never dialed — a worker must not spend a timeout learning what
// the state already says. One still unanswered means a Wake outran it; the run ends as a no-op
// and the pass owns the question again.
func (e *Engine[S]) runProbe(ctx context.Context, k key) {
	e.mu.Lock()
	sub := e.subjects[k.subject]
	if sub == nil {
		e.mu.Unlock()
		return
	}
	sp := e.specs[k.probe]
	snap := sub.snap
	needs := e.needsOf(sub, sp.cfg.needs)
	// Marked before the lock is dropped, so InFlight is true for as long as the request is
	// out and a pass landing meanwhile leaves the run alone.
	startedAt := time.Now()
	sub.attempts[k.probe].begin(startedAt)
	e.mu.Unlock()

	var res Result
	var apply func(*S)
	ran := false
	switch needs {
	case needsOK:
		res, apply = e.dispatch(ctx, sp, k.subject, snap)
		ran = true
	case needsFailing:
		res = Suspend(ReasonDependencyFailed, "a dependency is failing")
		startedAt = time.Time{} // recorded, never dispatched
	default: // needsUnanswered
		res = Skip()
	}
	e.commit(k, sub, startedAt, res, apply, ran)
}

// dispatch runs the probe's body, bounded by its timeout, and answers for it when it misbehaves.
// A body that panics or hands back nothing is a bug, and both must still produce a result —
// otherwise the probe reads as in flight forever and its key stays held in the queue. Only here
// does the engine log: the Internal record a caller sees carries no stack, and nothing else in
// the system can report a bug in a body.
func (e *Engine[S]) dispatch(ctx context.Context, sp spec[S], subjectName string, snap S) (res Result, apply func(*S)) {
	if sp.cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, sp.cfg.timeout)
		defer cancel()
	}
	defer func() {
		if r := recover(); r != nil {
			slog.Error("probe: run panicked", "probe", sp.name, "subject", subjectName,
				"panic", r, "stack", string(debug.Stack()))
			res, apply = Fail(ReasonInternal, fmt.Errorf("probe %s panicked: %v", sp.name, r)), nil
		}
	}()

	res, apply = sp.run(ctx, subjectName, snap)
	if res.kind == resultInvalid {
		slog.Error("probe: run returned the zero Result", "probe", sp.name, "subject", subjectName)
		return Fail(ReasonInternal, fmt.Errorf("probe %s returned the zero Result", sp.name)), nil
	}
	return res, apply
}

// commit files what a run concluded and asks for a pass. Called for every run, including one
// that concluded nothing: ending the run is what lets the pass schedule this probe again.
//
// The subject is re-checked under the lock, so a Remove landing mid-run cannot let a run that
// predates a re-Add be committed against whatever holds the name now.
func (e *Engine[S]) commit(k key, held *subject[S], startedAt time.Time, res Result, apply func(*S), ran bool) {
	e.mu.Lock()
	if e.subjects[k.subject] != held {
		e.mu.Unlock()
		return
	}

	a := &held.attempts[k.probe]
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
		})
		held.skipped[k.probe] = false
		if apply != nil {
			apply(&held.snap)
		}
	case resultSkip:
		if ran {
			held.skipped[k.probe] = true
		}
	}
	// Due now rather than suspended: the pass this asks for is a queue hop away, and a zero
	// here would read as suspended for as long as that takes.
	a.schedule(now)
	e.mu.Unlock()

	e.passQ.Add(k.subject)
}
