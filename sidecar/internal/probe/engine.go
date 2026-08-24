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
// The contract is docs/specs/probe-engine.md; the invariants each unexported method must hold
// are on the method. The engine bodies are being ported from
// sidecar/internal/clustersvc/internal/kubeconn, whose tests pin the same rules.
package probe

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/workqueue"
)

// ID is a probe's registration index, returned by Register. Read and OnChange hand back
// attempts indexed by it.
type ID int

// Run is one probe's body: request against the snapshot, classify, and hand back the result
// plus a commit — nil to write nothing. The commit is applied by the engine under its lock,
// so a run must do its waiting before it returns.
type Run[S any] func(ctx context.Context, snap S) (Result, func(*S))

// Backoff paces a failing probe: Base widened by Factor per consecutive failure, capped at Cap.
// The ladder is a pure function of Attempts.Failures — ScheduledAt is rendered by the frontend,
// so the next time must be derivable from recorded state and stable across passes. No jitter,
// and no stateful rate limiter.
type Backoff struct {
	Base   time.Duration
	Factor float64
	Cap    time.Duration
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
	// timer brings the pass back when the soonest scheduled run comes due. One per subject,
	// and it is a wake, not a cadence: the pass decides again per probe.
	timer *time.Timer
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
// duplicate name, or a call after Start — a table wired wrong at boot, not a runtime error.
func (e *Engine[S]) Register(name string, run Run[S], opts ...ProbeOption) ID {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.started {
		panic("probe: Register after Start")
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

// Add tracks subject. Every probe derives from zero, so whatever is runnable for a fresh
// subject is queued by its first pass; adding a subject already tracked changes nothing.
func (e *Engine[S]) Add(subject string) {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// Remove stops tracking subject: its timer stops, and a run still in flight against it commits
// nothing. Removing a subject not tracked changes nothing.
func (e *Engine[S]) Remove(subject string) {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// Wake says these probes' answers are stale: run them again, suspension notwithstanding. It
// adds straight to the run queue, whose held/dirty machinery redelivers a key that was mid-run
// when its commit lands — a Wake is never lost.
func (e *Engine[S]) Wake(subject string, ids ...ID) {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// WakeAll is Wake over every tracked subject.
func (e *Engine[S]) WakeAll(ids ...ID) {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// Read is subject's snapshot and attempts (indexed by ID), copied under one lock so the values
// and the schedule beside them agree. ok is false for a subject not tracked.
func (e *Engine[S]) Read(subject string) (snap S, at []Attempts, ok bool) {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// Start runs the pass worker and the run workers; the stop func cancels them and waits. ctx
// bounds startup alone.
func (e *Engine[S]) Start(ctx context.Context) (func(context.Context) error, error) {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// Close drops every subject and closes the queues. Past here nothing works them off.
func (e *Engine[S]) Close() error {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// pass re-derives one subject's schedule and publishes: due runs go on runQ, the soonest future
// time arms the subject's one timer, and OnChange fires after — outside the lock, serialized per
// subject because passQ runs one worker. It is the only publisher, so every state a reader sees
// carries a schedule that matches the answers beside it.
//
// Port of kubeconn's reconcile.
func (e *Engine[S]) pass(subjectName string) {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// due is when probe id should next run for sub, zero for nothing scheduled. The whole scheduling
// policy: a run in flight schedules nothing (its commit passes again); the dependency lifecycle
// (untouched before its needs have answered, one DependencyFailed record then suspension while
// any is failing, due at once when they recover); then the verdict — Succeeded waits out the
// interval, Failed climbs the ladder, Suspended and never-run-after-Skip wait for a Wake.
//
// Port of kubeconn's due, with the Reason cases replaced by Verdict.
func (e *Engine[S]) due(sub *subject[S], id ID, now time.Time) time.Time {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}

// runProbe runs one due probe and commits what it found. The subject is captured first and
// re-checked at the commit — what can race a run is a Remove, not another run of the same key.
// Needs is re-checked at dispatch: a dependency that failed since the pass means the run is
// recorded as DependencyFailed, never dialed. begin is marked before the lock is dropped, the
// run is bounded by its timeout, a panic records Fail("Internal") rather than wedging the key,
// and the commit ends the run, applies the write, and asks for a pass in one critical section.
//
// Port of kubeconn's runCheck + commitCheck.
func (e *Engine[S]) runProbe(ctx context.Context, k key) {
	panic("probe: not implemented — see docs/specs/probe-engine.md")
}
