// Package poke is a cross-subsystem resync broadcaster. It owns a wall-clock
// gap detector (the backstop for machine sleep/resume) and fans poke Signals
// out to all subscribers via a gochan broadcast hub. Any caller — today the
// cloud settings engine, tomorrow the cluster-sync engine or a gRPC handler
// from the host — subscribes or calls Poke directly.
package poke

import (
	"context"
	"time"

	"github.com/amorey/gochan/broadcast"
)

const hubCapacity = 2

// Source identifies what triggered a poke.
type Source string

const (
	// SourceWallClock is set by the built-in gap detector when it infers a
	// machine sleep/resume from a wall-clock jump.
	SourceWallClock Source = "wallclock"
	// SourceHost is set by the gRPC resync handler (follow-up PR) when the
	// Tauri host forwards an OS wake or network-on event.
	SourceHost Source = "host"
)

// Signal is broadcast to all subscribers on every poke.
type Signal struct {
	Source Source
	At     time.Time
}

// options configures a Service: the gap-detector tuning plus the clock and
// ticker seams. Zero values get sane defaults via applyDefaults. The struct and
// its with* seams are unexported because production tunes nothing — New takes no
// arguments, and only this package's white-box tests inject seams via
// newWithOptions, mirroring the auth/cloud/prefsync option convention.
type options struct {
	tick      time.Duration // gap-detector heartbeat (default 15s)
	gapFactor float64       // wall gap > tick*gapFactor ⇒ treat as resume (default 2.0)
	now       func() time.Time
	newTicker func(time.Duration) (<-chan time.Time, func())
}

func (o *options) applyDefaults() {
	if o.tick <= 0 {
		o.tick = 15 * time.Second
	}
	if o.gapFactor <= 0 {
		o.gapFactor = 2.0
	}
	if o.now == nil {
		o.now = time.Now
	}
	if o.newTicker == nil {
		o.newTicker = func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		}
	}
}

// option is an unexported build seam for newWithOptions. Because the type is
// unexported, only this package's white-box tests can construct one — the
// production New takes no seams. Mirrors the auth/cloud/prefsync option pattern.
type option func(*options)

// withTick overrides the gap-detector heartbeat (default 15s).
func withTick(d time.Duration) option { return func(o *options) { o.tick = d } }

// withGapFactor overrides the resume-detection factor (default 2.0).
func withGapFactor(f float64) option { return func(o *options) { o.gapFactor = f } }

// withNow overrides the clock seam.
func withNow(f func() time.Time) option { return func(o *options) { o.now = f } }

// withTicker overrides the ticker seam.
func withTicker(f func(time.Duration) (<-chan time.Time, func())) option {
	return func(o *options) { o.newTicker = f }
}

// Service fans resync pokes to all subscribers. Construct with New, then
// call Start (or Run) to launch the wall-clock gap detector. Poke may be
// called at any time from any goroutine.
type Service struct {
	hub *broadcast.Hub[Signal]
	tx  *broadcast.Sender[Signal]
	opt options

	cancel context.CancelFunc
	done   chan struct{} // closed when the Run goroutine exits
}

// New builds a Service with production defaults. It does not start anything;
// call Start (or Run). The gap-detector tuning and clock/ticker seams use
// defaults — only this package's white-box tests override them via
// newWithOptions.
func New() *Service {
	return newWithOptions()
}

// newWithOptions is the build entry point that also accepts the unexported test
// seams. New is the production wrapper (no options); in-package white-box tests
// call this directly to inject a deterministic clock/ticker.
func newWithOptions(opts ...option) *Service {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	o.applyDefaults()
	hub := broadcast.New[Signal](hubCapacity)
	return &Service{
		hub: hub,
		tx:  hub.Sender(),
		opt: o,
	}
}

// Poke broadcasts a Signal to all active subscribers. Never blocks; idempotent.
// This is the entry point for the gRPC resync handler (follow-up PR) and for
// any in-process caller that detects a wake or network-return event.
func (s *Service) Poke(src Source) {
	_ = s.tx.Send(Signal{Source: src, At: s.opt.now()})
}

// Subscribe returns a channel that receives Signals and a cancel function.
// The channel is closed when the subscription is cancelled or Run's context
// ends. Calling cancel when done is required — abandoning the channel without
// cancelling leaks the feeder goroutine.
func (s *Service) Subscribe() (<-chan Signal, func()) {
	rx := s.hub.Receiver()
	return rx.Chan(), rx.Close
}

// Start launches Run in a goroutine bound to a context derived from ctx, so a
// cancel of ctx (or Close) stops it. Mirrors cluster.Service/cloud.Service so
// the composition root can own the lifecycle uniformly. Call once.
func (s *Service) Start(ctx context.Context) {
	runCtx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	s.done = make(chan struct{})
	go func() {
		defer close(s.done)
		s.Run(runCtx)
	}()
}

// Close cancels the running detector and waits for it to exit (which closes the
// hub and all subscriber channels). Safe to call without Start.
func (s *Service) Close() {
	if s.cancel != nil {
		s.cancel()
		<-s.done
	}
}

// Run starts the wall-clock gap detector and blocks until ctx is cancelled.
// On exit it closes the hub so all subscriber channels are closed. Call once
// per Service (Start wraps this in a goroutine).
func (s *Service) Run(ctx context.Context) {
	tickC, stop := s.opt.newTicker(s.opt.tick)
	defer stop()

	lastSeen := s.opt.now()
	threshold := time.Duration(float64(s.opt.tick) * s.opt.gapFactor)

	for {
		select {
		case <-ctx.Done():
			s.hub.Close()
			return
		case <-tickC:
			now := s.opt.now()
			if now.Sub(lastSeen) > threshold {
				s.Poke(SourceWallClock)
			}
			lastSeen = now
		}
	}
}
