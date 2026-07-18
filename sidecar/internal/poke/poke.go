// Package poke is a cross-subsystem resync broadcaster. It owns a wall-clock
// gap detector (the backstop for machine sleep/resume) and fans poke Signals
// out to all subscribers via a gochan broadcast hub. Callers subscribe or
// call Poke directly.
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
	// SourceHost is set by the gRPC resync handler when the Tauri host
	// forwards an OS wake or network-on event.
	SourceHost Source = "host"
)

// Signal is broadcast to all subscribers on every poke.
type Signal struct {
	Source Source
	At     time.Time
}

// options configures a Service: the gap-detector tuning plus the clock and
// ticker seams. Zero values get defaults via applyDefaults. Unexported because
// production tunes nothing — only white-box tests inject seams via
// newWithOptions.
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

// option is an unexported build seam for newWithOptions, so only white-box
// tests can construct one; production New takes no seams.
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
}

// New builds a Service with production defaults. It does not start anything;
// call Start (or Run).
func New() *Service {
	return newWithOptions()
}

// newWithOptions is the build entry point that also accepts the unexported test
// seams; New is the production wrapper (no options).
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
// The entry point for the gRPC resync handler and any in-process caller that
// detects a wake or network-return event.
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

// Start launches Run in a goroutine bound to a context derived from ctx. ctx
// scopes initialization only; the returned stop func cancels the detector and
// blocks until it exits (which closes the hub and all subscriber channels),
// honoring its own drain-deadline context. Mirrors cluster.Service/cloud.Service
// so the composition root can own the lifecycle uniformly. Call once.
func (s *Service) Start(ctx context.Context) (func(context.Context) error, error) {
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(runCtx)
	}()

	stop := func(ctx context.Context) error {
		cancel()
		select {
		case <-done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return stop, nil
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
