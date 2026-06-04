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

// Options configures a Service. Zero values get sane defaults.
type Options struct {
	Tick      time.Duration // gap-detector heartbeat (default 15s)
	GapFactor float64       // wall gap > Tick*GapFactor ⇒ treat as resume (default 2.0)
	Now       func() time.Time
	// NewTicker returns a tick channel and a stop function. Injectable for tests.
	NewTicker func(time.Duration) (<-chan time.Time, func())
}

func (o *Options) applyDefaults() {
	if o.Tick <= 0 {
		o.Tick = 15 * time.Second
	}
	if o.GapFactor <= 0 {
		o.GapFactor = 2.0
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.NewTicker == nil {
		o.NewTicker = func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		}
	}
}

// Service fans resync pokes to all subscribers. Construct with New, then
// call Start (or Run) to launch the wall-clock gap detector. Poke may be
// called at any time from any goroutine.
type Service struct {
	hub *broadcast.Hub[Signal]
	tx  *broadcast.Sender[Signal]
	opt Options

	cancel context.CancelFunc
	done   chan struct{} // closed when the Run goroutine exits
}

// New builds a Service. It does not start anything; call Start (or Run).
func New(opt Options) *Service {
	opt.applyDefaults()
	hub := broadcast.New[Signal](hubCapacity)
	return &Service{
		hub: hub,
		tx:  hub.Sender(),
		opt: opt,
	}
}

// Poke broadcasts a Signal to all active subscribers. Never blocks; idempotent.
// This is the entry point for the gRPC resync handler (follow-up PR) and for
// any in-process caller that detects a wake or network-return event.
func (s *Service) Poke(src Source) {
	_ = s.tx.Send(Signal{Source: src, At: s.opt.Now()})
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
	tickC, stop := s.opt.NewTicker(s.opt.Tick)
	defer stop()

	lastSeen := s.opt.Now()
	threshold := time.Duration(float64(s.opt.Tick) * s.opt.GapFactor)

	for {
		select {
		case <-ctx.Done():
			s.hub.Close()
			return
		case <-tickC:
			now := s.opt.Now()
			if now.Sub(lastSeen) > threshold {
				s.Poke(SourceWallClock)
			}
			lastSeen = now
		}
	}
}
