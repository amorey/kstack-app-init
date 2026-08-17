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

// The probe loop behind a set of credentials: what starts it, what keeps it going, and
// what it does with an outcome. One loop per key, and only while something wants one.
package kubeconn

import (
	"context"
	"time"
)

// demand asks for a probe, starting the loop if none is running.
//
// Callers hold the mutex. Whether a loop is needed and whether one exists are the same
// question, and answering them apart would leave a claim with no loop behind it.
//
// probing is set here rather than when the probe starts, so a caller that asks and then
// reads State is not told nothing is happening while its request waits for a slot.
//
// It refuses once the pool is winding down, which is the window entryFor cannot close: that
// re-checks under the lock and then releases it, so a shutdown can land before the caller
// gets here. Refusing has to reach the caller — no loop starts after this, so a claim taken
// here would wait on a result nothing was going to produce, and a probe asked for here would
// never run.
func (s *Service) demand(key string, e *entry) error {
	if s.shuttingDown() {
		return ErrClosed
	}
	e.kick()
	e.probing = true
	if e.running {
		return nil
	}
	e.running = true
	s.wg.Go(func() { s.run(s.ctx, key, e) })
	return nil
}

// run probes, then waits for whatever asks for the next one: the cadence while the
// credentials are leased, another ask, or shutdown.
func (s *Service) run(ctx context.Context, key string, e *entry) {
	delay := s.budget.RetryBase
	next := time.NewTimer(0)
	next.Stop()
	defer next.Stop()

	for {
		// Waits before the first probe: pooling a connection costs nothing until something
		// asks to have it validated.
		select {
		case <-ctx.Done():
			s.endLoopOnShutdown(e)
			return
		case <-e.idle:
			// A claim was dropped. Leave now if nothing else wants this loop, rather than
			// sitting on the timer to run one more probe on the way out.
			if _, keep := s.keepRunning(e); !keep {
				return
			}
			continue
		case <-e.wake:
		case <-next.C:
		}

		res := s.probeOnce(ctx, key, e)
		if ctx.Err() != nil {
			s.endLoopOnShutdown(e)
			return
		}

		leased, keep := s.keepRunning(e)
		if !keep {
			return
		}

		// Only a claim arms the cadence. An ask on its own wanted one answer, and got it.
		if leased {
			if res.Failed() {
				next.Reset(delay)
				delay = nextDelay(delay, s.budget.RetryMax)
			} else {
				next.Reset(s.budget.Cadence)
				delay = s.budget.RetryBase
			}
		}
	}
}

// keepRunning decides whether the loop goes on, and reports whether a claim is what keeps
// it going. It clears running when the answer is no.
//
// The decision and that flag are taken together, under the lock demand also takes: a claim
// arriving mid-decision therefore either keeps this loop or starts a fresh one, never
// neither — which would leave a caller parked in Conn with nothing left to answer it.
func (s *Service) keepRunning(e *entry) (leased, keep bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e.leases == 0 && !pending(e) {
		e.running = false
		return false, false
	}
	return e.leases > 0, true
}

// endLoopOnShutdown records that this entry has no loop and nothing pending on it, without
// the decision above. Only the shutdown paths may take it that way: stopped is set before
// the loops are cancelled, so no demand can start another and find one already claimed.
func (s *Service) endLoopOnShutdown(e *entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e.running = false
	// Or every key with a probe in flight at shutdown reports Probing for good.
	e.probing = false
}

// pending reports whether a probe was asked for while the last one ran — which is what the
// wake channel's buffer already is. Distinct from entry.probing, which stays true for the
// whole run of a probe: the loop drains the wake before probing, so during one they differ,
// and this is what probing is re-derived from once the probe lands.
func pending(e *entry) bool { return len(e.wake) > 0 }

// nextDelay returns the delay to follow one of d: doubled, then held at max.
func nextDelay(d, max time.Duration) time.Duration {
	return min(2*d, max)
}

// probeOnce probes over the pooled connection and publishes the outcome, to whoever is
// parked in Conn and to the observers.
func (s *Service) probeOnce(ctx context.Context, key string, e *entry) Result {
	var res Result

	// Announced before the slot is won, not after: with a fleet declared in one pass most
	// keys spend that wait queued, which is exactly the stretch this covers. Only when
	// nothing is known yet — once a result stands, a running probe is just the cadence and
	// says nothing new.
	s.mu.Lock()
	first := e.result == nil
	s.mu.Unlock()
	if first {
		s.announce(key)
	}

	select {
	case s.slots <- struct{}{}:
	case <-ctx.Done():
		return res
	}

	probeCtx, cancel := context.WithTimeout(ctx, s.budget.Timeout)
	res.Identity, res.Err = s.probe(probeCtx, e.conn)
	cancel()
	// The slot bounds dialing; what follows is a callback nothing else should queue
	// behind.
	<-s.slots

	if ctx.Err() != nil {
		// Cancelled with the loop: the error describes the shutdown, not the cluster.
		return res
	}

	s.mu.Lock()
	news := !res.sameAs(e.result)
	// Stored before it is sent, so a woken caller reading back through State cannot see
	// the older one.
	e.result = &res
	asked := e.probing
	e.probing = pending(e)
	// An explicit ask is answered even when the answer repeats: a user retrying a cluster
	// that is still down has watched Probing go true, and nothing else would turn it back
	// off for them. The cadence sets no such flag, so this stays quiet between asks.
	answered := asked && !e.probing
	s.mu.Unlock()

	// The send fails only against a closed bus, which is shutdown: the loops drain before
	// Close, so there is nobody left to tell.
	_ = s.resultsHub.Sender().Send(key, &res)

	// Telling a reader what it already holds costs it a pass that can only conclude nothing
	// moved — per set of credentials, every cadence, forever.
	if news || answered {
		s.announce(key)
	}
	return res
}

// announce publishes that a key moved. Never blocks: the bus coalesces per key, which is
// what lets a probe loop raise news without waiting on whoever reads it.
func (s *Service) announce(key string) {
	_ = s.newsHub.Sender().Send(key, struct{}{})
}
