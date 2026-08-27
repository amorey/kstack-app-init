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

package clustersvc

import (
	"context"
	"sync/atomic"

	"github.com/amorey/beehive"
)

// Stream is what a watch whose source can die returns: the frames, and why they
// stopped. Frames closes on every exit — ctx cancelled, source drained, source
// failed — so Err is the only thing that tells a failure apart from an ordinary
// teardown, and a consumer that ignores it turns a broken watch into a silent one.
//
// The rule is the SOURCE, not the shape: a watch over a fallible upstream returns a
// Stream, gauges included. One that retries forever and has no terminal failure to
// report may stay a plain channel.
//
// **A clean end is a nil Err, and some expected endings are clean.** The cached-data
// watches end that way when their cache is cleared: a user pressing a button is not a
// broken watch, and reporting one would put an error in front of them per open watch. The
// reason the "silent broken watch" argument above does not cover it is that the client's
// reconnect succeeds — there is no invisible retry loop, and the next snapshot is the
// report. Err is for a source that is actually broken.
type Stream[T any] struct {
	Frames <-chan T
	err    atomic.Pointer[error]
}

// Err returns why the stream ended, or nil if it ended cleanly. Read it once
// Frames has closed; before that it is always nil.
func (s *Stream[T]) Err() error {
	if p := s.err.Load(); p != nil {
		return *p
	}
	return nil
}

// sendFrame delivers one frame, reporting false when the consumer is gone. It is how
// a pump keeps NewStream's requirement below: a bare channel send blocks forever once
// the consumer stops draining, leaking the goroutine and the watch behind it.
func sendFrame[T any](ctx context.Context, out chan<- T, frame T) bool {
	select {
	case out <- frame:
		return true
	case <-ctx.Done():
		return false
	}
}

// deltaWatch is what one kind contributes to a record watch: how a stored object
// becomes a frame, how a removal does, and the bookmark that closes a snapshot. The
// pumps below are the rest, and are the same for every kind — which is the point, since
// the bookmark discipline is a protocol rule rather than a per-kind choice.
// See docs/adr/2026-08-09-delta-watch-protocol.md.
type deltaWatch[Spec, Status, Frame any] struct {
	// frame projects one object into a frame of the given type. Fallible because a
	// projection reads the owner edge, which is a store read.
	frame func(DeltaFrameType, *beehive.Object[Spec, Status]) (Frame, error)
	// departed builds the removal frame. Its own function rather than frame's business
	// because the row is gone: beehive loads no owner edge for it, and beehive reports a
	// removal whose final row it could not decode with no object at all. The frame must
	// still carry the id — nothing later in the log mentions a deleted one, and a
	// consumer drops a change with no entity rather than folding it, so a dropped frame
	// strands the record in its map for the life of the subscription.
	departed func(beehive.ObjectChange[Spec, Status]) Frame
	// bookmark closes the snapshot. A value rather than frame(DeltaFrameBookmark, nil),
	// which would make every projection handle a nil object it is never otherwise given.
	bookmark Frame
}

// streamOne serves a single-object watch. An id holding nothing yet is a snapshot of
// none rather than a failure: the record may still arrive, and this reports it.
func (w deltaWatch[Spec, Status, Frame]) streamOne(ctx context.Context, src *beehive.ObjectStream[Spec, Status]) *Stream[Frame] {
	return NewStream(ctx, func(ctx context.Context, out chan<- Frame) error {
		var snapshot []*beehive.Object[Spec, Status]
		if src.Object != nil {
			snapshot = append(snapshot, src.Object)
		}
		return w.pump(ctx, out, snapshot, src.Changes, src.Err)
	})
}

// streamList serves a list watch — every object of a kind, or every one owned by a
// given id. The two differ only in what beehive put in the snapshot.
func (w deltaWatch[Spec, Status, Frame]) streamList(ctx context.Context, src *beehive.ObjectListStream[Spec, Status]) *Stream[Frame] {
	return NewStream(ctx, func(ctx context.Context, out chan<- Frame) error {
		return w.pump(ctx, out, src.Objects, src.Changes, src.Err)
	})
}

// pumpChanges is pump for a source opened AFTER this stream's bookmark went out: its
// snapshot is reported as ordinary Added frames and its own bookmark is not sent, since a
// second one would claim a snapshot boundary the consumer has already been given.
func (w deltaWatch[Spec, Status, Frame]) pumpChanges(
	ctx context.Context,
	out chan<- Frame,
	src *beehive.ObjectListStream[Spec, Status],
) error {
	for _, obj := range src.Objects {
		frame, err := w.frame(DeltaFrameAdded, obj)
		if err != nil {
			return err
		}
		if !sendFrame(ctx, out, frame) {
			return nil
		}
	}
	return w.pumpAfterSnapshot(ctx, out, src.Changes, src.Err)
}

// pump streams a snapshot, the bookmark closing it, then every change above it.
//
// beehive hands the snapshot complete as of a resource version, with changes carrying
// everything above it, so the bookmark lands between the two without holding any frame
// back. srcErr is the upstream's terminal reason, read once the changes run out.
func (w deltaWatch[Spec, Status, Frame]) pump(
	ctx context.Context,
	out chan<- Frame,
	snapshot []*beehive.Object[Spec, Status],
	changes <-chan beehive.ObjectChange[Spec, Status],
	srcErr func() error,
) error {
	for _, obj := range snapshot {
		frame, err := w.frame(DeltaFrameAdded, obj)
		if err != nil {
			return err
		}
		if !sendFrame(ctx, out, frame) {
			return nil
		}
	}
	if !sendFrame(ctx, out, w.bookmark) {
		return nil
	}
	return w.pumpAfterSnapshot(ctx, out, changes, srcErr)
}

// pumpAfterSnapshot streams every change above a snapshot the caller has already sent.
func (w deltaWatch[Spec, Status, Frame]) pumpAfterSnapshot(
	ctx context.Context,
	out chan<- Frame,
	changes <-chan beehive.ObjectChange[Spec, Status],
	srcErr func() error,
) error {
	for change := range changes {
		if change.Type == beehive.Deleted {
			if !sendFrame(ctx, out, w.departed(change)) {
				return nil
			}
			continue
		}

		// Only a removal arrives without an object.
		if change.Object == nil {
			continue
		}
		frame, err := w.frame(deltaFrameType(change), change.Object)
		if err != nil {
			return err
		}
		if !sendFrame(ctx, out, frame) {
			return nil
		}
	}
	return srcErr()
}

// NewStream runs pump on its own goroutine, publishing what it returns as the
// terminal error. Frames closes only after that error is recorded, which is what
// makes "Frames closed" a safe cue to read Err.
//
// pump must select on ctx around every send. The consumer stops draining Frames the
// moment ctx is done, so a send that blocks on the channel alone blocks forever —
// leaking the goroutine and the upstream watch behind it, once per client
// disconnect. ctx is a parameter rather than a closed-over variable so that the
// requirement is visible in every pump's own signature.
//
// Cancellation is an ordinary teardown: return nil for it. A non-nil error is
// reported to the client as the reason its watch died.
//
// Exported because Stream sits in the family interfaces: an implementation outside
// this package — a fake in the resolver tests — has to be able to build one.
func NewStream[T any](ctx context.Context, pump func(ctx context.Context, out chan<- T) error) *Stream[T] {
	ch := make(chan T, 1)
	s := &Stream[T]{Frames: ch}
	go func() {
		defer close(ch)
		if err := pump(ctx, ch); err != nil {
			s.err.Store(&err)
		}
	}()
	return s
}
