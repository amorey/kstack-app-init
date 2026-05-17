// Package mutationqueue is the durable offline-write buffer for
// updateSettings. When the cloud write fails (offline/transient) the
// mutation is persisted here; the always-on engine drains it on its next
// successful (re)connect, so an edit made offline still reaches the cloud
// — surviving app restarts via the same atomicjson file mechanics as the
// rest of the local state.
//
// Settings is a single deep-merged field, so the queue coalesces to the
// latest pending input (last write wins) rather than keeping a FIFO.
//
// The disk file is the durable source of truth, but three in-memory
// atomics keep the hot path off disk and make concurrent drains safe:
//   - pending: gates Drain so the dominant "nothing queued" path (every
//     reconnect, no offline edit) never touches disk;
//   - seq:     monotonic within the process; guards Drain's
//     read→push→clear so a write that races the push isn't cleared away;
//   - draining: single-flight, so overlapping engine reconnects can't
//     double-push the same mutation.
package mutationqueue

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/kubetail-org/kstack-app/sidecar/internal/atomicjson"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud"
)

type queued struct {
	Input   cloud.UpdateInput `json:"input"`
	Pending bool              `json:"pending"`
}

// Queue is the single-slot, coalescing, durable mutation buffer. Safe for
// concurrent use: the resolver Enqueues while the engine Drains, and
// overlapping Drains are serialized to a single push.
type Queue struct {
	path string
	mu   sync.Mutex // serializes Enqueue's write against Drain's load/clear

	pending  atomic.Bool
	seq      atomic.Uint64
	draining atomic.Bool
}

// New returns a Queue backed by the JSON file at path. It does one disk
// read so a mutation persisted before a restart is still drained;
// thereafter `pending` is maintained in memory.
func New(path string) *Queue {
	q := &Queue{path: path}
	if cur, err := atomicjson.Load[queued](path); err == nil && cur.Pending {
		q.pending.Store(true)
	}
	return q
}

// Enqueue records in as the latest pending mutation, replacing any prior
// one (coalesce).
func (q *Queue) Enqueue(in cloud.UpdateInput) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.seq.Add(1)
	if err := atomicjson.Save(q.path, queued{Input: in, Pending: true}); err != nil {
		return err
	}
	q.pending.Store(true)
	return nil
}

// Pending reports whether a mutation is waiting to be drained. (The error
// return is retained for API symmetry; it is always nil now that the flag
// is in memory.)
func (q *Queue) Pending() (bool, error) {
	return q.pending.Load(), nil
}

// Drain pushes the pending mutation (if any) via push and clears the slot
// on success — unless a newer Enqueue raced the push (seq changed), in
// which case it's left for the next round. A push error is returned and
// leaves the entry pending. Only one Drain runs the push-cycle at a time;
// a concurrent Drain returns immediately. The network call runs without
// the lock held.
func (q *Queue) Drain(ctx context.Context, push func(context.Context, cloud.UpdateInput) error) error {
	if !q.pending.Load() {
		return nil
	}
	if !q.draining.CompareAndSwap(false, true) {
		return nil
	}
	defer q.draining.Store(false)

	q.mu.Lock()
	cur, err := atomicjson.Load[queued](q.path)
	seqAtLoad := q.seq.Load()
	q.mu.Unlock()
	if err != nil {
		return err
	}
	if !cur.Pending {
		q.pending.Store(false)
		return nil
	}

	if err := push(ctx, cur.Input); err != nil {
		return err
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if q.seq.Load() != seqAtLoad {
		return nil
	}
	if err := atomicjson.Save(q.path, queued{Pending: false}); err != nil {
		return err
	}
	q.pending.Store(false)
	return nil
}
