// Package mutationqueue is the durable FIFO write queue behind the local-first
// settings path. A local edit applies to prefs immediately and is enqueued here;
// the sync engine drains it to the cloud when online and Acks each entry once
// accepted. Persisted (crash-safe, via internal/atomicjson), so offline edits
// survive a restart and still reach the cloud on reconnect.
//
// Entry ids come from a persisted monotonic counter (not wall-clock/random), so
// the queue is deterministic.
package mutationqueue

import (
	"strconv"
	"sync"

	"github.com/kubetail-org/kstack-app/sidecar/internal/atomicjson"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
)

// Entry is one pending settings patch awaiting delivery to the cloud.
type Entry struct {
	ID    string         `json:"id"`
	Patch prefs.Settings `json:"patch"`
}

// state is the on-disk shape: the pending entries plus the id counter, so a
// reload never reissues an already-used id.
type state struct {
	Seq     int     `json:"seq"`
	Entries []Entry `json:"entries"`
}

// Queue is a durable FIFO of pending settings patches. Safe for concurrent
// use; every mutation is persisted before it returns.
type Queue struct {
	path string
	mu   sync.Mutex
	st   state
}

// New opens (or lazily creates) the queue file at path, loading any
// previously persisted entries.
func New(path string) (*Queue, error) {
	st, err := atomicjson.Load[state](path)
	if err != nil {
		return nil, err
	}
	return &Queue{path: path, st: st}, nil
}

// Enqueue appends a patch to the tail and persists the queue, returning the
// stored Entry (with its assigned id).
func (q *Queue) Enqueue(p prefs.Settings) (Entry, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.st.Seq++
	// Clone so we own the patch: a caller mutating what it passed in can't change
	// the queued payload.
	e := Entry{ID: strconv.Itoa(q.st.Seq), Patch: prefs.Clone(p)}
	q.st.Entries = append(q.st.Entries, e)
	if err := q.save(); err != nil {
		// Roll back so a failed persist doesn't leave the queue ahead of disk.
		q.st.Entries = q.st.Entries[:len(q.st.Entries)-1]
		q.st.Seq--
		return Entry{}, err
	}
	// Return a clone so a caller mutating the returned Patch can't change the
	// queued payload.
	return Entry{ID: e.ID, Patch: prefs.Clone(e.Patch)}, nil
}

// Pending returns a deep copy of the queued entries in FIFO order, so a caller
// can't mutate the queued payload via the returned pointers.
func (q *Queue) Pending() []Entry {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Entry, len(q.st.Entries))
	for i, e := range q.st.Entries {
		out[i] = Entry{ID: e.ID, Patch: prefs.Clone(e.Patch)}
	}
	return out
}

// Ack removes the entry with the given id and persists the removal. An unknown id
// is a no-op (it may already be gone after a crash between push and ack).
func (q *Queue) Ack(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	idx := -1
	for i, e := range q.st.Entries {
		if e.ID == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return nil
	}
	removed := q.st.Entries[idx]
	q.st.Entries = append(q.st.Entries[:idx], q.st.Entries[idx+1:]...)
	if err := q.save(); err != nil {
		// Roll back so a failed persist doesn't drop the entry from Pending() while
		// it's still durably queued on disk.
		q.st.Entries = append(q.st.Entries, Entry{})
		copy(q.st.Entries[idx+1:], q.st.Entries[idx:])
		q.st.Entries[idx] = removed
		return err
	}
	return nil
}

// save persists the current state. Caller holds q.mu.
func (q *Queue) save() error {
	return atomicjson.Save(q.path, q.st)
}
