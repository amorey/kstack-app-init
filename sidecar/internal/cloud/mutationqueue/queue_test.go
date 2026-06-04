package mutationqueue_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/mutationqueue"
	"github.com/kubetail-org/kstack-app/sidecar/internal/cloud/prefs"
)

func patch(theme string) prefs.Settings { return prefs.Settings{Theme: &theme} }

// C7: Enqueue then Pending returns entries in FIFO order, each with a
// distinct id.
func TestEnqueuePendingFIFO(t *testing.T) {
	q, err := mutationqueue.New(filepath.Join(t.TempDir(), "queue.json"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e1, err := q.Enqueue(patch("a"))
	if err != nil {
		t.Fatalf("Enqueue a: %v", err)
	}
	e2, err := q.Enqueue(patch("b"))
	if err != nil {
		t.Fatalf("Enqueue b: %v", err)
	}
	if e1.ID == e2.ID {
		t.Fatalf("ids must be distinct, both %q", e1.ID)
	}
	pending := q.Pending()
	if len(pending) != 2 {
		t.Fatalf("Pending len: want 2, got %d", len(pending))
	}
	if *pending[0].Patch.Theme != "a" || *pending[1].Patch.Theme != "b" {
		t.Fatalf("FIFO order broken: %+v", pending)
	}
}

// C8: a queue reloaded from the same path returns the previously
// enqueued entries (durable across restarts).
func TestQueueReloadPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.json")
	q, err := mutationqueue.New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := q.Enqueue(patch("a")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if _, err := q.Enqueue(patch("b")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	reopened, err := mutationqueue.New(path)
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	pending := reopened.Pending()
	if len(pending) != 2 || *pending[0].Patch.Theme != "a" {
		t.Fatalf("reloaded queue: %+v", pending)
	}
}

// C9: Ack removes that entry and persists the removal across a reload.
func TestAckRemovesAndPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.json")
	q, err := mutationqueue.New(path)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e1, _ := q.Enqueue(patch("a"))
	_, _ = q.Enqueue(patch("b"))

	if err := q.Ack(e1.ID); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	pending := q.Pending()
	if len(pending) != 1 || *pending[0].Patch.Theme != "b" {
		t.Fatalf("after Ack: %+v", pending)
	}

	reopened, err := mutationqueue.New(path)
	if err != nil {
		t.Fatalf("reopen New: %v", err)
	}
	if got := reopened.Pending(); len(got) != 1 || *got[0].Patch.Theme != "b" {
		t.Fatalf("reloaded after Ack: %+v", got)
	}
}

// When Ack's persist fails, the entry is restored to the queue so Pending()
// still reflects it (the ack was not durably recorded).
func TestAckRollsBackOnSaveFailure(t *testing.T) {
	dir := t.TempDir()
	q, err := mutationqueue.New(filepath.Join(dir, "queue.json"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e1, _ := q.Enqueue(patch("a"))
	_, _ = q.Enqueue(patch("b"))

	// Replace the queue's directory with a regular file so atomicjson.Save fails
	// during Ack (its os.MkdirAll errors on a non-directory path component).
	// Cross-platform, unlike chmod which doesn't restrict writes on Windows.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll: %v", err)
	}
	if err := os.WriteFile(dir, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if err := q.Ack(e1.ID); err == nil {
		t.Fatal("want error when Ack persist fails, got nil")
	}
	pending := q.Pending()
	if len(pending) != 2 || *pending[0].Patch.Theme != "a" || *pending[1].Patch.Theme != "b" {
		t.Fatalf("entry not restored after failed Ack: %+v", pending)
	}
}

// Ack of an unknown id is a harmless no-op (the entry may already be gone
// after a crash between push and ack).
func TestAckUnknownIDNoop(t *testing.T) {
	q, err := mutationqueue.New(filepath.Join(t.TempDir(), "queue.json"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := q.Enqueue(patch("a")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if err := q.Ack("nope"); err != nil {
		t.Fatalf("Ack unknown: %v", err)
	}
	if len(q.Pending()) != 1 {
		t.Fatalf("Ack unknown should not remove anything")
	}
}

// Enqueue stores a deep copy of the patch: mutating the string the caller
// passed in must not change the queued (to-be-drained) payload.
func TestEnqueueClonesPatch(t *testing.T) {
	q, err := mutationqueue.New(filepath.Join(t.TempDir(), "queue.json"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	theme := "dark"
	if _, err := q.Enqueue(prefs.Settings{Theme: &theme}); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	theme = "mutated-after-enqueue"

	pending := q.Pending()
	if len(pending) != 1 || pending[0].Patch.Theme == nil || *pending[0].Patch.Theme != "dark" {
		t.Fatalf("queue aliased caller's pointer: %+v", pending)
	}
}

// Enqueue returns a clone: mutating the returned entry's patch must not change
// the queued (to-be-drained) payload.
func TestEnqueueReturnsClone(t *testing.T) {
	q, err := mutationqueue.New(filepath.Join(t.TempDir(), "queue.json"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	e, err := q.Enqueue(patch("dark"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	*e.Patch.Theme = "mutated-via-returned-entry"

	pending := q.Pending()
	if len(pending) != 1 || pending[0].Patch.Theme == nil || *pending[0].Patch.Theme != "dark" {
		t.Fatalf("mutating returned entry changed the queue: %+v", pending)
	}
}
