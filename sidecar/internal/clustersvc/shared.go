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

// Vocabulary every family reuses: the app's services as this package sees them,
// identity, conditions, the kind-agnostic Event and Schedule projections, and the
// delta-watch frame type. Nothing here belongs to one kind — a type only one family
// uses lives in that family's file, however generic its name reads. The projections
// mirror the shared-vocabulary section of graph/schema.graphqls, which they bind to.
package clustersvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/amorey/beehive"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubesync"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
)

// ErrNotFound is the boundary's sentinel for an id that names no tracked record,
// matched with errors.Is. Nothing in graph maps it to errors.ErrRecordNotFound yet, so
// a mutation's failure still reaches the webview as an internal string — see docs/TODO.md.
var ErrNotFound = errors.New("clustersvc: cluster not found")

// ErrDeclaredBySource is the sentinel for deleting a record its source still declares,
// matched with errors.Is. The source decides which records exist, so deleting one it
// still lists would only re-import it under a fresh id — losing the user's toggles to
// the new record's defaults, and every id a client was holding. Stop tracking such a
// cluster at the source, or disable it. Unmapped on the wire, like ErrNotFound above —
// see docs/TODO.md.
var ErrDeclaredBySource = errors.New("clustersvc: cluster is still declared by its source")

// ErrNotConnectable is the sentinel for a record that exists and will not be connected:
// disabled, awaiting deletion, or from a source that carries no credentials. Told apart
// from ErrNotFound because the remedy is the record's own state rather than a bad id — a
// caller offers to enable the cluster instead of reporting it gone. Unmapped on the wire,
// like the two above — see docs/TODO.md.
var ErrNotConnectable = errors.New("clustersvc: cluster cannot be connected")

// --- Process-wide services ---
//
// The app's own services as this package uses them: narrow, so a test hands a
// controller a static config or stands in for a live cluster without the network.
// Together here rather than beside the kind that reads one, because they travel
// together in deps and reach every kind through it.

// kubeconfigService is the one reader of the user's kubeconfig. RESTConfig is the
// connection probe's seam; the reconciles read Get and the trigger subscribes.
type kubeconfigService interface {
	Get() (*api.Config, bool)
	RESTConfig(contextName string) (*rest.Config, string, error)
	Subscribe() kubeconfig.Subscription
}

// kubeconnService is the pool a cluster is talked to over. Acquire names a context and
// claims its connection, which is also what arms the probe behind it; a claim outlives
// the pass that took it, so the caller holds and releases rather than asking per pass.
// RetryAndWait re-runs a context's probes now and returns when the one it asked for has
// finished, so a caller can report the probe's own duration.
type kubeconnService interface {
	Acquire(contextName string) kubeconn.Lease
	RetryAndWait(ctx context.Context, contextName string) error
	Subscribe() kubeconn.Subscription
}

// kubestoreManager is the cache directory as this package reaches it: the teardowns and
// clears the controllers drive, plus what the stats gauge measures. Named for the leaf
// type it stands for, the way every narrow interface here is.
//
// **OpenExisting is the door to a cache's contents** — reading it, or clearing a kind in
// it — and claims the file for as long as the caller holds it. It never creates one: a
// reader reconnecting in the window between a cache being marked for deletion and its
// teardown pass would otherwise resurrect it as an orphan.
//
// Subscribe borrows the change feed of a cache someone already holds open, taking no
// claim, for the gauge — which measures through Stats and needs no file of its own.
// WatchOpen is the read side's other half: a watch that found no file waits on it rather
// than polling, since nothing else would tell it the cache came up.
type kubestoreManager interface {
	OpenExisting(cacheID int64) (*kubestore.Store, bool, error)
	Subscribe(cacheID int64, keys ...string) (kubestore.Subscription, bool)
	WatchOpen(cacheID int64) kubestore.OpenSubscription
	Clear(cacheID int64) error
	Remove(cacheID int64) error
	Stats(ctx context.Context, cacheID int64) (kubestore.Stats, error)
}

// kubesyncService is the seam that fills a cache, as this package reaches it. It speaks
// cache ids, kube-contexts, server UIDs and GVRs; turning one of those into a record is
// this package's business and never reaches back down.
//
// The two arming pairs AND rather than nest: the cache's says whether it syncs at all,
// each kind's says which kinds, and a kind registered against an unarmed cache is held
// rather than refused — so a cache's pass and a kind's may land in either order.
//
// ForgetDiscovery is a pause and ForgetCache is a teardown, and the difference is what
// becomes of the kinds: a pause keeps them registered so a resume is one call, where a
// deleted cache must leave nothing behind to hold them.
//
// **RunWithCacheSyncStopped/RunWithKindSyncStopped are what a clear runs inside.** A clear swaps the
// file under whoever holds it open, and only kubesync can stop the workers writing
// through it; the store work stays here, because a paused cache has no session and its
// file is still there to clear.
type kubesyncService interface {
	TrackDiscovery(cacheID int64, p kubesync.Params)
	ForgetDiscovery(cacheID int64)
	ForgetCache(cacheID int64)
	TrackKind(cacheID int64, k kubestore.Kind)
	ForgetKind(cacheID int64, k kubestore.Kind)
	GetDiscoveryState(cacheID int64) (kubesync.DiscoveryState, bool)
	GetKindState(cacheID int64, k kubestore.Kind) (kubesync.KindState, bool)
	WatchDiscoveryNews() kubesync.DiscoveryNews
	WatchKindNews() kubesync.KindNews
	RunWithCacheSyncStopped(cacheID int64, fn func() error) error
	RunWithKindSyncStopped(cacheID int64, k kubestore.Kind, fn func() error) error
	RestartAll()
}

// --- Identity ---

// ObjectID is the identity of a persisted object — the beehive ObjectID of any
// kind, opaque on the wire (a decimal string) and bound to the one GraphQL
// ObjectID scalar. The per-kind aliases below name the same type purely to
// document which kind an id refers to.
type ObjectID int64

// ClusterID identifies a cluster record: the beehive ObjectID of its Cluster
// object — opaque, source-agnostic, stable for the record's life (changes only on
// an explicit Delete). The source's natural key lives only on the beehive name.
type ClusterID = ObjectID

// ClusterCacheID identifies one ClusterCache record. A cluster can own several,
// so the cache id is not derivable from the cluster id.
type ClusterCacheID = ObjectID

// ClusterCachedKindID identifies one synced kind's ClusterCachedKind object.
type ClusterCachedKindID = ObjectID

// RecordMeta is the per-record metadata every served kind carries, embedded so a new
// beehive metadata field is one edit rather than one per kind. Conditions sit here
// too: beehive keeps them as object rows, not in the status blob, so every kind reads
// them off the object the same way.
//
// The id is ObjectID rather than a per-kind alias — the aliases name the same type, so
// one field serves them all and each kind's doc comment still says what it points at.
//
// Exported only so other packages can build a record literal; nothing outside this one
// fills it from an object (toRecordMeta does that).
type RecordMeta struct {
	ID                  ObjectID
	Generation          int64
	CreatedAt           time.Time
	DeletionRequestedAt *time.Time // beehive's soft-delete tombstone, surfaced as-is
	Conditions          []Condition
}

// toRecordMeta projects the metadata half of any stored object.
func toRecordMeta[Spec, Status any](obj *beehive.Object[Spec, Status]) RecordMeta {
	return RecordMeta{
		ID:                  ObjectID(obj.ID),
		Generation:          obj.Generation,
		CreatedAt:           obj.CreatedAt,
		DeletionRequestedAt: obj.DeletionRequestedAt,
		Conditions:          obj.Conditions,
	}
}

// parseObjectID parses an ObjectID from its decimal-string wire form; a
// malformed value is a client error surfaced through UnmarshalGQL.
func parseObjectID(s string) (ObjectID, error) {
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid object id %q: %w", s, err)
	}
	return ObjectID(n), nil
}

// MarshalGQL writes the ObjectID to the GraphQL ObjectID scalar as a quoted
// decimal string (its wire form).
func (id ObjectID) MarshalGQL(w io.Writer) {
	io.WriteString(w, strconv.Quote(strconv.FormatInt(int64(id), 10)))
}

// UnmarshalGQL parses the GraphQL ObjectID scalar into a typed ObjectID. Accepts
// string, json.Number (JSON-variable number), and int64/int (inline literal).
func (id *ObjectID) UnmarshalGQL(v any) error {
	switch t := v.(type) {
	case string:
		n, err := parseObjectID(t)
		if err != nil {
			return err
		}
		*id = n
	case json.Number:
		n, err := parseObjectID(t.String())
		if err != nil {
			return err
		}
		*id = n
	case int64:
		*id = ObjectID(t)
	case int:
		*id = ObjectID(t)
	default:
		return fmt.Errorf("ObjectID must be a string or integer, got %T", v)
	}
	return nil
}

// ObjectRef points at another record — today always the beehive owner every child
// kind hangs off, which is the join key a client folds on. Kind-agnostic, so one type
// serves the whole ownership chain rather than a differently-named id per kind.
//
// A defined type rather than beehive.ObjectRef, which aliases into an internal package
// gqlgen cannot import. Group is left off: every kind here is in the empty group, so
// the field would always be "".
type ObjectRef struct {
	ID   ObjectID
	Kind string
}

// toOwnerRef reads the owner edge a child record's join key comes from. The read must
// have loaded it (beehive.LoadOwner) — the beehive name embeds the same id, but it is
// a reconcile key rather than a field, and parsing it back would let the two disagree
// the moment either moves. A read that forgot the load is a caller bug, so it errors.
//
// **No edge is the zero ref, not an error.** A live record with no owner is corruption,
// and the client's join drops it — the same outcome as any record whose owner it cannot
// find, and better than failing a whole read over one row. A collected record never
// reaches here at all: its edges went with it (ON DELETE CASCADE) and beehive loads none
// for the removal, so a watch builds that frame without an owner (see cacheDeparture).
func toOwnerRef[Spec, Status any](obj *beehive.Object[Spec, Status]) (ObjectRef, error) {
	owner, ok, err := obj.Owner()
	if err != nil {
		return ObjectRef{}, fmt.Errorf("read %s %d owner: %w", obj.Kind, obj.ID, err)
	}
	if !ok {
		return ObjectRef{}, nil
	}
	return ObjectRef{ID: ObjectID(owner.ID), Kind: owner.Kind}, nil
}

// --- Raw JSON ---

// RawJSON binds the GraphQL `JSON` scalar: already-serialized JSON in a string,
// written verbatim by MarshalGQL. A string (not a map) so the enclosing record stays
// comparable — the delta watches diff frames with ==, which is what makes an in-place
// object edit surface as Modified. Empty value = absent body → null.
type RawJSON string

// MarshalGQL writes the stored JSON bytes verbatim, or `null` for the absent body.
func (r RawJSON) MarshalGQL(w io.Writer) {
	if r == "" {
		io.WriteString(w, "null")
		return
	}
	io.WriteString(w, string(r))
}

// UnmarshalGQL re-serializes a decoded value back to JSON bytes (nil → absent body).
// The schema uses RawJSON as output only; this exists to satisfy gqlgen's scalar
// contract (Marshaler and Unmarshaler both required).
func (r *RawJSON) UnmarshalGQL(v any) error {
	if v == nil {
		*r = ""
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("invalid JSON scalar: %w", err)
	}
	*r = RawJSON(b)
	return nil
}

// --- Conditions ---

// ConditionStatus is a condition's three-valued verdict, Kubernetes-style.
type ConditionStatus = beehive.ConditionStatus

const (
	ConditionTrue    = beehive.ConditionTrue
	ConditionFalse   = beehive.ConditionFalse
	ConditionUnknown = beehive.ConditionUnknown
)

// ConditionType names one independently-tracked aspect of a record's observed
// state. Each type has exactly one writer.
type ConditionType string

const (
	// ConditionConnected reports whether the last connection probe
	// reached the cluster's API server and resolved its identity facts.
	ConditionConnected ConditionType = "Connected"
	// ConditionIdentified reports whether the probe could tell which cluster
	// answered. Separate from Connected, whose subject is whether we reached the
	// server at all: this one is about what these credentials are allowed to read,
	// and it is what gates the cache, since a cache is named for the identity it
	// mirrors.
	ConditionIdentified ConditionType = "Identified"
	// ConditionSynced reports the state of a sync. Nothing writes one today — the seam
	// that fills a cache is being redesigned.
	ConditionSynced ConditionType = "Synced"
)

// Condition reason constants — CamelCase machine-readable explanations for a
// condition's status, Kubernetes-style. Human detail goes in Message.
const (
	// ReasonInactive: no connection is maintained — the record is orphaned,
	// archived, or deactivated.
	ReasonInactive = "Inactive"
	// ReasonConnecting: a probe is owed but none has succeeded or failed yet
	// (a freshly-minted record awaiting its first pass).
	ReasonConnecting = "Connecting"
	// ReasonConnected: the last connection probe succeeded.
	ReasonConnected = "Connected"
	// ReasonProbeFailed: credentials resolved but the dial/identity probe
	// failed.
	ReasonProbeFailed = "ProbeFailed"
	// ReasonUnreachable: the health probe's transport failed outright.
	ReasonUnreachable = "Unreachable"
	// ReasonNoConnection: health cannot be assessed without a live
	// connection this pass.
	ReasonNoConnection = "NoConnection"
	// ReasonIdentified: the probe read the cluster's identity.
	ReasonIdentified = "Identified"
	// ReasonUIDUnreadable: the probe reached the server and was refused the read
	// that names it, so the cluster is usable but cannot be mirrored.
	ReasonUIDUnreadable = "UIDUnreadable"
	// ReasonPaused: nothing is syncing — the record is sync-disabled, deactivated,
	// orphaned, or archived.
	ReasonPaused = "Paused"
)

// Condition aliases beehive's status condition. Conditions are beehive object
// rows (not inside our status blob); the store owns TransitionedAt stamping,
// no-op suppression, and the liveness downgrade.
type Condition = beehive.Condition

// LiveCondition is the sole condition constructor. Every condition here describes
// process-scoped state, so Liveness makes beehive downgrade a previous process's
// write to Unknown until re-confirmed (docs/adr/2026-08-09-liveness-conditions.md).
// Capping the message here — the one place every condition is built — is what keeps
// an unbounded source out of a frame re-serialized to every watcher.
func LiveCondition(t ConditionType, status ConditionStatus, reason, message string) Condition {
	return Condition{
		Type: string(t), Status: status, Reason: reason, Message: TruncateMessage(message), Liveness: true,
	}
}

// FindCondition returns a pointer to the condition of the given type, or nil.
func FindCondition(conds []Condition, t ConditionType) *Condition {
	for i := range conds {
		if conds[i].Type == string(t) {
			return &conds[i]
		}
	}
	return nil
}

// MaxMessageLen caps a persisted message (on a condition or an event run). Both are read
// back on every frame of a whole-fleet watch, and their sources are unbounded: a raw
// client-go error, or a /readyz?verbose=true body, which routinely runs to kilobytes.
const MaxMessageLen = 200

// TruncateMessage caps s at MaxMessageLen bytes, appending an ellipsis when it overflows.
// (Byte-bounded; error strings are effectively ASCII.)
func TruncateMessage(s string) string {
	if len(s) <= MaxMessageLen {
		return s
	}
	return s[:MaxMessageLen] + "…"
}

// Probe outcomes are not stored on ClusterStatus — they ride beehive's event log,
// keeping per-probe chatter off the status watch (a repeated failure bumps a run's
// Count, no status rewrite).
const (
	// ConnectionEventCategory is the connection controller's probe-outcome category.
	ConnectionEventCategory = "connection"
)

// Event is one coalesced run from a beehive object's event timeline, served under
// every kind's events surface. ID is the client's upsert key (a reason can repeat;
// a run id cannot); a repeated same-outcome occurrence bumps Count, and
// [FirstAt, LastAt] is the run's window.
type Event struct {
	ID       ObjectID          `json:"id"`
	Category string            `json:"category"`
	Type     beehive.EventType `json:"type"`
	Reason   string            `json:"reason"`
	Message  string            `json:"message"`
	Count    int               `json:"count"`
	FirstAt  time.Time         `json:"firstAt"`
	LastAt   time.Time         `json:"lastAt"`
}

// Schedule is the kind-agnostic projection of when an object is next due to be
// looked at (a gauge), served live via the schedule watch — a scheduling change
// fires no object WatchList. What does the looking is the kind's own: a cluster's
// is the pool's probe cadence.
type Schedule struct {
	// NextRequeueAt is the next attempt's time, or nil while a probe is in flight or
	// nothing is scheduled.
	NextRequeueAt *time.Time `json:"nextRequeueAt"`
	// Probing reports whether a network probe is in flight, asserted rather than
	// inferred from the countdown so the webview can show a definite "checking
	// now". Merged into this gauge because a probe start/end fires no WatchList.
	Probing bool `json:"probing"`
}

// DeltaFrameType classifies one frame on a delta watch, mirroring a Kubernetes watch
// event. Named for the frame rather than the change because Bookmark is not a change:
// the values are what a frame can BE, and only three of the four carry an entity.
// Added/Modified/Deleted match beehive's strings, so a watch pump converts plainly. A
// defined type rather than an alias of beehive.ChangeType — that aliases into an
// internal package gqlgen can't import — so the GraphQL enum binds straight to it.
type DeltaFrameType string

const (
	DeltaFrameAdded    DeltaFrameType = "Added"
	DeltaFrameModified DeltaFrameType = "Modified"
	DeltaFrameDeleted  DeltaFrameType = "Deleted"
	// DeltaFrameBookmark closes the on-subscribe snapshot: exactly one per stream, after
	// the last snapshot object and before the first live change. It carries no object —
	// the one case for which every frame's entity is a pointer — so a consumer must skip
	// it rather than key on it.
	// See docs/adr/2026-08-09-delta-watch-protocol.md.
	DeltaFrameBookmark DeltaFrameType = "Bookmark"
)

// deltaFrameType classifies a change that is not a removal, for any kind's watch pump.
// The soft-delete mark is one of them: the row is still there, wearing a tombstone the
// record carries through, so it is a Modified like any other field moving.
func deltaFrameType[Spec, Status any](change beehive.ObjectChange[Spec, Status]) DeltaFrameType {
	if change.Type == beehive.Added {
		return DeltaFrameAdded
	}
	return DeltaFrameModified
}

// EventFrameType classifies one frame on an event-timeline watch. Two values where
// DeltaFrameType has four, because an event log is a positioned log rather than a mirrored
// set: beehive delivers the snapshot plus what grows above it and never reports a
// prune, so a run is only ever upserted and there is no Deleted to forward.
type EventFrameType string

const (
	EventFrameRun EventFrameType = "Run"
	// EventFrameBookmark closes the on-subscribe snapshot, exactly once per stream, so
	// a consumer can tell an empty timeline from one still arriving.
	EventFrameBookmark EventFrameType = "Bookmark"
)

// EventWatchFrame is one frame on an event-timeline watch: a run to upsert by
// Event.ID, or the bookmark closing the snapshot, which carries no run.
type EventWatchFrame struct {
	Type  EventFrameType
	Event *Event
}

// TimePtrEqual compares two optional timestamps: both nil is equal, and a present
// pair compares by instant, so two readings of the same stamp never look changed.
func TimePtrEqual(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Equal(*b)
}
