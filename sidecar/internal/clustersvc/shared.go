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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/amorey/beehive"
	"github.com/amorey/gobus/conflate"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeidentity"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconn"
)

// ErrNotFound is the boundary's sentinel for an id that names no tracked record,
// matched with errors.Is. Nothing in graph maps it to errors.ErrRecordNotFound yet, so
// a mutation's failure still reaches the webview as an internal string — see TODO.md.
var ErrNotFound = errors.New("clustersvc: cluster not found")

// ErrDeclaredBySource is the sentinel for deleting a record its source still declares,
// matched with errors.Is. The source decides which records exist, so deleting one it
// still lists would only re-import it under a fresh id — losing the user's toggles to
// the new record's defaults, and every id a client was holding. Stop tracking such a
// cluster at the source, or disable it. Unmapped on the wire, like ErrNotFound above —
// see TODO.md.
var ErrDeclaredBySource = errors.New("clustersvc: cluster is still declared by its source")

// --- Process-wide services ---
//
// The app's own services as this package uses them: narrow, so a test hands a
// controller a static config or stands in for a live cluster without the network.
// Together here rather than beside the kind that reads one, because they travel
// together in deps and reach every kind through it.

// kubeconfigService is the one reader of the user's kubeconfig. RESTConfig is the
// connection probe's seam; the reconciles read Get and the notifier subscribes.
type kubeconfigService interface {
	Get() (*api.Config, bool)
	RESTConfig(contextName string) (*rest.Config, string, error)
	Subscribe() kubeconfig.Subscription
}

// kubeidentityService answers which server a kube-context reaches, from a cache its own
// workers keep fresh. Read-only and non-blocking by contract: a pass reports a cluster's
// identity without the dial entering the pass.
type kubeidentityService interface {
	// Get is what the last probe learned, and whether anything is known at all. Asking
	// is also what keeps the context probed — see the package.
	Get(contextName string) (kubeidentity.State, bool)
	// Subscribe reports the contexts whose answer moved.
	Subscribe() kubeidentity.Subscription
}

// kubeconnService is the connection pool: a claim on one set of credentials, an
// out-of-band probe, and the news either produces.
//
// Everything about pacing — the cadence a held claim re-probes on, the backoff a
// failing one follows, how many probe at once — belongs to that package. What this one
// decides is which clusters are worth connecting, which is the whole of its half.
type kubeconnService interface {
	// Acquire claims the connection for these credentials and arms their cadence.
	Acquire(cfg *rest.Config, key string) (kubeconn.Lease, error)
	// ProbeNow asks for one probe and does not wait for the outcome.
	ProbeNow(cfg *rest.Config, key string) error
	// State is what the pool knows about these credentials: the last outcome, and
	// whether a probe is asked for and unanswered.
	State(key string) kubeconn.State
	// Subscribe reports that these credentials moved, so State is worth re-reading.
	// Close the receiver when done.
	Subscribe(key string) *conflate.Receiver[string, struct{}]
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

// ClusterCachedCatalogID identifies one cache's GVR-discovery child.
type ClusterCachedCatalogID = ObjectID

// ClusterCachedResourceID identifies one synced kind's ClusterCachedResource object.
type ClusterCachedResourceID = ObjectID

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
	// ConditionHealthy reports the API server's own condition (its
	// readiness checks), as distinct from our ability to reach it.
	ConditionHealthy ConditionType = "Healthy"
	// ConditionSynced reports the state of a sync. It is reported at two levels: coarse
	// on the ClusterCache (did this cache decide to sync?) and per kind on each
	// ClusterCachedResource, which is the verdict a UI wants.
	ConditionSynced ConditionType = "Synced"
	// ConditionDiscovered reports whether the cache's GVR discovery pass reached the
	// API server and enumerated the kinds it serves. A separate axis from Synced: a
	// cache can have a complete, current kind list while its per-kind workers are
	// still catching up, and a discovery outage says nothing about workers already
	// running.
	ConditionDiscovered ConditionType = "Discovered"
)

// Condition reason constants — CamelCase machine-readable explanations for a
// condition's status, Kubernetes-style. Human detail goes in Message.
const (
	// ReasonInactive: no connection is maintained — the record is orphaned,
	// archived, deactivated, or its source has no resolvable credentials.
	ReasonInactive = "Inactive"
	// ReasonConnecting: a probe is owed but none has succeeded or failed yet
	// (a freshly-minted record awaiting its first pass).
	ReasonConnecting = "Connecting"
	// ReasonConnected: the last connection probe succeeded.
	ReasonConnected = "Connected"
	// ReasonResolveFailed: credentials could not be resolved from the
	// record's source (e.g. the kube-context vanished from the kubeconfig).
	ReasonResolveFailed = "ResolveFailed"
	// ReasonProbeFailed: credentials resolved but the dial/identity probe
	// failed.
	ReasonProbeFailed = "ProbeFailed"
	// ReasonReady: the API server reports its readiness checks passing.
	ReasonReady = "Ready"
	// ReasonReadyzFailed: the API server responded but named failing checks.
	ReasonReadyzFailed = "ReadyzFailed"
	// ReasonUnreachable: the health probe's transport failed outright.
	ReasonUnreachable = "Unreachable"
	// ReasonNoConnection: health cannot be assessed without a live
	// connection this pass.
	ReasonNoConnection = "NoConnection"
	// ReasonPaused: nothing is syncing — the record is sync-disabled, deactivated,
	// orphaned, or archived.
	ReasonPaused = "Paused"
	// ReasonSyncing: the sync is starting or catching up. Condition-only — the
	// event vocabulary uses SyncStart/ResyncStart instead.
	ReasonSyncing = "Syncing"
	// ReasonWatching: the watch is established and proven live — caught up and
	// streaming deltas.
	ReasonWatching = "Watching"
	// ReasonSyncFailed: the worker itself failed (it could not start, or its run loop
	// exited) and is retrying with backoff.
	ReasonSyncFailed = "SyncFailed"
	// ReasonDiscovered: the last discovery pass enumerated every group the API
	// server serves, and the per-GVR sync children match it.
	ReasonDiscovered = "Discovered"
	// ReasonDiscoveryPartial: some groups answered, others didn't — the pass adds
	// children without pruning (a group that failed to answer is not shown gone).
	ReasonDiscoveryPartial = "DiscoveryPartial"
	// ReasonDiscoveryFailed: the discovery request itself failed, so nothing is
	// known about the served kinds this pass. The existing children are left alone.
	ReasonDiscoveryFailed = "DiscoveryFailed"
	// ReasonDiscoveryDraining: the kind list is current but a still-served kind has
	// no live child yet — an earlier prune's child is draining and holds its name.
	ReasonDiscoveryDraining = "DiscoveryDraining"
	// ReasonStale: caught up, but the watch stopped proving itself alive past the
	// threshold — the cache may be behind (a Synced=False state distinct from
	// SyncFailed, which is a hard worker failure).
	ReasonStale = "Stale"

	// Sync-EVENT reasons (the event log's transition vocabulary, distinct from the
	// Synced-condition reasons above): start/complete pairs, cold and warm. A
	// healthy steady state records no event.
	//
	// ReasonSyncStart: a cold cache began its first-ever build.
	ReasonSyncStart = "SyncStart"
	// ReasonSyncComplete: a cold build reached the caught-up milestone.
	ReasonSyncComplete = "SyncComplete"
	// ReasonResyncStart: an already-populated cache began resuming (poke,
	// reconnect, credential restart) — its message reports the warm cache size.
	ReasonResyncStart = "ResyncStart"
	// ReasonResyncComplete: a resume re-reached the caught-up milestone; its
	// message disambiguates a real catch-up (counts) from a bare liveness
	// recovery (no counts).
	ReasonResyncComplete = "ResyncComplete"
	// ReasonSyncDegraded: the worker failed and is retrying with backoff. The
	// event-log parallel of the SyncFailed condition reason.
	ReasonSyncDegraded = "SyncDegraded"
	// ReasonSyncStopped: the cache's syncs were stopped because the cluster became
	// sync-ineligible (sync paused/disabled, or the context departed).
	ReasonSyncStopped = "SyncStopped"
	// ReasonSyncStale: a caught-up watch stopped delivering updates past the
	// threshold — the event-log parallel of the Stale condition reason.
	ReasonSyncStale = "SyncStale"
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
	// SyncEventCategory is the sync-transition category, the sync-side parallel of
	// ConnectionEventCategory.
	SyncEventCategory = "sync"
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

// Schedule is the kind-agnostic projection of a beehive object's reconcile
// schedule (a gauge), served live via the schedule watch — a scheduling change
// fires no object WatchList.
type Schedule struct {
	// NextRequeueAt is the next reconcile time, or nil when nothing is scheduled.
	NextRequeueAt *time.Time `json:"nextRequeueAt"`
	// Probing reports whether a reconcile's network probe is in flight, asserted
	// by the core controller so the webview can show a definite "checking now".
	// Merged into this gauge because a probe start/end fires no WatchList.
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
