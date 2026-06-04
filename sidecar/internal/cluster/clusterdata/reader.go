// Package clusterdata is the read side of the per-cluster SQLite mirror. It
// turns the universal `objects` table (and the dedicated `events` table) that
// internal/clustersync writes into the typed, list-shaped snapshots the GraphQL
// surface serves — keeping all the SQL out of the resolver layer so graph stays
// a thin delegate-and-map shell.
//
// Every method is nil-tolerant: a Reader built from a nil cache (the sidecar
// ran without --data-dir) returns empty results rather than erroring, mirroring
// the graceful-degradation contract the resolvers rely on.
package clusterdata

import (
	"context"
	"database/sql"
	"strings"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/clustercache"
)

// Reader serves typed snapshots from the cluster cache. Safe for concurrent use
// (the underlying cache/reader pools are).
type Reader struct {
	cache *clustercache.Manager
}

// NewReader wraps a cache. cache may be nil (no --data-dir); methods then
// return empty snapshots.
func NewReader(cache *clustercache.Manager) *Reader {
	return &Reader{cache: cache}
}

// Pod mirrors one cached pod row. Phase is the adapter's status_summary (the
// worst unhealthy container reason, else status.phase) — more useful for a list
// view than raw phase.
type Pod struct {
	ClusterUUID string
	Namespace   string
	Name        string
	UID         string
	Phase       string
	NodeName    string
	UpdatedAt   int
}

// Service mirrors one cached service row.
type Service struct {
	ClusterUUID string
	Namespace   string
	Name        string
	UID         string
	Type        string
	ClusterIP   string
	UpdatedAt   int
}

// Deployment mirrors one cached deployment row. Replicas/ReadyReplicas are the
// spec'd vs ready values materialized by the adapter.
type Deployment struct {
	ClusterUUID   string
	Namespace     string
	Name          string
	UID           string
	Replicas      int
	ReadyReplicas int
	UpdatedAt     int
}

// Node mirrors one cached node row. Ready is true when the node has Ready=True.
type Node struct {
	ClusterUUID string
	Name        string
	UID         string
	Ready       bool
	UpdatedAt   int
}

// Event mirrors one row of the dedicated events table.
type Event struct {
	ClusterUUID       string
	UID               string
	Type              string
	Reason            string
	Message           string
	InvolvedKind      string
	InvolvedNamespace string
	InvolvedName      string
	FirstSeen         int
	LastSeen          int
	Count             int
}

// readerFor returns the reader pool for a cluster that the cache already has
// open, or ok=false when it isn't. It deliberately uses Lookup, NOT Open: a read
// must never open (and thus start syncing) a cluster. So a disabled cluster
// (frozen, closed by the Coordinator) or an absent/unknown one yields an empty
// result rather than silently resuming its sync or creating a stray cache. The
// Coordinator is the sole opener.
func (r *Reader) readerFor(clusterUUID string) (*sql.DB, bool) {
	if r == nil || r.cache == nil {
		return nil, false
	}
	cdb := r.cache.Lookup(clusterUUID)
	if cdb == nil {
		return nil, false
	}
	return cdb.Reader(), true
}

// loadFor is the shared body of every snapshot query: resolve the reader pool
// for an already-open cluster (empty result if it isn't), then run the typed
// loader against it. A free function rather than a method because Go methods
// can't take their own type parameter — same reason watch() is one.
func loadFor[T any](r *Reader, ctx context.Context, clusterUUID string, load func(context.Context, *sql.DB, string) ([]T, error)) ([]T, error) {
	db, ok := r.readerFor(clusterUUID)
	if !ok {
		return []T{}, nil
	}
	return load(ctx, db, clusterUUID)
}

// Pods returns the cluster's pods, ordered by namespace then name.
func (r *Reader) Pods(ctx context.Context, clusterUUID string) ([]Pod, error) {
	return loadFor(r, ctx, clusterUUID, loadPods)
}

// Services returns the cluster's services.
func (r *Reader) Services(ctx context.Context, clusterUUID string) ([]Service, error) {
	return loadFor(r, ctx, clusterUUID, loadServices)
}

// Deployments returns the cluster's deployments.
func (r *Reader) Deployments(ctx context.Context, clusterUUID string) ([]Deployment, error) {
	return loadFor(r, ctx, clusterUUID, loadDeployments)
}

// Nodes returns the cluster's nodes.
func (r *Reader) Nodes(ctx context.Context, clusterUUID string) ([]Node, error) {
	return loadFor(r, ctx, clusterUUID, loadNodes)
}

// Events returns the cluster's most recent events, newest first. limit <= 0
// defaults to 100 and is capped at 500 to keep one query from pulling a busy
// cluster's whole retention window.
func (r *Reader) Events(ctx context.Context, clusterUUID string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	return loadFor(r, ctx, clusterUUID, func(ctx context.Context, db *sql.DB, uuid string) ([]Event, error) {
		return loadEvents(ctx, db, uuid, limit)
	})
}

// --- queries ------------------------------------------------------------
//
// All typed snapshots source rows from the universal `objects` table; the
// per-kind shape is rebuilt by mapping objects.* columns + status_summary onto
// the fields the UI expects. Events have their own table.

func loadPods(ctx context.Context, db *sql.DB, clusterUUID string) ([]Pod, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT uid, namespace, name, status_summary, host, updated_at
		FROM objects
		WHERE kind='Pod' AND api_version='v1'
		ORDER BY namespace, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Pod, 0, 16)
	for rows.Next() {
		p := Pod{ClusterUUID: clusterUUID}
		var status, host sql.NullString
		var updatedAt int64
		if err := rows.Scan(&p.UID, &p.Namespace, &p.Name, &status, &host, &updatedAt); err != nil {
			return nil, err
		}
		p.Phase = status.String
		p.NodeName = host.String
		p.UpdatedAt = int(updatedAt)
		out = append(out, p)
	}
	return out, rows.Err()
}

// loadServices: status_summary is "<type> <clusterIP>" (e.g. "ClusterIP
// 10.0.0.1"); split on the first space to keep the typed surface's shape.
func loadServices(ctx context.Context, db *sql.DB, clusterUUID string) ([]Service, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT uid, namespace, name, status_summary, updated_at
		FROM objects
		WHERE kind='Service' AND api_version='v1'
		ORDER BY namespace, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Service, 0, 16)
	for rows.Next() {
		s := Service{ClusterUUID: clusterUUID}
		var summary sql.NullString
		var updatedAt int64
		if err := rows.Scan(&s.UID, &s.Namespace, &s.Name, &summary, &updatedAt); err != nil {
			return nil, err
		}
		if summary.Valid {
			if sp := strings.IndexByte(summary.String, ' '); sp > 0 {
				s.Type = summary.String[:sp]
				s.ClusterIP = summary.String[sp+1:]
			} else {
				s.Type = summary.String
			}
		}
		s.UpdatedAt = int(updatedAt)
		out = append(out, s)
	}
	return out, rows.Err()
}

func loadDeployments(ctx context.Context, db *sql.DB, clusterUUID string) ([]Deployment, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT uid, namespace, name, ready_count, total_count, updated_at
		FROM objects
		WHERE kind='Deployment' AND api_version='apps/v1'
		ORDER BY namespace, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Deployment, 0, 16)
	for rows.Next() {
		d := Deployment{ClusterUUID: clusterUUID}
		var ready, total sql.NullInt64
		var updatedAt int64
		if err := rows.Scan(&d.UID, &d.Namespace, &d.Name, &ready, &total, &updatedAt); err != nil {
			return nil, err
		}
		d.ReadyReplicas = int(ready.Int64)
		d.Replicas = int(total.Int64)
		d.UpdatedAt = int(updatedAt)
		out = append(out, d)
	}
	return out, rows.Err()
}

func loadNodes(ctx context.Context, db *sql.DB, clusterUUID string) ([]Node, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT uid, name, ready_count, updated_at
		FROM objects
		WHERE kind='Node' AND api_version='v1'
		ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Node, 0, 16)
	for rows.Next() {
		n := Node{ClusterUUID: clusterUUID}
		var ready sql.NullInt64
		var updatedAt int64
		if err := rows.Scan(&n.UID, &n.Name, &ready, &updatedAt); err != nil {
			return nil, err
		}
		n.Ready = ready.Int64 == 1
		n.UpdatedAt = int(updatedAt)
		out = append(out, n)
	}
	return out, rows.Err()
}

// loadEvents returns the most recent events. Ordered by last_seen DESC because
// that's what a troubleshooting view wants; NULL last_seen sorts last via
// SQLite's NULLS LAST default.
func loadEvents(ctx context.Context, db *sql.DB, clusterUUID string, limit int) ([]Event, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT uid, type, reason, message,
		       involved_kind, involved_ns, involved_name,
		       first_seen, last_seen, count
		FROM events
		ORDER BY last_seen DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Event, 0, limit)
	for rows.Next() {
		e := Event{ClusterUUID: clusterUUID}
		var typ, reason, message, ik, ins, iname sql.NullString
		var first, last, count sql.NullInt64
		if err := rows.Scan(&e.UID, &typ, &reason, &message,
			&ik, &ins, &iname, &first, &last, &count); err != nil {
			return nil, err
		}
		e.Type = typ.String
		e.Reason = reason.String
		e.Message = message.String
		e.InvolvedKind = ik.String
		e.InvolvedNamespace = ins.String
		e.InvolvedName = iname.String
		e.FirstSeen = int(first.Int64)
		e.LastSeen = int(last.Int64)
		e.Count = int(count.Int64)
		out = append(out, e)
	}
	return out, rows.Err()
}
