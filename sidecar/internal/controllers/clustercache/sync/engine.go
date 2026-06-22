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

// Package clustersync mirrors one real Kubernetes cluster into its local
// SQLite cache (internal/kube/clustercache).
//
// Discovery + dynamic/metadata clients + one per-GVR kindDriver means one
// code path serves every Kind on every cluster — built-ins and CRDs alike.
// The Engine walks /apis discovery, picks every list/watchable resource,
// and spins up one driver per (group, version, resource) feeding the
// SQLite-backed stores. Events get their own store (and table) because their
// access pattern differs; everything else lands in the universal `objects`
// table.
//
// Each driver (driver.go) is built instead of a raw client-go Reflector so a
// wake can resume cheaply: it seeds a RetryWatcher from the kind's persisted
// resourceVersion (resume — apply deltas, no LIST) and only on a 410 (RV too
// old) or a cold cache falls back to a metadata-first full re-sync (list
// metadata, fetch bodies for just the changed objects). The stock Reflector
// can't be seeded with a stored RV, so it always re-LISTs every body.
//
// One Engine runs per synced cluster, owned by the kube package's sync
// controller: the controller decides when an engine starts, stops, or
// restarts (spec changes, credential rotation, resync pokes); the engine
// reports its coarse state back through the Sink it was constructed with.
package cachesync

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers/clustercache/store"
)

// init routes client-go's API-server warning headers (deprecation
// notices, etc.) through slog at Debug level instead of stderr. The
// default handler logs them as INFO/WARN, which makes the app logs
// noisy whenever the cluster has any deprecated API in use — the
// signal-to-noise ratio is poor because the warnings are about the
// cluster's contents, not anything the sidecar can act on.
func init() {
	rest.SetDefaultWarningHandler(slogWarningHandler{})
}

type slogWarningHandler struct{}

func (slogWarningHandler) HandleWarningHeader(code int, agent, message string) {
	if message == "" {
		return
	}
	slog.Debug("k8s API warning", "code", code, "agent", agent, "message", message)
}

// EngineState is the engine's coarse phase, reported through the Sink.
type EngineState string

const (
	// EngineSyncing means the engine is starting or catching the cache up:
	// discovery walk, or at least one driver still pre-first-watch.
	EngineSyncing EngineState = "Syncing"
	// EngineWatching means every driver has entered its watch phase at least
	// once — the cache is caught up and streaming deltas.
	EngineWatching EngineState = "Watching"
	// EngineErrored means the engine hit an engine-level failure (discovery,
	// client construction) and is retrying with backoff.
	EngineErrored EngineState = "Errored"
)

// EngineStatus is one coherent snapshot of the engine's reportable state.
// Per-driver errors are deliberately not folded in — they're logged; the
// state is a coarse human-facing label.
type EngineStatus struct {
	State EngineState
	// LastError is the engine-level failure message; non-empty only for
	// EngineErrored.
	LastError string
	// LastSyncedAt is when the cache last received fresh data; nil if not
	// yet this engine run. Flushed coarsely (~30s), not per write.
	LastSyncedAt *time.Time
}

// Sink receives engine status snapshots; implemented by the kube package's
// sync controller, which folds them onto the cluster record. Report must be
// safe for concurrent use (the run loop and freshness loop both report).
type Sink interface {
	Report(s EngineStatus)
}

// Engine mirrors one cluster into its cache for as long as it runs: a
// supervision loop around discovery + per-GVR drivers, plus a freshness
// tracker. Construct with NewEngine, then Start once; Stop tears it down.
type Engine struct {
	cfg  *rest.Config
	cdb  *store.ClusterDB
	sink Sink

	// status is the engine's current snapshot; reports merge into it under
	// mu so the two reporting goroutines (run loop, freshness loop) always
	// hand the sink a coherent EngineStatus.
	mu     sync.Mutex
	status EngineStatus

	backoffInit   time.Duration
	backoffMax    time.Duration
	flushInterval time.Duration
	// sleep/now are test seams (deterministic backoff and freshness stamps).
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time

	baseCtx       context.Context
	baseCtxCancel context.CancelFunc
	wg            sync.WaitGroup
}

// Engine retry backoff: if a run exits while the engine is still wanted (a
// startup error, or discovery transiently yielding nothing), the engine
// retries itself — the sync controller deliberately doesn't watch for engine
// death, only for record changes. Capped exponential, reset on a run that
// reaches its drivers.
const (
	engineBackoffInit = 1 * time.Second
	engineBackoffMax  = 30 * time.Second

	// freshnessFlushInterval is how often the in-memory "last received data"
	// timestamp is flushed through the sink. Coarse on purpose — the value
	// backs a human-facing "synced N ago" label, not precise accounting.
	freshnessFlushInterval = 30 * time.Second
)

// engineOption configures an Engine's test seams. Production never tunes
// them, so NewEngine exposes none — the unexported-option pattern shared
// with the driver and internal/cloud/prefsync.
type engineOption func(*Engine)

func withEngineSleep(fn func(context.Context, time.Duration) error) engineOption {
	return func(e *Engine) { e.sleep = fn }
}

func withFlushInterval(d time.Duration) engineOption {
	return func(e *Engine) { e.flushInterval = d }
}

func withEngineNow(fn func() time.Time) engineOption {
	return func(e *Engine) { e.now = fn }
}

// NewEngine builds an engine over resolved credentials, an open cluster
// cache, and the status sink. It starts nothing; call Start.
func NewEngine(cfg *rest.Config, cdb *store.ClusterDB, sink Sink) *Engine {
	return newEngineWithOptions(cfg, cdb, sink)
}

func newEngineWithOptions(cfg *rest.Config, cdb *store.ClusterDB, sink Sink, opts ...engineOption) *Engine {
	baseCtx, cancel := context.WithCancel(context.Background())
	e := &Engine{
		cfg:           cfg,
		cdb:           cdb,
		sink:          sink,
		backoffInit:   engineBackoffInit,
		backoffMax:    engineBackoffMax,
		flushInterval: freshnessFlushInterval,
		sleep:         ctxSleep,
		now:           time.Now,
		baseCtx:       baseCtx,
		baseCtxCancel: cancel,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Start brings the engine online: the supervised run loop and the freshness
// tracker. The freshness subscription is taken synchronously so no write ping
// from the run loop's drivers can slip past it. Call once.
func (e *Engine) Start() {
	pings, cancelSub := e.cdb.Subscribe()
	e.wg.Go(func() { e.runLoop(e.baseCtx) })
	e.wg.Go(func() {
		defer cancelSub()
		e.freshnessLoop(e.baseCtx, pings)
	})
}

// Stop tears the engine down: cancels both loops and waits for them to join,
// bounded by ctx (a stuck driver mid-SQL write joins as soon as its statement
// returns). Returns ctx.Err() if the deadline expires first.
func (e *Engine) Stop(ctx context.Context) error {
	e.baseCtxCancel()
	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// report merges a partial update into the engine's status under mu and
// forwards the full snapshot to the sink.
func (e *Engine) report(update func(*EngineStatus)) {
	e.mu.Lock()
	update(&e.status)
	snapshot := e.status
	e.mu.Unlock()
	e.sink.Report(snapshot)
}

// runLoop supervises run: any return while the engine is still wanted (a
// startup error, or discovery transiently yielding nothing) is abnormal —
// report Errored and retry with capped backoff until Stop.
func (e *Engine) runLoop(ctx context.Context) {
	backoff := e.backoffInit
	for {
		e.report(func(s *EngineStatus) { s.State, s.LastError = EngineSyncing, "" })
		err := e.run(ctx)
		if ctx.Err() != nil {
			return
		}
		msg := "sync engine exited unexpectedly"
		if err != nil {
			msg = err.Error()
		}
		slog.Error("clustersync: engine run exited, retrying",
			"id", e.cdb.ID(), "err", err, "backoff", backoff)
		e.report(func(s *EngineStatus) { s.State, s.LastError = EngineErrored, msg })
		if e.sleep(ctx, backoff) != nil {
			return
		}
		if backoff *= 2; backoff > e.backoffMax {
			backoff = e.backoffMax
		}
	}
}

// run blocks for one engine generation: build clients, walk discovery, and
// drive one kindDriver per discovered GVR until ctx is cancelled. The state
// flips to Watching once every driver has entered its watch phase.
func (e *Engine) run(ctx context.Context) error {
	dc, err := discovery.NewDiscoveryClientForConfig(e.cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(e.cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}
	md, err := metadata.NewForConfig(e.cfg)
	if err != nil {
		return fmt.Errorf("metadata client: %w", err)
	}

	writer := e.cdb.Writer()
	entries, err := discoverGVRs(ctx, dc, writer)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	if len(entries) == 0 {
		// Zero drivers would make run block on nothing and report Watching
		// for a cluster that mirrors no data; treat as a transient failure.
		return errors.New("discovery returned no syncable resources")
	}
	slog.Info("clustersync: discovered syncable GVRs on API server", "id", e.cdb.ID(), "count", len(entries))

	var pending atomic.Int64
	pending.Store(int64(len(entries)))

	var wg sync.WaitGroup
	for _, entry := range entries {
		var store kindStore
		if isEventGVK(entry.GVK) {
			store = newEventsStore(ctx, e.cdb.ID(), entry.GVK, writer, e.cdb)
		} else {
			store = newObjectsStore(ctx, e.cdb.ID(), entry.GVK, writer, e.cdb)
		}
		// Seed the driver from the kind's persisted resourceVersion so it resumes
		// the watch instead of re-LISTing every body; "" (never synced, or a read
		// error) just means start with a full re-sync.
		seedRV, err := readLastListRV(ctx, writer, entry.GVK)
		if err != nil {
			slog.Warn("clustersync: read resume rv", "gvk", entry.GVK.String(), "err", err)
			seedRV = ""
		}
		d := newKindDriver(newLiveSource(dyn, md, entry), store, entry.GVK, seedRV)
		d.onWatch = func() {
			if pending.Add(-1) == 0 {
				e.report(func(s *EngineStatus) { s.State, s.LastError = EngineWatching, "" })
			}
		}
		slog.Debug("clustersync: starting driver", "id", e.cdb.ID(), "gvk", entry.GVK.String(), "seedRV", seedRV)
		wg.Go(func() {
			if err := d.Run(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("clustersync: driver exited", "id", e.cdb.ID(), "gvk", entry.GVK.String(), "err", err)
			}
		})
	}
	wg.Wait()
	return ctx.Err()
}

// freshnessLoop tracks when the cache last received data (via the ClusterDB's
// write pings) and flushes the timestamp through the sink on a coarse cadence
// — per-write reporting would turn every watch delta into a store write.
func (e *Engine) freshnessLoop(ctx context.Context, pings <-chan struct{}) {
	ticker := time.NewTicker(e.flushInterval)
	defer ticker.Stop()

	var last time.Time
	dirty := false
	flush := func() {
		if !dirty {
			return
		}
		dirty = false
		at := last
		e.report(func(s *EngineStatus) { s.LastSyncedAt = &at })
	}
	for {
		select {
		case <-ctx.Done():
			// A ping delivered just before shutdown may still sit in the
			// subscription's slot; drain it so the final flush carries it.
			select {
			case _, ok := <-pings:
				if ok {
					last = e.now().UTC()
					dirty = true
				}
			default:
			}
			flush() // the final stamp must not be lost to the 30s cadence
			return
		case _, ok := <-pings:
			if !ok {
				flush()
				return
			}
			last = e.now().UTC()
			dirty = true
		case <-ticker.C:
			flush()
		}
	}
}

// --- credential fingerprinting (the sync controller's restart trigger) ---

// ConfigFingerprint hashes the connection- and credential-relevant fields of a
// rest.Config. Two configs with the same fingerprint connect and authenticate
// identically, so the cluster's running drivers don't need restarting; a
// different fingerprint (rotated token, new client cert, changed CA/server URL,
// or an edited exec/auth-provider/impersonation block) means the open engine is
// running on stale config and must be restarted. We hash the *static* exec/
// auth-provider config (command/args/env/plugin settings) — runtime token
// minting is the transport's job, but editing how tokens are obtained must
// invalidate the fingerprint.
//
// proxyURL is the kubeconfig cluster's proxy-url. clientcmd compiles it into
// rest.Config.Proxy (an opaque func we can't hash), so the caller passes the raw
// string (ContextProxyURL): a changed proxy routes the connection differently
// and must restart the drivers, even when every other field is identical.
func ConfigFingerprint(cfg *rest.Config, proxyURL string) string {
	if cfg == nil {
		return ""
	}
	h := sha256.New()
	// NUL-separate every field so boundaries can't be aliased by concatenation.
	write := func(s string) { h.Write([]byte(s)); h.Write([]byte{0}) }
	writeBytes := func(b []byte) { h.Write(b); h.Write([]byte{0}) }

	write(proxyURL)

	t := cfg.TLSClientConfig
	for _, s := range []string{
		cfg.Host, cfg.APIPath, cfg.Username, cfg.Password,
		cfg.BearerToken, cfg.BearerTokenFile,
		t.ServerName, t.CAFile, t.CertFile, t.KeyFile,
		strconv.FormatBool(t.Insecure),
	} {
		write(s)
	}
	writeBytes(t.CAData)
	writeBytes(t.CertData)
	writeBytes(t.KeyData)

	// Impersonation.
	im := cfg.Impersonate
	write(im.UserName)
	write(im.UID)
	for _, g := range im.Groups {
		write(g)
	}
	for _, k := range sortedKeys(im.Extra) {
		write(k)
		for _, v := range im.Extra[k] {
			write(v)
		}
	}

	// Auth-provider plugin (name + static config).
	if ap := cfg.AuthProvider; ap != nil {
		write(ap.Name)
		for _, k := range sortedKeys(ap.Config) {
			write(k)
			write(ap.Config[k])
		}
	}

	// Exec credential plugin (command/args/env/apiVersion).
	if ep := cfg.ExecProvider; ep != nil {
		write(ep.Command)
		write(ep.APIVersion)
		for _, a := range ep.Args {
			write(a)
		}
		for _, e := range ep.Env {
			write(e.Name)
			write(e.Value)
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// ContextProxyURL returns the proxy-url of the cluster a kubeconfig context
// points at, or "" if the context, its cluster, or the field is absent. The
// sync controller folds it into the config fingerprint because clientcmd
// compiles it into rest.Config.Proxy, an opaque func the fingerprint can't
// otherwise see.
func ContextProxyURL(cfg *api.Config, ctxName string) string {
	ctx, ok := cfg.Contexts[ctxName]
	if !ok || ctx == nil {
		return ""
	}
	cluster, ok := cfg.Clusters[ctx.Cluster]
	if !ok || cluster == nil {
		return ""
	}
	return cluster.ProxyURL
}

// sortedKeys returns a map's keys in deterministic order, so hashing a map
// doesn't depend on Go's randomized iteration order.
func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- discovery ------------------------------------------------------------

// gvrEntry packages everything needed to start one driver.
type gvrEntry struct {
	GVR        schema.GroupVersionResource
	GVK        schema.GroupVersionKind
	Namespaced bool
	IsCRD      bool
}

// Group prefixes whose resources we never want to mirror. They expose
// query APIs (metrics) or replicate state we already capture elsewhere
// (events.k8s.io duplicates core/v1 Event semantics; we keep core/v1
// only to avoid double-counting).
var skipGroups = map[string]bool{
	"metrics.k8s.io":          true,
	"external.metrics.k8s.io": true,
	"custom.metrics.k8s.io":   true,
	"events.k8s.io":           true,
}

// Specific (group, resource) entries to skip even though they pass the
// generic filters. Right now: v1 Endpoints — deprecated in k8s 1.33+ in
// favor of discovery.k8s.io/v1 EndpointSlice, which we already mirror.
// The two resources hold the same data; keeping both wastes a watch and
// makes the API server emit a deprecation warning on every LIST.
var skipResources = map[string]map[string]bool{
	"": {"endpoints": true}, // core/v1 endpoints
}

func isEventGVK(g schema.GroupVersionKind) bool {
	return g.Kind == "Event" && (g.Group == "" || g.Group == "events.k8s.io")
}

// discoverGVRs walks /apis, returns one entry per list/watchable
// resource (preferred version only), and populates kind_catalog so the
// agent (and UI) can ask "what kinds exist on this cluster?" without
// re-doing discovery.
//
// We use ServerPreferredResources rather than ServerGroupsAndResources
// because the latter returns every version of every resource, which
// means we'd start one driver per (resource, version) — duplicating
// watches against the same underlying data and getting deprecation
// warnings on every alpha/beta version we accidentally watched.
// Preferred-only gives us one driver per logical resource.
func discoverGVRs(ctx context.Context, dc discovery.DiscoveryInterface, writer *sql.DB) ([]gvrEntry, error) {
	complete := true
	lists, err := dc.ServerPreferredResources()
	if err != nil {
		if len(lists) == 0 {
			// Nothing usable came back (e.g. a transient discovery
			// failure or unreachable API server). Returning zero entries
			// here would make run start no drivers and report success,
			// leaving an open cluster that never mirrors data until the
			// next reconcile. Fail so the sync surfaces the error.
			return nil, fmt.Errorf("discovery returned no resources: %w", err)
		}
		// Partial discovery errors are common when an aggregated API
		// server is down; the returned lists are still usable. Log and
		// continue rather than fail the whole cluster — but mark discovery
		// incomplete so we don't prune objects for a kind that's merely in a
		// transiently-unavailable group (see the prune below).
		complete = false
		slog.Warn("clustersync: partial discovery", "err", err)
	}

	// Pull the CRD list once so kind_catalog can record CRD schemas.
	crds, _ := listCRDs(ctx, dc)

	var out []gvrEntry
	type catalogRow struct {
		apiVersion, kind, resource, scope string
		isCRD                             bool
		schemaJSON                        string
	}
	var catalog []catalogRow

	for _, list := range lists {
		if list == nil {
			continue
		}
		gv, err := schema.ParseGroupVersion(list.GroupVersion)
		if err != nil {
			continue
		}
		if skipGroups[gv.Group] {
			continue
		}
		for _, r := range list.APIResources {
			// Skip subresources (pods/status, pods/exec, ...).
			if strings.Contains(r.Name, "/") {
				continue
			}
			if !hasVerb(r.Verbs, "list") || !hasVerb(r.Verbs, "watch") {
				continue
			}
			if skipResources[gv.Group][r.Name] {
				continue
			}
			gvk := schema.GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: r.Kind}
			gvr := gv.WithResource(r.Name)
			isCRD := isCRDKind(crds, gvk)
			out = append(out, gvrEntry{GVR: gvr, GVK: gvk, Namespaced: r.Namespaced, IsCRD: isCRD})

			scope := "Cluster"
			if r.Namespaced {
				scope = "Namespaced"
			}
			schemaJSON := ""
			if isCRD {
				schemaJSON = crdSchemaJSON(crds, gvk)
			}
			catalog = append(catalog, catalogRow{
				apiVersion: gv.String(), kind: r.Kind, resource: r.Name,
				scope: scope, isCRD: isCRD, schemaJSON: schemaJSON,
			})
		}
	}

	// Persist the kind catalog. Truncate-and-rewrite is correct because
	// CRDs can be installed/uninstalled between sidecar runs and we want
	// the catalog to reflect "what exists right now".
	tx, err := writer.BeginTx(ctx, nil)
	if err != nil {
		return out, err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.ExecContext(ctx, `DELETE FROM kind_catalog`); err != nil {
		return out, err
	}
	for _, c := range catalog {
		isCRDInt := 0
		if c.isCRD {
			isCRDInt = 1
		}
		var schemaArg any
		if c.schemaJSON != "" {
			schemaArg = c.schemaJSON
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT OR REPLACE INTO kind_catalog(api_version, kind, resource, scope, is_crd, schema_json)
			 VALUES(?, ?, ?, ?, ?, ?)`,
			c.apiVersion, c.kind, c.resource, c.scope, isCRDInt, schemaArg); err != nil {
			return out, err
		}
	}
	if err := tx.Commit(); err != nil {
		return out, err
	}

	// Evict objects whose kind no longer exists on the cluster (e.g. an
	// uninstalled CRD). Only safe after a complete discovery — a partial
	// result omits kinds that are still live. Build the keep-set from the
	// same catalog we just persisted so the two stay consistent.
	if complete {
		keep := make(map[kindKey]struct{}, len(catalog))
		for _, c := range catalog {
			keep[kindKey{kind: c.kind, apiVersion: c.apiVersion}] = struct{}{}
		}
		if n, err := pruneOrphanedKinds(ctx, writer, keep); err != nil {
			// Non-fatal: a failed prune just leaves stale rows for the next
			// discovery to retry. Don't fail the whole sync over it.
			slog.Warn("clustersync: prune orphaned kinds failed", "err", err)
		} else if n > 0 {
			slog.Info("clustersync: pruned orphaned objects", "rows", n)
		}
	}

	return out, nil
}

func hasVerb(vs []string, want string) bool {
	return slices.Contains(vs, want)
}

// listCRDs fetches the cluster's CRDs so we can tag kind_catalog and
// record OpenAPI schemas. Returns an empty list (not an error) if the
// caller lacks RBAC for apiextensions — most resources are still
// discoverable; we just won't know which are CRDs vs built-ins.
func listCRDs(ctx context.Context, dc discovery.DiscoveryInterface) ([]apiextensionsv1.CustomResourceDefinition, error) {
	// The discovery client doesn't expose CRDs directly; reach the REST
	// client under it to read apiextensions.k8s.io.
	type restable interface {
		RESTClient() rest.Interface
	}
	rc, ok := dc.(restable)
	if !ok {
		return nil, nil
	}
	raw, err := rc.RESTClient().Get().AbsPath("/apis/apiextensions.k8s.io/v1/customresourcedefinitions").DoRaw(ctx)
	if err != nil {
		return nil, err
	}
	var out apiextensionsv1.CustomResourceDefinitionList
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out.Items, nil
}

func isCRDKind(crds []apiextensionsv1.CustomResourceDefinition, gvk schema.GroupVersionKind) bool {
	for _, c := range crds {
		if c.Spec.Group != gvk.Group {
			continue
		}
		if c.Spec.Names.Kind != gvk.Kind {
			continue
		}
		for _, v := range c.Spec.Versions {
			if v.Name == gvk.Version {
				return true
			}
		}
	}
	return false
}

func crdSchemaJSON(crds []apiextensionsv1.CustomResourceDefinition, gvk schema.GroupVersionKind) string {
	for _, c := range crds {
		if c.Spec.Group != gvk.Group || c.Spec.Names.Kind != gvk.Kind {
			continue
		}
		for _, v := range c.Spec.Versions {
			if v.Name == gvk.Version && v.Schema != nil && v.Schema.OpenAPIV3Schema != nil {
				b, _ := json.Marshal(v.Schema.OpenAPIV3Schema)
				return string(b)
			}
		}
	}
	return ""
}
