// Package k8ssync drives the per-cluster local cache from real
// Kubernetes clusters defined in the user's kubeconfig.
//
// Discovery + dynamic client + raw Reflectors means one code path serves
// every Kind on every cluster — built-ins and CRDs alike. At startup we
// walk /apis discovery, pick every list/watchable resource, and spin up
// one Reflector per (group, version, resource) feeding a SQLite-backed
// cache.Store. Events get their own store (and table) because their
// access pattern differs; everything else lands in the universal
// `objects` table.
package k8ssync

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustercache"
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

// Cluster is the discovered identity + connection info for one
// kubeconfig context.
type Cluster struct {
	UUID    string
	Name    string
	Context string
	config  *rest.Config
	// fingerprint of config's connection/auth fields, so reconcile can detect a
	// kubeconfig entry changing under an unchanged cluster UUID and restart sync.
	fingerprint string
}

// configFingerprint hashes the connection- and credential-relevant fields of a
// rest.Config. Two configs with the same fingerprint connect and authenticate
// identically, so the cluster's running reflectors don't need restarting; a
// different fingerprint (rotated token, new client cert, changed CA/server URL,
// or an edited exec/auth-provider/impersonation block) means the open cluster is
// running on stale config and must be restarted. We hash the *static* exec/
// auth-provider config (command/args/env/plugin settings) — runtime token
// minting is the transport's job, but editing how tokens are obtained must
// invalidate the fingerprint.
//
// proxyURL is the kubeconfig cluster's proxy-url. clientcmd compiles it into
// rest.Config.Proxy (an opaque func we can't hash), so the caller passes the raw
// string: a changed proxy routes the connection differently and must restart the
// reflectors, even when every other field is identical.
func configFingerprint(cfg *rest.Config, proxyURL string) string {
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

// contextProxyURL returns the proxy-url of the cluster a kubeconfig context
// points at, or "" if the context, its cluster, or the field is absent. The
// Coordinator folds it into the config fingerprint because clientcmd compiles it
// into rest.Config.Proxy, an opaque func the fingerprint can't otherwise see.
func contextProxyURL(cfg *api.Config, ctxName string) string {
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

// probeClusterUID identifies a cluster by its kube-system namespace UID —
// stable across kubeconfig context renames and unique per cluster. The
// Coordinator (coordinator.go) calls this to resolve each reachable context to
// a UUID.
func probeClusterUID(ctx context.Context, cfg *rest.Config) (string, error) {
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return "", err
	}
	ns, err := cs.CoreV1().Namespaces().Get(ctx, "kube-system", metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	if ns.UID == "" {
		return "", errors.New("kube-system namespace has empty UID")
	}
	return string(ns.UID), nil
}

// Upstream implements clustercache.Upstream.
type Upstream struct {
	cache *clustercache.Manager

	mu      sync.RWMutex
	configs map[string]*rest.Config
}

func NewUpstream(c *clustercache.Manager) *Upstream {
	return &Upstream{
		cache:   c,
		configs: make(map[string]*rest.Config),
	}
}

// SetConfig registers (or replaces) the rest.Config the sync runner uses for a
// cluster. The Coordinator calls this before Open so Run finds a config.
func (u *Upstream) SetConfig(clusterUUID string, cfg *rest.Config) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.configs[clusterUUID] = cfg
}

// RemoveConfig drops a cluster's config after its context disappears from the
// kubeconfig and the Coordinator has closed it.
func (u *Upstream) RemoveConfig(clusterUUID string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	delete(u.configs, clusterUUID)
}

// gvrEntry packages everything needed to start one reflector.
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

// Run blocks for the cluster's lifetime, driving one Reflector per
// discovered GVK against the SQLite-backed stores. Returns when ctx is
// cancelled and every reflector has stopped.
func (u *Upstream) Run(ctx context.Context, clusterUUID string, writer *sql.DB) error {
	u.mu.RLock()
	cfg, ok := u.configs[clusterUUID]
	u.mu.RUnlock()
	if !ok {
		return fmt.Errorf("k8ssync: no kubeconfig for cluster %s", clusterUUID)
	}

	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return fmt.Errorf("discovery client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("dynamic client: %w", err)
	}

	cdb := u.cache.Lookup(clusterUUID)
	if cdb == nil {
		return fmt.Errorf("k8ssync: registry has no ClusterDB for %s", clusterUUID)
	}

	entries, err := discoverGVRs(ctx, dc, writer)
	if err != nil {
		return fmt.Errorf("discover: %w", err)
	}
	slog.Info("k8ssync: discovered resources", "cluster", clusterUUID, "count", len(entries))

	stop := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stop)
	}()

	var wg sync.WaitGroup
	for _, e := range entries {
		var store cache.Store
		if isEventGVK(e.GVK) {
			store = newEventsStore(ctx, clusterUUID, e.GVK, writer, cdb)
		} else {
			store = newObjectsStore(ctx, clusterUUID, e.GVK, writer, cdb)
		}
		lw := newDynamicListWatch(ctx, dyn, e)
		r := cache.NewReflector(lw, &unstructured.Unstructured{}, store, 0)
		slog.Debug("k8ssync: starting reflector", "cluster", clusterUUID, "gvk", e.GVK.String())
		wg.Go(func() {
			r.Run(stop)
		})
	}
	wg.Wait()
	return ctx.Err()
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
// means we'd start one Reflector per (resource, version) — duplicating
// watches against the same underlying data and getting deprecation
// warnings on every alpha/beta version we accidentally watched.
// Preferred-only gives us one Reflector per logical resource.
func discoverGVRs(ctx context.Context, dc discovery.DiscoveryInterface, writer *sql.DB) ([]gvrEntry, error) {
	complete := true
	lists, err := dc.ServerPreferredResources()
	if err != nil {
		if len(lists) == 0 {
			// Nothing usable came back (e.g. a transient discovery
			// failure or unreachable API server). Returning zero entries
			// here would make Run start no reflectors and report success,
			// leaving an open cluster that never mirrors data until the
			// next kubeconfig event. Fail so the sync surfaces the error.
			return nil, fmt.Errorf("discovery returned no resources: %w", err)
		}
		// Partial discovery errors are common when an aggregated API
		// server is down; the returned lists are still usable. Log and
		// continue rather than fail the whole cluster — but mark discovery
		// incomplete so we don't prune objects for a kind that's merely in a
		// transiently-unavailable group (see the prune below).
		complete = false
		slog.Warn("k8ssync: partial discovery", "err", err)
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
			slog.Warn("k8ssync: prune orphaned kinds failed", "err", err)
		} else if n > 0 {
			slog.Info("k8ssync: pruned orphaned objects", "rows", n)
		}
	}

	return out, nil
}

func hasVerb(vs []string, want string) bool {
	for _, v := range vs {
		if v == want {
			return true
		}
	}
	return false
}

// listCRDs fetches the cluster's CRDs so we can tag kind_catalog and
// record OpenAPI schemas. Returns an empty list (not an error) if the
// caller lacks RBAC for apiextensions — most resources are still
// discoverable; we just won't know which are CRDs vs built-ins.
func listCRDs(ctx context.Context, dc discovery.DiscoveryInterface) ([]apiextensionsv1.CustomResourceDefinition, error) {
	// The discovery client doesn't expose CRDs directly; build a dynamic
	// client off the same REST config to read apiextensions.k8s.io.
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

// newDynamicListWatch builds a ListerWatcher that calls the dynamic
// client's per-GVR Resource(). Namespaced vs cluster-scoped is the same
// API surface (dynamic.NamespaceableResourceInterface), so we don't
// branch on it.
func newDynamicListWatch(ctx context.Context, dyn dynamic.Interface, e gvrEntry) cache.ListerWatcher {
	return &cache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			return dyn.Resource(e.GVR).List(ctx, opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			return dyn.Resource(e.GVR).Watch(ctx, opts)
		},
	}
}
