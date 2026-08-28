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

// The sweep: three probes over a per-cache subject, the fan-out over the group list, what
// it keeps, and the write to kind_catalog.
//
// **Discovery is a probe whose collection cannot be watched.** /api and /apis are plain
// GETs with no resourceVersion and no watch verb, so a sweep is a cold list with no watch
// phase and re-lists on the engine's cadence where a kind's worker would go live.
// SyncKinds reconciles its answer by fingerprint and prune, as a relist does by mark and
// sweep. **The answer goes to disk and nowhere else** — the sweep starts no worker and
// stops none, because what is mirrored is the records' to say; it publishes news and the
// mirror pass does the rest.
package kubesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// The three probes' registration names, which are their whole public identity.
const (
	nameAPIVersions = "apiVersions"
	nameAPIGroups   = "apiGroups"
	nameResources   = "resources"
)

// discoveryProbes is the whole sweep, for the reads and the wakes that address all of it.
var discoveryProbes = []string{nameAPIVersions, nameAPIGroups, nameResources}

// discoveryInterval is how often a settled sweep re-reads. A catalog moves when an operator
// installs a CRD or upgrades the api server, so it is human-paced; the store bus and the
// connection wake are what make it prompt.
const discoveryInterval = 10 * time.Minute

// maxDocument bounds a discovery document. Generous — a large cluster's group-version
// document runs to tens of kilobytes — and short of anything a hostile endpoint could
// stream.
const maxDocument = 8 << 20

// crdGVR is the collection IsCRD is read from: discovery describes a custom resource
// exactly as it describes a built-in, so nothing in the documents says which is which.
var crdGVR = schema.GroupVersionResource{
	Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions",
}

// The two kinds whose rows change a catalog. The cache already mirrors both, so the sweep
// subscribes to the store's bus rather than opening a second watch on the same collections
// over the same connection.
const (
	apiServiceAPIVersion = "apiregistration.k8s.io/v1"
	apiServiceResource   = "apiservices"
)

// eventsAPIGroup is the spelling of Event that is not synced: one store backs both, and
// v1/events is the canonical one.
const eventsAPIGroup = "events.k8s.io"

// registerProbes wires the sweep. Kept side by side on purpose — the set's rules are
// checked by eye.
func registerProbes(e *probe.Engine, s *Service) {
	probe.Register(e, nameAPIVersions, underSession(s, probeAPIVersions), probe.WithInterval(discoveryInterval))
	probe.Register(e, nameAPIGroups, underSession(s, probeAPIGroups), probe.WithInterval(discoveryInterval))
	// A data edge on both documents and no dependency edge: they are the fan-out's input,
	// and one that has not answered leaves it Skipped — waiting for the commit that wakes
	// it — rather than failing over a read another probe already reports.
	probe.Register(e, nameResources, underSession(s, probeResources), probe.WithInterval(discoveryInterval), probe.WithWatches(nameAPIVersions, nameAPIGroups))
}

// sessionScoped is what every probe body is registered wrapped in. It resolves the session
// and the connection vouching for the cache's identity, so a body is only its read.
//
// The session half is the promise ForgetDiscovery makes: the run is counted so a teardown
// waits for it, and its context ends with the session's so a teardown reaches the request
// in flight rather than only the schedule. Wrapped at registration because a body that
// forgot would break the promise silently.
//
// The connection half is the identity gate. A sweep runs on the engine, where waiting for
// a connection would hold a worker — so it records why and suspends, and the session's
// wake loop is what brings it back.
type sessionScoped[T any] struct {
	s    *Service
	body func(ctx context.Context, sess *session, conn *kubeconn.Connection, pass *probe.Pass[T]) probe.Result
}

func underSession[T any](s *Service, body func(context.Context, *session, *kubeconn.Connection, *probe.Pass[T]) probe.Result) sessionScoped[T] {
	return sessionScoped[T]{s: s, body: body}
}

func (p sessionScoped[T]) Run(ctx context.Context, pass *probe.Pass[T]) probe.Result {
	cacheID, ok := cacheIDOf(pass.Subject())
	if !ok {
		return probe.Skip()
	}
	sess, ok := p.s.enterSweep(cacheID)
	if !ok {
		// Nothing has armed this cache, or its teardown has begun: either way the claims a
		// run would write through are going, so it records nothing.
		return probe.Skip()
	}
	defer sess.leaveSweep()

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer context.AfterFunc(sess.ctx, cancel)()

	conn, err := sess.lease.ConnFor(runCtx, sess.params.ServerUID)
	if err != nil {
		reason := connectionReason(err)
		if reason == ReasonDiscoveryFailed {
			return probe.Fail(reason, err)
		}
		return probe.Suspend(reason, err.Error())
	}
	return p.body(runCtx, sess, conn, pass)
}

// connectionReason names why a cache cannot dial: the verdict a run suspends under, and
// what the wake loop compares its own answer against. One mapping, so a sweep and the loop
// that wakes it can never disagree about what the pool just said.
func connectionReason(err error) Reason {
	switch {
	case errors.Is(err, kubeconn.ErrNoConnection):
		return ReasonNoConnection
	case errors.Is(err, kubeconn.ErrIdentityMismatch):
		return ReasonIdentityMismatch
	default:
		return ReasonDiscoveryFailed
	}
}

// subjectOf is the engine's name for one cache, and cacheIDOf reads it back.
func subjectOf(cacheID int64) string { return strconv.FormatInt(cacheID, 10) }

func cacheIDOf(subject string) (int64, bool) {
	n, err := strconv.ParseInt(subject, 10, 64)
	return n, err == nil
}

// probeAPIVersions reads GET /api: the core group's versions, and half the fan-out's input.
func probeAPIVersions(ctx context.Context, _ *session, conn *kubeconn.Connection, pass *probe.Pass[[]string]) probe.Result {
	var doc metav1.APIVersions
	if err := getJSON(ctx, conn, "/api", &doc); err != nil {
		return probe.Fail(ReasonDiscoveryFailed, err)
	}
	// Only on a change: a committed value is what re-runs the fan-out watching it.
	if !pass.Known() || !slices.Equal(pass.Prev(), doc.Versions) {
		pass.Commit(doc.Versions)
	}
	return probe.Succeeded()
}

// probeAPIGroups reads GET /apis: the group-versions served, one per group.
func probeAPIGroups(ctx context.Context, _ *session, conn *kubeconn.Connection, pass *probe.Pass[[]string]) probe.Result {
	var doc metav1.APIGroupList
	if err := getJSON(ctx, conn, "/apis", &doc); err != nil {
		return probe.Fail(ReasonDiscoveryFailed, err)
	}
	next := preferredGroupVersions(doc)
	if !pass.Known() || !slices.Equal(pass.Prev(), next) {
		pass.Commit(next)
	}
	return probe.Succeeded()
}

// preferredGroupVersions is one group-version per group. Every served version mirrors the
// same objects again: two catalog rows, two workers, two watches, two copies of every row
// over one storage.
func preferredGroupVersions(doc metav1.APIGroupList) []string {
	gvs := make([]string, 0, len(doc.Groups))
	for _, g := range doc.Groups {
		gv := g.PreferredVersion.GroupVersion
		if gv == "" && len(g.Versions) > 0 {
			gv = g.Versions[0].GroupVersion
		}
		if gv != "" {
			gvs = append(gvs, gv)
		}
	}
	slices.Sort(gvs)
	return gvs
}

// probeResources is the fan-out: one document per group-version, filtered to what a worker
// can mirror, written to kind_catalog. Its value is the fingerprint it committed — the rows
// belong on disk, and a fingerprint is all "the answer moved" requires.
func probeResources(ctx context.Context, sess *session, conn *kubeconn.Connection, pass *probe.Pass[uint64]) probe.Result {
	gvs, ok := fanOutInput(pass.Snapshot())
	if !ok {
		// Neither document has answered yet. The data edge is what brings this back, so a
		// group list that will not load leaves the fan-out parked rather than failing it.
		return probe.Skip()
	}

	rows, failed := sweepResources(ctx, conn, gvs)
	if len(rows) == 0 && len(failed) > 0 {
		return probe.Fail(ReasonDiscoveryFailed, errors.New(strings.Join(failed, "; ")))
	}
	markCRDs(ctx, conn, rows)

	complete := len(failed) == 0
	fingerprint := fingerprintOf(rows, complete)
	wrote, err := commitCatalog(ctx, sess.store, rows, complete, fingerprint)
	if err != nil {
		return probe.Fail(ReasonDiscoveryFailed, err)
	}
	if wrote {
		sess.announce()
	}

	sess.recordSweep(!complete, strings.Join(failed, "; "))
	if !pass.Known() || fingerprint != pass.Prev() {
		pass.Commit(fingerprint)
	}
	return probe.Succeeded()
}

// fanOutInput is every group-version to read a document for: the core group's, and one per
// group. False while nothing has answered, which is the fan-out's cue to park.
func fanOutInput(snap probe.Snapshot) ([]string, bool) {
	core := probe.Get[[]string](snap, nameAPIVersions)
	groups := probe.Get[[]string](snap, nameAPIGroups)
	if !core.Known() || !groups.Known() {
		return nil, false
	}
	// The core group has no preferred marker of its own, and client-go's own aggregation
	// takes the first version /api lists.
	gvs := make([]string, 0, len(groups.Value)+1)
	if len(core.Value) > 0 {
		gvs = append(gvs, core.Value[0])
	}
	return append(gvs, groups.Value...), true
}

// sweepResources reads one document per group-version and keeps what a worker can mirror.
//
// A group that will not answer degrades the sweep rather than failing it: its kinds' workers
// watch independently and report their own verdicts, so a broken aggregated API shows up
// twice and correctly. What it does block is the prune, since SyncKinds takes one flag for
// the whole answer.
func sweepResources(ctx context.Context, conn *kubeconn.Connection, gvs []string) ([]kubestore.KindRow, []string) {
	var (
		rows   []kubestore.KindRow
		failed []string
	)
	for _, gv := range gvs {
		var doc metav1.APIResourceList
		if err := getJSON(ctx, conn, documentPath(gv), &doc); err != nil {
			failed = append(failed, fmt.Sprintf("%s: %v", gv, err))
			continue
		}
		for _, r := range doc.APIResources {
			if !mirrorable(gv, r) {
				continue
			}
			rows = append(rows, kubestore.KindRow{
				APIVersion: gv, Kind: r.Kind, Resource: r.Name, Scope: scopeOf(r.Namespaced),
			})
		}
	}
	// Sorted so a fingerprint names the answer rather than the order the legs came back in.
	slices.SortFunc(rows, func(a, b kubestore.KindRow) int {
		return strings.Compare(a.APIVersion+"/"+a.Kind, b.APIVersion+"/"+b.Kind)
	})
	return rows, failed
}

// mirrorable is the three filters a row must pass on top of the preferred-version one the
// group list already applied. A kind that gets through is one a worker can actually mirror.
func mirrorable(gv string, r metav1.APIResource) bool {
	switch {
	case strings.Contains(r.Name, "/"):
		// pods/log and deployments/scale are subresources with no collection behind them.
		return false
	case !slices.Contains(r.Verbs, "list") || !slices.Contains(r.Verbs, "watch"):
		// A create-only kind — tokenreviews, subjectaccessreviews, bindings — is a worker
		// that can only fail.
		return false
	case groupOf(gv) == eventsAPIGroup && r.Name == "events":
		// The server serves the same events under two spellings backed by one store, so
		// exactly one may be synced.
		return false
	}
	return true
}

// markCRDs flags the rows a CustomResourceDefinition serves, matched by (group, plural) with
// no version: one definition serves several, and a kind found at any of them is the same
// custom resource.
//
// **Best-effort, and outside the verdict**: listing CRDs is a cluster-scoped read RBAC
// commonly denies, and failing a sweep over it would take discovery away from users it
// otherwise serves. A refusal leaves every kind reading as built-in.
func markCRDs(ctx context.Context, conn *kubeconn.Connection, rows []kubestore.KindRow) {
	list, err := conn.Dynamic.Resource(crdGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return
	}

	served := make(map[[2]string]bool, len(list.Items))
	for i := range list.Items {
		group, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "group")
		plural, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "names", "plural")
		served[[2]string{group, plural}] = true
	}
	for i := range rows {
		rows[i].IsCRD = served[[2]string{groupOf(rows[i].APIVersion), rows[i].Resource}]
	}
}

// catalogWriter is the store as the sweep uses it: the fingerprint the table carries, and
// the write that replaces it.
type catalogWriter interface {
	KindsWithFingerprint(ctx context.Context) ([]kubestore.KindRow, uint64, bool, error)
	SyncKinds(ctx context.Context, rows []kubestore.KindRow, prune bool, fingerprint uint64) error
}

// commitCatalog writes the sweep's answer and reports whether it wrote.
//
// It **skips a write whose fingerprint the table already carries**: SyncKinds is a delete
// plus an upsert per row — six hundred statements for a large catalog, in one transaction
// against the single writer every kind's deltas queue behind. The stored fingerprint is read
// rather than remembered, so a restart and a cleared cache each write once instead of
// skipping over a table that no longer holds the answer.
func commitCatalog(ctx context.Context, store catalogWriter, rows []kubestore.KindRow, prune bool, fingerprint uint64) (bool, error) {
	_, stored, ok, err := store.KindsWithFingerprint(ctx)
	if err != nil {
		return false, err
	}
	if ok && stored == fingerprint {
		return false, nil
	}
	if err := store.SyncKinds(ctx, rows, prune, fingerprint); err != nil {
		return false, err
	}
	return true, nil
}

// fingerprintOf names one answer: the rows, and whether it was complete enough to prune. The
// prune flag is part of it because a partial answer and a complete one over identical rows
// are different writes — the second must delete what the first left standing.
func fingerprintOf(rows []kubestore.KindRow, prune bool) uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%t\x00", prune)
	for _, r := range rows {
		fmt.Fprintf(h, "%s\x00%s\x00%s\x00%s\x00%t\x00", r.APIVersion, r.Kind, r.Resource, r.Scope, r.IsCRD)
	}
	return h.Sum64()
}

// discoveryStateOf projects the engine's snapshot into what the seam serves. partial and
// message are the session's own record of the last sweep, which no Result can carry.
func discoveryStateOf(snap probe.Snapshot, partial bool, message string) DiscoveryState {
	state := DiscoveryState{
		APIVersions: probe.Get[[]string](snap, nameAPIVersions),
		APIGroups:   probe.Get[[]string](snap, nameAPIGroups),
		Resources:   probe.Get[uint64](snap, nameResources),
	}
	state.Reason, state.Message = discoveryVerdict(state, partial, message)
	return state
}

// discoveryVerdict is the precedence rule the seam owes its consumers: a suspended session
// over a failing read, a failing read over one that has yet to answer. Made here because the
// news feed gates on it either way, and a boundary folding its own would fold it differently.
func discoveryVerdict(state DiscoveryState, partial bool, message string) (string, string) {
	reads := []Attempts{state.APIVersions.Attempts, state.APIGroups.Attempts, state.Resources.Attempts}
	for _, a := range reads {
		if a.LastAttempt.Verdict == probe.VerdictSuspended {
			return string(a.LastAttempt.Reason), a.LastAttempt.Message
		}
	}
	for _, a := range reads {
		if a.LastAttempt.Verdict == probe.VerdictFailed {
			return ReasonDiscoveryFailed, a.LastAttempt.Message
		}
	}
	switch {
	case !state.Resources.Known():
		return ReasonDiscovering, ""
	case partial:
		return ReasonPartial, message
	}
	return ReasonDiscovered, ""
}

// getJSON reads one raw API path over the connection's pooled client — rather than
// client-go's discovery client, which takes no context, and a leg the engine cancels needs
// one. The documents have no collection semantics: no paging, no watch.
func getJSON(ctx context.Context, conn *kubeconn.Connection, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, conn.BaseURL.JoinPath(path).String(), nil)
	if err != nil {
		return fmt.Errorf("build request for %s: %w", path, err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := conn.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	defer resp.Body.Close()

	body := io.LimitReader(resp.Body, maxDocument)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Drained so the connection can be reused, and discarded: the status line is the
		// whole answer here, unlike the one endpoint that reports detail in a failure body.
		_, _ = io.Copy(io.Discard, body)
		return fmt.Errorf("%s: %s", path, resp.Status)
	}
	if err := json.NewDecoder(body).Decode(out); err != nil {
		return fmt.Errorf("malformed answer from %s: %w", path, err)
	}
	return nil
}

// documentPath is where a group-version's resource document lives: the core group under
// /api, everything else under /apis.
func documentPath(gv string) string {
	if strings.Contains(gv, "/") {
		return "/apis/" + gv
	}
	return "/api/" + gv
}

// groupOf is the api group of a group-version, empty for the core group.
func groupOf(apiVersion string) string {
	group, _, found := strings.Cut(apiVersion, "/")
	if !found {
		return ""
	}
	return group
}

func scopeOf(namespaced bool) string {
	if namespaced {
		return kubestore.ScopeNamespaced
	}
	return kubestore.ScopeCluster
}
