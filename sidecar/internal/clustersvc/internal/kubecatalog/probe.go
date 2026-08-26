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

// The sweep: what one discovery run asks the server and how the answer classifies.
// Scheduling belongs to the engine; the leases the sweep borrows are service.go's.
package kubecatalog

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubestore"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// nameCatalog is the one probe's registration name — its whole public identity to the
// engine — and keyCatalog states its name↔type pairing once for every read.
const nameCatalog = "catalog"

var keyCatalog = probe.NewKey[Catalog](nameCatalog)

// The sweep's reason vocabulary, in the attempt a caller reads why a run concluded
// what it did. The caller's condition reasons are its own; these are the leaf's.
const (
	// ReasonNoConnection: the subject's context resolves to no connection, so nothing
	// was asked. The sweep parks until the pool says the connection moved.
	ReasonNoConnection probe.Reason = "NoConnection"
	// ReasonIdentityMismatch: the context answers as a server this subject is not for,
	// or has not said which server it is yet. Nothing was asked either way — kinds read
	// off another cluster must not land in this one's catalog. Both park until the pool
	// reports the identity moved; the message says which case it was.
	//
	// The first is a context re-pointed at another cluster, and lasts: the subject's
	// cache is superseded and will be disarmed when that reaches the record. The second
	// is the gap between a connection answering and the UID probe behind it answering,
	// and clears itself within a cycle.
	ReasonIdentityMismatch probe.Reason = "IdentityMismatch"
	// ReasonSweepFailed: the discovery request itself failed, so nothing is known about
	// the served kinds this run. The standing answer is untouched.
	ReasonSweepFailed probe.Reason = "SweepFailed"
	// ReasonSweepPartial: some groups answered, others did not. The partial answer is
	// committed — a group that went quiet has not stopped being served — and the run
	// fails, so the ladder retries sooner than the interval.
	ReasonSweepPartial probe.Reason = "SweepPartial"
	// ReasonStoreFailed: the answer is good and the mirror would not take it, so nothing
	// was committed and nobody was signalled. It outranks ReasonSweepPartial: a partial
	// answer whose write failed committed nothing at all, and naming the incomplete
	// answer would point a reader at an api group that is not the problem.
	ReasonStoreFailed probe.Reason = "StoreFailed"
	// ReasonStoreRemoved: the cache is being torn down and this subject's Forget is on
	// its way. There is nothing to write into and nothing worth reporting, so the run
	// suspends and lets the teardown disarm it.
	ReasonStoreRemoved probe.Reason = "StoreRemoved"
)

// Kind is one served kind a cache can mirror, at the version the server prefers.
type Kind struct {
	GroupVersion string
	Kind         string
	Resource     string
	Namespaced   bool
	// IsCRD marks a kind served by a CustomResourceDefinition rather than built in.
	// Best-effort: a cluster that refuses the CRD list reads every kind as built-in.
	IsCRD bool
}

// Catalog is one sweep's answer: the mirrorable kinds, sorted, and whether the list is
// the whole truth — an aggregated group that failed to answer makes it partial.
type Catalog struct {
	Kinds   []Kind
	Partial bool
}

// equal is the commit guard's compare; the slice keeps Catalog out of ==.
func (c Catalog) equal(o Catalog) bool {
	return c.Partial == o.Partial && slices.Equal(c.Kinds, o.Kinds)
}

// sweepInterval paces re-discovery: the pull cadence correctness rests on, since what
// the sweep enumerates is a remote server's and nothing here sees a CRD land.
const sweepInterval = 10 * time.Minute

// sweepRetryBase starts the failure ladder, capped at sweepInterval. Above the engine's
// default second, because what a failure retries here is a full ServerPreferredResources
// — dozens of round trips over every group-version, paid for at someone else's cluster —
// and promptness comes from the watch, never from the ladder.
const sweepRetryBase = 30 * time.Second

// sweepTimeout bounds one run's context. It cannot cancel the sweep itself — client-go's
// discovery calls take no context — so the real per-request bound is the discovery
// client's own timeout; this is generous so a healthy long sweep is not cut.
const sweepTimeout = 5 * time.Minute

// catalogProbe answers which kinds the subject's server serves. The interval is the
// correctness bound; the watch it holds over the connection is the promptness.
type catalogProbe struct {
	// conn resolves the subject to the connection the service's lease holds; sweep is
	// the seam a test substitutes for the API server. Production wires connFor and
	// discoverServedKinds; nothing else may be nil here.
	conn  func(ctx context.Context, id string) (*kubeconn.Connection, error)
	sweep func(ctx context.Context, conn *kubeconn.Connection) (Catalog, error)
	// mirror writes one answer into the subject's cache store, the seam the Service wires
	// to the store manager: the sweep is this table's one writer, so the rows are the
	// leaf's own to lay down.
	mirror func(ctx context.Context, id string, c Catalog) error
	// watch and unwatch are the standing watch's lifetime, which is the Service's to hold —
	// establishing one has to be measured against whether the subject is still tracked. watch
	// returns once the watch is open, bounded by ctx.
	watch   func(ctx context.Context, id string, conn *kubeconn.Connection)
	unwatch func(id string)
}

func (p *catalogProbe) Run(ctx context.Context, pass *probe.Pass[Catalog]) probe.Result {
	conn, err := p.conn(ctx, pass.Subject())
	if err != nil {
		// Every refusal takes the watcher with it. conn.Done() does not cover this: a
		// connection that goes conflicted is never retired, so one left standing would go on
		// waking a subject that can only suspend.
		p.unwatch(pass.Subject())

		if errors.Is(err, kubeconn.ErrIdentityMismatch) {
			return probe.Suspend(ReasonIdentityMismatch, err.Error())
		}
		// The outage is the cluster pass's to report; this run parks until the
		// connection bridge hears the pool reached the server.
		return probe.Suspend(ReasonNoConnection, err.Error())
	}

	// Reconciled here, against the connection rather than the answer, because the connection is
	// the whole of what the watch needs: conn has just proved reachable and this subject's own,
	// which is what the streams inherit. Deferring it to a clean sweep would leave the previous
	// connection's watcher standing for as long as discovery is failing — and a partial sweep
	// over an aggregated API server that is down is not a short window.
	p.watch(ctx, pass.Subject(), conn)

	found, err := p.sweep(ctx, conn)
	if err != nil && !found.Partial {
		if errors.Is(err, context.Canceled) {
			return probe.Skip()
		}
		// The standing answer is untouched: an empty answer is not "serves nothing",
		// and the ladder is what retries a server that keeps refusing a sweep this
		// expensive.
		return probe.Fail(ReasonSweepFailed, err)
	}

	// The rows go down before anything is committed: a commit is the fold's wake, and
	// waking it over rows that are not there yet would have it converge on a table the
	// write is still catching up to. Unconditional, whether or not the answer moved,
	// which is what puts a wiped table back with no repair protocol.
	if err := p.mirror(ctx, pass.Subject(), found); err != nil {
		if errors.Is(err, kubestore.ErrRemoved) {
			return probe.Suspend(ReasonStoreRemoved, err.Error())
		}
		return probe.Fail(ReasonStoreFailed, err)
	}

	// Commit only on a change — a committed value is the fold's wake — and on the first
	// answer whatever it is: a cluster serving nothing mirrorable answers the zero
	// Catalog, which Prev cannot tell from "never swept".
	if !pass.Known() || !found.equal(pass.Prev()) {
		pass.Commit(found)
	}
	if found.Partial {
		return probe.Fail(ReasonSweepPartial, err)
	}
	return probe.Succeeded()
}

// discoverServedKinds enumerates the kinds the server serves, each at the version it
// prefers. A partial answer is the routine failure rather than an exception: an
// aggregated API server that is down fails its own group and no other, and client-go
// hands back the groups that did answer alongside the error naming the ones that did
// not — so a partial Catalog comes back with the error still attached.
func discoverServedKinds(ctx context.Context, conn *kubeconn.Connection) (Catalog, error) {
	lists, err := conn.Discovery.ServerPreferredResources()

	var groupErr *discovery.ErrGroupDiscoveryFailed
	if err != nil && !errors.As(err, &groupErr) {
		return Catalog{}, err
	}
	kinds := servedKinds(lists)

	// Best-effort, and deliberately not part of the verdict: listing CRDs is a
	// cluster-scoped read RBAC commonly denies, and failing the sweep over it would take
	// discovery away from users this one otherwise serves. A refusal leaves every kind
	// reading as built-in.
	if crds, crdErr := listCRDs(ctx, conn); crdErr == nil {
		markCRDs(kinds, crds)
	}
	return Catalog{Kinds: kinds, Partial: groupErr != nil}, err
}

// crdRef is one CustomResourceDefinition as the match needs it. The version is
// deliberately absent: one definition serves several, and a kind discovered at any of
// them is the same custom resource.
type crdRef struct {
	group  string
	plural string
}

// listCRDs enumerates the cluster's CustomResourceDefinitions, over the same collection
// the watcher already streams.
func listCRDs(ctx context.Context, conn *kubeconn.Connection) ([]crdRef, error) {
	list, err := conn.Dynamic.Resource(crdGVR).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	refs := make([]crdRef, 0, len(list.Items))
	for _, item := range list.Items {
		group, _, _ := unstructured.NestedString(item.Object, "spec", "group")
		plural, _, _ := unstructured.NestedString(item.Object, "spec", "names", "plural")
		if group != "" && plural != "" {
			refs = append(refs, crdRef{group: group, plural: plural})
		}
	}
	return refs, nil
}

// markCRDs sets the bit on every kind one of these definitions serves, in place.
func markCRDs(kinds []Kind, crds []crdRef) {
	defined := make(map[crdRef]struct{}, len(crds))
	for _, c := range crds {
		defined[c] = struct{}{}
	}
	for i, k := range kinds {
		gv, err := schema.ParseGroupVersion(k.GroupVersion)
		if err != nil {
			continue
		}
		if _, ok := defined[crdRef{group: gv.Group, plural: k.Resource}]; ok {
			kinds[i].IsCRD = true
		}
	}
}

// servedKinds filters a discovery answer down to what a cache can mirror, sorted so a
// sweep is deterministic.
func servedKinds(lists []*metav1.APIResourceList) []Kind {
	var kinds []Kind
	for _, list := range lists {
		if list == nil || !mirrorableGroup(list.GroupVersion) {
			continue
		}
		for _, res := range list.APIResources {
			if !mirrorableResource(res) {
				continue
			}
			kinds = append(kinds, Kind{
				GroupVersion: list.GroupVersion,
				Kind:         res.Kind,
				Resource:     res.Name,
				Namespaced:   res.Namespaced,
			})
		}
	}

	slices.SortFunc(kinds, func(a, b Kind) int {
		if c := strings.Compare(a.GroupVersion, b.GroupVersion); c != 0 {
			return c
		}
		return strings.Compare(a.Resource, b.Resource)
	})
	return kinds
}

// eventsAltGroup is the alternate spelling of the core event store.
const eventsAltGroup = "events.k8s.io"

// mirrorableGroup drops the alternate events spelling. One event store is served under
// two group-versions, so mirroring both would cache every event twice; canonical v1 wins.
func mirrorableGroup(groupVersion string) bool {
	gv, err := schema.ParseGroupVersion(groupVersion)
	return err == nil && gv.Group != eventsAltGroup
}

// mirrorableResource reports whether one resource can back a cache. A subresource has no
// collection of its own, and a kind that cannot be listed and watched cannot be mirrored —
// which is the whole of what a worker does.
func mirrorableResource(res metav1.APIResource) bool {
	if strings.Contains(res.Name, "/") {
		return false
	}
	return slices.Contains(res.Verbs, "list") && slices.Contains(res.Verbs, "watch")
}
