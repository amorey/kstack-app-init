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
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeconn"
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
	// ReasonSweepFailed: the discovery request itself failed, so nothing is known about
	// the served kinds this run. The standing answer is untouched.
	ReasonSweepFailed probe.Reason = "SweepFailed"
	// ReasonSweepPartial: some groups answered, others did not. The partial answer is
	// committed — a group that went quiet has not stopped being served — and the run
	// fails, so the ladder retries sooner than the interval.
	ReasonSweepPartial probe.Reason = "SweepPartial"
)

// Kind is one served kind a cache can mirror, at the version the server prefers.
type Kind struct {
	GroupVersion string
	Kind         string
	Resource     string
	Namespaced   bool
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

// sweepTimeout bounds one run's context. It cannot cancel the sweep itself — client-go's
// discovery calls take no context — so the real per-request bound is the discovery
// client's own timeout; this is generous so a healthy long sweep is not cut.
const sweepTimeout = 5 * time.Minute

// catalogProbe answers which kinds the subject's server serves. The watch layer that
// will make it prompt (CRDs, APIServices) is specified and unbuilt; the interval is the
// promptness bound meanwhile. → docs/specs/kubecatalog-discovery.md.
type catalogProbe struct {
	// conn resolves the subject to the connection the service's lease holds; sweep is
	// the seam a test substitutes for the API server. Production wires connFor and
	// discoverServedKinds; nothing else may be nil here.
	conn  func(ctx context.Context, id string) (*kubeconn.Connection, error)
	sweep func(*kubeconn.Connection) (Catalog, error)
}

func (p *catalogProbe) Run(ctx context.Context, pass *probe.Pass[Catalog]) probe.Result {
	conn, err := p.conn(ctx, pass.Subject())
	if err != nil {
		// The outage is the cluster pass's to report; this run parks until the
		// connection bridge hears the pool reached the server.
		return probe.Suspend(ReasonNoConnection, err.Error())
	}

	found, err := p.sweep(conn)
	if err != nil && !found.Partial {
		if errors.Is(err, context.Canceled) {
			return probe.Skip()
		}
		// The standing answer is untouched: an empty answer is not "serves nothing",
		// and the ladder is what retries a server that keeps refusing a sweep this
		// expensive.
		return probe.Fail(ReasonSweepFailed, err)
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
func discoverServedKinds(conn *kubeconn.Connection) (Catalog, error) {
	lists, err := conn.Discovery.ServerPreferredResources()

	var groupErr *discovery.ErrGroupDiscoveryFailed
	if err != nil && !errors.As(err, &groupErr) {
		return Catalog{}, err
	}
	return Catalog{Kinds: servedKinds(lists), Partial: groupErr != nil}, err
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
