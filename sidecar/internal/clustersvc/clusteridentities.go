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

// The ClusterIdentity kind: one record per kube-context, holding what connecting with
// that context's credentials revealed. A beehive kind with no GraphQL type behind it —
// internal, like ClusterSource.
package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeidentity"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// ClusterIdentityGroupKind identifies the probe's kind: one object per set of
// credentials a cluster connects with, which is what a probe is scheduled, retried, and
// settled against.
//
// A kind of its own rather than fields on Cluster: it carries the probe's own status,
// conditions and cadence, and the edge that wakes every cluster reading it. The dial is
// not here — kubeconn's loops own that, so no pass blocks on a network.
var ClusterIdentityGroupKind = beehive.GroupKind{Kind: "ClusterIdentity"}

// namePrefixIdentityKubeconfig namespaces the kubeconfig-sourced identities inside this
// kind, the way Cluster names its own. Its own constant, not Cluster's: the two happen
// to share a convention, and a change to one is not a change to the other.
const namePrefixIdentityKubeconfig = "kubeconfig/"

// ClusterIdentityName is the reconcile key for one kube-context's identity.
func ClusterIdentityName(contextName string) string {
	return namePrefixIdentityKubeconfig + contextName
}

// ClusterIdentitySpec is what the owning cluster declares about the probe.
//
// Comparable by value, which is what lets a pass tell an unchanged relay from one worth
// writing. A field that breaks that — a pointer, a slice — puts every pass back on the
// write path.
type ClusterIdentitySpec struct {
	// Context is the kube-context whose credentials the probe dials with. Its own field
	// rather than Cluster's source union: that names where a *record* came from, and
	// borrowing it would tie this kind's persisted spec to one the GraphQL surface
	// shapes.
	Context string `json:"context"`
	// Enabled relays the cluster's toggle: a cluster the user switched off is not
	// probed. Relayed into the spec rather than read back through an edge — a spec
	// write bumps the generation, which is already the wake, and an edge onto the owner
	// would close a cycle against the one the owner declares onto this record.
	Enabled bool `json:"enabled"`
	// CredentialsDigest is what the context resolved to when the owner last looked. It
	// is the wake this record needs most: the credentials it probes with live in a file
	// nothing here watches, and a context re-pointed at another server keeps its name,
	// so only a moved digest says the probe is now about something else.
	//
	// Relayed for the same reason Enabled is — the write is the wake, and it reaches
	// this record alone rather than every record the file mentions.
	CredentialsDigest string `json:"credentialsDigest"`
}

// ClusterIdentityStatus is what the last probe learned. Nil fields mean never learned:
// each survives a probe that could not improve on it, so a server that goes unreachable
// keeps the identity it reported.
//
// No timestamps here, and none in the conditions beside them that a pass writes
// unconditionally. Every write to this record wakes every cluster that depends on it,
// so a field that moves on each probe would wake the fleet on each probe. Reporting only
// what was observed leaves beehive's no-op suppression to keep the steady state silent.
type ClusterIdentityStatus struct {
	ServerUID     *string `json:"serverUID,omitempty"`
	ServerVersion *string `json:"serverVersion,omitempty"`
	Username      *string `json:"username,omitempty"`
}

// identityProbeInterval paces the re-probe. Registered as the kind's individual pass,
// so each record is timed from the end of its own last probe and a fleet spreads itself
// out rather than dialing in one burst.
const identityProbeInterval = 5 * time.Minute

// ensureClusterIdentity gives a kubeconfig-sourced cluster the identity record its probe
// writes, owned by the cluster so beehive's GC cascades to it, and relays the cluster's
// Enabled toggle into its spec.
//
// Called by the parent's reconcile, since a controller only ever reconciles an object
// that already exists. The writes live here so the kind's vocabulary stays in the kind's
// file.
//
// Returns the record's id, which the caller declares its dependency on.
func ensureClusterIdentity(
	ctx context.Context,
	client beehive.Client[ClusterIdentitySpec, ClusterIdentityStatus],
	obj *beehive.Object[ClusterSpec, ClusterStatus],
	digest string,
) (beehive.ObjectID, error) {
	src := obj.Spec.Source.Kubeconfig
	if src == nil {
		return 0, nil
	}
	spec := ClusterIdentitySpec{
		Context:           src.Context,
		Enabled:           obj.Spec.Enabled,
		CredentialsDigest: digest,
	}
	name := ClusterIdentityName(src.Context)

	// One transaction resolves the name and writes: beehive measured the probe a caller
	// would put in front of this and found it costs more than it saves below roughly four
	// converged writes per changed one.
	//
	// A row awaiting collection is refused rather than rewritten — the relay would land on
	// an incarnation the GC is coming for, and the replacement cannot be created until the
	// name is released with it. Nothing to wake and nothing to project until then, which
	// is what the zero id tells the caller.
	found, _, err := client.CreateOrUpdate(ctx, name, spec, beehive.WithOwner(obj.ID))
	switch {
	case errors.Is(err, beehive.ErrDeletionPending):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("apply cluster identity %s: %w", name, err)
	}
	return found.ID, nil
}

// clusterIdentityController reports what the probe of one context's credentials found.
// The dial itself happens on kubeconn's loops, off this pass's goroutine.
type clusterIdentityController struct {
	lifecycle.None
	deps
}

func (c *clusterIdentityController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterIdentityStatus],
	obj *beehive.Object[ClusterIdentitySpec, ClusterIdentityStatus],
) beehive.ReconcileResult {
	// A record on its way out is about to be collected with its owner, and beehive
	// collects it with no finalizer to clear. Reporting on it would only write a
	// condition the GC is coming for.
	if obj.DeletionRequestedAt != nil {
		return beehive.Settled()
	}

	// A disabled cluster is not probed at all. The toggle is relayed into this spec, so the
	// pass answers without reading its owner.
	if !obj.Spec.Enabled {
		return c.report(ctx, client, ConditionFalse, ReasonInactive, "cluster is disabled")
	}

	state, known := c.kubeidentitySvc.Get(obj.Spec.Context)
	if !known {
		// Asked and not answered. The signal a probe publishes is what will bring this record
		// back, with the resync behind it covering a signal that went missing.
		return c.report(ctx, client, ConditionUnknown, ReasonConnecting, "probe pending")
	}

	if errors.Is(state.Err, kubeconfig.ErrContextNotFound) {
		// The context left the file. Told apart from a probe that failed because they are
		// different news: this one the user did on purpose, and there is nothing to
		// connect to until they undo it.
		return c.report(ctx, client, ConditionFalse, ReasonInactive,
			"kube-context is no longer in the kubeconfig")
	}
	if state.Err != nil {
		// The context is there and its entries do not resolve — a file the user has to fix,
		// which beehive's backoff cannot. Reported on the record rather than failing the
		// pass, and left to this kind's cadence to retry.
		return c.report(ctx, client, ConditionFalse, ReasonResolveFailed, state.Err.Error())
	}

	if err := client.Within(ctx, func(ctx context.Context) error {
		if err := client.UpdateStatus(ctx, identityStatus(state.Identity)); err != nil {
			return err
		}
		return client.SetCondition(ctx, connectedCondition())
	}); err != nil {
		return beehive.Fail(fmt.Errorf("report cluster identity: %w", err))
	}
	return beehive.Settled()
}

// identityStatus is what a probe learned, in the shape the record stores. A field the
// probe could not read stays nil rather than empty: nil is what a cluster's projection
// reads as "no better answer than the one I have".
func identityStatus(id kubeidentity.Identity) ClusterIdentityStatus {
	var status ClusterIdentityStatus
	if id.ServerUID != "" {
		status.ServerUID = &id.ServerUID
	}
	if id.ServerVersion != "" {
		status.ServerVersion = &id.ServerVersion
	}
	if id.Username != "" {
		status.Username = &id.Username
	}
	return status
}

// connectedCondition reports a probe that reached the server.
func connectedCondition() Condition {
	return LiveCondition(ConditionConnected, ConditionTrue, ReasonConnected, "")
}

// report writes what this pass observed and settles. Only the condition moves for now,
// and beehive suppresses one that matches what is stored — which is what keeps a record
// whose answer has not changed from waking every cluster that depends on it.
func (c *clusterIdentityController) report(
	ctx context.Context,
	client beehive.ControllerClient[ClusterIdentityStatus],
	status ConditionStatus,
	reason, message string,
) beehive.ReconcileResult {
	cond := LiveCondition(ConditionConnected, status, reason, message)
	if err := client.SetCondition(ctx, cond); err != nil {
		return beehive.Fail(fmt.Errorf("set cluster identity condition: %w", err))
	}
	return beehive.Settled()
}
