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

// The ClusterSource kind: one discovery anchor per ClusterSpecSource variant, naming
// the clusters that source declares. Its beehive shapes, its controller, and the
// bootstrap that creates the anchors. Internal to this package — no GraphQL type
// binds it.
package clustersvc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/amorey/beehive"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/lifecycle"
)

// ClusterSourceGroupKind identifies the discovery-anchor kind: one object per source
// variant, which is what a discovery pass is scheduled, retried, and settled against.
var ClusterSourceGroupKind = beehive.GroupKind{Kind: "ClusterSource"}

// ClusterSourceNameKubeconfig names the kubeconfig anchor. Exactly one per variant,
// so creation is idempotent under name-uniqueness dedup; a creation key only, never
// an identity.
const ClusterSourceNameKubeconfig = "clustersource/kubeconfig"

// clusterSourceNames is every anchor the package keeps. A new source variant is one
// entry here, plus the controller branch that reads it.
var clusterSourceNames = []string{ClusterSourceNameKubeconfig}

// clusterSourceResyncInterval paces the discovery pass, as the kind's full-pass sweep
// (registered in service.go). A backstop, not the mechanism: kubeconfigNotifier is
// what makes a file change prompt, and this is what covers a kick that was never
// delivered. A pass observing an unchanged snapshot writes nothing, so the tick wakes
// no dependent.
//
// A sweep rather than a requeue on the result, because the poll is the anchor's
// correctness: every return path would otherwise have to re-arm it, and the one that
// forgot would stop discovery for the life of the process.
const clusterSourceResyncInterval = 10 * time.Minute

// ClusterSourceSpec is empty. An anchor's identity is its name and a source has
// nothing the user configures; the object exists to give the discovery pass a place
// in the control plane rather than to hold desired state.
type ClusterSourceSpec struct{}

// ClusterSourceStatus is the anchor's propagation channel and nothing else. Every
// Cluster from this source depends on the anchor, so writing this wakes all of them —
// which is the point, since their observation reads the source rather than the object
// beehive handed them, and no store write would otherwise reach it.
type ClusterSourceStatus struct {
	// Fingerprint identifies what the last pass observed. It changes exactly when
	// something a dependent would observe changed, so an unchanged snapshot leaves it
	// alone; a stamp that moved every pass would wake every Cluster every pass.
	Fingerprint string `json:"fingerprint"`
}

// clusterSourceController runs one source's discovery pass: it creates the Cluster
// records the source declares, then publishes what it observed for those records to
// wake on.
//
// It never writes a Cluster's status. That is the Cluster controller's, which is why
// this pass ends at a fingerprint rather than an observation — set membership here,
// per-object interpretation there.
type clusterSourceController struct {
	lifecycle.None
	deps
}

func (c *clusterSourceController) Reconcile(
	ctx context.Context,
	client beehive.ControllerClient[ClusterSourceStatus],
	obj *beehive.Object[ClusterSourceSpec, ClusterSourceStatus],
) beehive.ReconcileResult {
	// An anchor on its way out has no set to maintain, and beehive collects it either
	// way. Nothing deletes one today.
	if obj.DeletionRequestedAt != nil {
		return beehive.Settled()
	}

	// beehive starts ahead of this, so an owed pass can arrive before the kubeconfig's
	// first read. The pre-read config is empty and indistinguishable from a file with
	// no contexts, so importing against it would fingerprint an empty set and wake
	// every record to observe its context absent.
	cfg, loaded := c.kubeconfigSvc.Get()
	if !loaded {
		return beehive.Unsettled().RequeueAfter(startupRequeue)
	}

	// Runs unconditionally, not behind the fingerprint gate: a create that failed is
	// retried against the snapshot that failed, so a pass returning early on an
	// unchanged fingerprint would skip the retry and never import that context.
	//
	// Its error is carried to the end rather than returned here. The publish below is
	// the only thing that wakes a dependent, and the two describe different things —
	// the fingerprint says what this pass observed, not which records it managed to
	// create. One context stuck behind a draining row would otherwise hold back the
	// observation for every other record, including a context that departed in the
	// same edit, which nothing else would reach.
	createErr := ensureKubeconfigClusters(ctx, c.clusterClient, cfg)

	fingerprint, err := kubeconfigFingerprint(cfg)
	if err != nil {
		return beehive.Fail(errors.Join(createErr, err))
	}

	// A repeat write would only marshal the same bytes and open a transaction to find
	// them equal; the settle below is what a pass that observed nothing new reports.
	if obj.Status == nil || obj.Status.Fingerprint != fingerprint {
		if err := client.UpdateStatus(ctx, ClusterSourceStatus{Fingerprint: fingerprint}); err != nil {
			return beehive.Fail(errors.Join(createErr, fmt.Errorf("update cluster source status: %w", err)))
		}
	}

	if createErr != nil {
		// A stuck create is retried by beehive's backoff ladder, which is tighter than
		// the sweep pacing this kind.
		return beehive.Fail(createErr)
	}
	return beehive.Settled()
}

// ensureClusterSources gives every variant its anchor. The one creation in this
// package with nothing above it — every other record is created by its parent, and an
// anchor is what a parent would have been — so it runs from the service's startup
// rather than from a reconcile.
func ensureClusterSources(ctx context.Context, client beehive.Client[ClusterSourceSpec, ClusterSourceStatus]) error {
	for _, name := range clusterSourceNames {
		if _, _, err := client.GetOrCreate(ctx, name, ClusterSourceSpec{}); err != nil {
			return fmt.Errorf("create cluster source %s: %w", name, err)
		}
	}
	return nil
}

// kubeconfigObservation pairs a context with what the fold makes of it. Marshalled as
// the digest's input, so the context name is part of what is hashed.
type kubeconfigObservation struct {
	Context  string                         `json:"context"`
	Observed *ClusterStatusSourceKubeconfig `json:"observed"`
}

// kubeconfigFingerprint digests what every record's own reconcile will observe, by
// running the same fold this pass's dependents run. Derived from observeKubeconfig
// rather than from the config's fields on purpose: a digest that named the fields
// itself would silently stop tracking the day the fold reads one more, and no record
// would ever be woken to observe it.
//
// A departed context is absent from cfg, so it contributes nothing — but its
// departure changes the name set, which moves the digest and wakes it to re-observe
// against its own last-known state.
func kubeconfigFingerprint(cfg *api.Config) (string, error) {
	names := make([]string, 0, len(cfg.Contexts))
	for name := range cfg.Contexts {
		names = append(names, name)
	}
	slices.Sort(names)

	// Sorted, because contexts come out of a map and the digest must not depend on
	// range order. JSON delimits its own values, so no two field boundaries collide.
	observed := make([]kubeconfigObservation, 0, len(names))
	for _, name := range names {
		observed = append(observed, kubeconfigObservation{
			Context:  name,
			Observed: observeKubeconfig(cfg, &ClusterSpecSourceKubeconfig{Context: name}, nil),
		})
	}

	b, err := json.Marshal(observed)
	if err != nil {
		return "", fmt.Errorf("digest kubeconfig observations: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
