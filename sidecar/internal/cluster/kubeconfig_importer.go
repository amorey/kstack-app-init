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

package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/amorey/beehive"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
)

// KubeconfigImporter watches the kubeconfig and is the sole creator of
// kubeconfig-sourced Cluster beehive objects (one per kube-context). Creation-only:
// on each snapshot it creates a Cluster — keyed by the deterministic name
// "kubeconfig/{context}" — for every context no Cluster yet references, writing only
// the source reference (ClusterSpec.Source). It never updates, orphans, or deletes.
// The deterministic name means beehive's per-kind name-uniqueness rules out a
// duplicate even under a concurrent create.
//
// Everything observed about the context (cluster/user names, presence, is-current)
// is written to ClusterStatus.Source by the ClusterCoreController, which observes it
// live each reconcile. So a departed context is orphaned by the controller flipping
// IsPresent=false, and a returning context revives because its (never-deleted)
// Cluster still references it — the importer finds it and skips re-creation.
//
// A future creator (manual, cloud) is a sibling importer writing Cluster objects with
// a different Source variant — they share this one Cluster kind.
type KubeconfigImporter struct {
	cfgSource  KubeConfigSource
	coreClient beehive.Client[ClusterSpec, ClusterStatus]
	baseCtx    context.Context
	baseCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewKubeconfigImporter builds an importer that uses coreClient to manage
// kubeconfig-sourced Cluster objects in beehive.
func NewKubeconfigImporter(cfgSource KubeConfigSource, coreClient beehive.Client[ClusterSpec, ClusterStatus]) *KubeconfigImporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &KubeconfigImporter{
		cfgSource:  cfgSource,
		coreClient: coreClient,
		baseCtx:    ctx,
		baseCancel: cancel,
	}
}

// ClusterClient exposes the underlying beehive client for tests that need to
// inspect the Cluster objects after a reconcile.
func (im *KubeconfigImporter) ClusterClient() beehive.Client[ClusterSpec, ClusterStatus] {
	return im.coreClient
}

// Start launches the import loop. The watcher subscription is established
// before Start returns and is current-on-subscribe, so startup state imports
// immediately.
func (im *KubeconfigImporter) Start() {
	sub := im.cfgSource.Subscribe()
	im.wg.Add(1)
	go func() {
		defer im.wg.Done()
		defer sub.Close()
		im.run(sub)
	}()
}

// Stop cancels the import loop and joins it.
func (im *KubeconfigImporter) Stop() {
	im.baseCancel()
	im.wg.Wait()
}

// run consumes kubeconfig snapshots until the importer is stopped or the
// watcher closes. An import failure is logged but not fatal: the loop stays up
// and the level-triggered diff re-derives everything on the next snapshot.
func (im *KubeconfigImporter) run(sub k8shelpers.KubeConfigSubscription) {
	// The last snapshot, kept so a failed import can be retried against it. Nothing else
	// would: this loop is driven by kubeconfig CHANGES, and the most common reason an
	// import is incomplete — a context whose name is still held by a Cluster draining after
	// a user deleted it — resolves on its own within seconds, with no kubeconfig write
	// behind it. Without the retry that context stays missing from the app until the user
	// edits their kubeconfig or restarts.
	var last *api.Config
	retry := time.NewTimer(importRetryInterval)
	retry.Stop()
	defer retry.Stop()

	reconcile := func() {
		err := im.ReconcileClusterSet(im.baseCtx, last)
		switch {
		case err == nil:
		case errors.Is(err, errNameHeld):
			slog.Debug("kubeconfig import incomplete, retrying", "err", err)
		default:
			slog.Error("kubeconfig import failed", "err", err)
		}
		if err != nil {
			retry.Reset(importRetryInterval)
		}
	}

	for {
		select {
		case <-im.baseCtx.Done():
			return
		case cfg, ok := <-sub.Chan():
			if !ok {
				return
			}
			last = cfg
			reconcile()
		case <-retry.C:
			if last != nil {
				reconcile()
			}
		}
	}
}

// importRetryInterval paces the retry of an incomplete import. Sized for the case that
// drives it: a name held by a Cluster whose deletion is draining, which clears as soon as
// its cache workers stop and GC collects the row.
const importRetryInterval = 2 * time.Second

// errNameHeld reports that a context could not be imported yet because a Cluster still
// draining holds its name. Distinct from a real failure: nothing is wrong, the import is
// simply incomplete, and the caller should try the same snapshot again shortly.
var errNameHeld = errors.New("a context's name is held by a Cluster still being deleted")

// ReconcileClusterSet aligns the kubeconfig-sourced Cluster beehive objects
// with one kubeconfig snapshot. It is safe to call at any time, any number of
// times: an unchanged snapshot writes nothing, so it triggers nothing
// downstream.
func (im *KubeconfigImporter) ReconcileClusterSet(ctx context.Context, cfg *api.Config) error {
	existing, err := im.coreClient.List(ctx)
	if err != nil {
		return err
	}
	// Contexts already tracked by a (non-deleting) kubeconfig-sourced Cluster. A
	// departed context's Cluster still references it, so a returning context is found
	// here and skipped — the controller revives it.
	tracked := map[string]bool{}
	for _, obj := range existing {
		if obj.DeletionRequestedAt != nil {
			continue // ignore objects being deleted
		}
		if kc := obj.Spec.Source.Kubeconfig; kc != nil {
			tracked[kc.Context] = true
		}
	}

	// Create a Cluster for any context not yet tracked. Only the source reference
	// is written; the ClusterCoreController observes presence/names/isDefault from
	// the live kubeconfig and writes them to status.
	//
	// Each context is isolated: one failure must not abandon the rest of the snapshot.
	// Contexts come out of a map, so an abort would skip an ARBITRARY subset — a set that
	// changes run to run, which is the worst shape a partial import can have.
	var errs []error
	var held []string
	for name := range cfg.Contexts {
		if tracked[name] {
			continue
		}
		err := im.createCluster(ctx, name)
		switch {
		case err == nil:
		case errors.Is(err, beehive.ErrNameTaken):
			// The name belongs to a Cluster on its way out — tracked deliberately ignores
			// deletion-pending objects (so a re-created context gets a fresh record), but
			// the name is held until GC collects the row.
			//
			// Not a failure, but not finished either, and it must not be swallowed: this
			// loop is driven by kubeconfig CHANGES, so "the next snapshot creates it" can
			// mean never. A user deleting a cluster produces exactly this — the delete's
			// drain window — and the context would then be missing from the app until they
			// edited their kubeconfig or restarted. errNameHeld asks the caller to retry.
			held = append(held, name)
		default:
			errs = append(errs, fmt.Errorf("create cluster for context %q: %w", name, err))
		}
	}
	if len(held) > 0 {
		slices.Sort(held) // map iteration order; a stable message is a readable log
		errs = append(errs, fmt.Errorf("%w: %s", errNameHeld, strings.Join(held, ", ")))
	}
	return errors.Join(errs...)
}

// createCluster creates a kubeconfig-sourced Cluster keyed by the deterministic
// name "kubeconfig/{context}", writing only the source reference. The name is the
// context's natural key (the importer's reconcile/uniqueness key); the record's
// identity is the ObjectID beehive assigns, not this name.
func (im *KubeconfigImporter) createCluster(ctx context.Context, contextName string) error {
	_, err := im.coreClient.Create(ctx, kubeconfigName(contextName), ClusterSpec{
		SyncEnabled: true,
		Enabled:     true,
		Source:      ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: contextName}},
	})
	return err
}
