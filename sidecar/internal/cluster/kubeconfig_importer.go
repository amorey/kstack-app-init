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

// KubeconfigImporter is the sole creator of Cluster objects, one per kube-context, and is
// CREATION-ONLY: each snapshot creates a Cluster (deterministic name "kubeconfig/{context}",
// so beehive's name-uniqueness rules out a duplicate under a concurrent create) for every
// context none yet references, writing only the source reference. It never updates, orphans,
// or deletes — a departed context is orphaned by ClusterCoreController flipping
// IsPresent=false, and a returning one reuses its never-deleted Cluster.
// See docs/adr/2026-08-09-beehive-control-plane.md.
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

// ClusterClient exposes the beehive client for tests inspecting Cluster objects.
func (im *KubeconfigImporter) ClusterClient() beehive.Client[ClusterSpec, ClusterStatus] {
	return im.coreClient
}

// Start launches the import loop. The subscription is established before Start returns and
// is current-on-subscribe, so startup state imports immediately.
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

// run consumes kubeconfig snapshots until stopped or the watcher closes. An import failure
// is logged, not fatal: the level-triggered diff re-derives everything next snapshot.
func (im *KubeconfigImporter) run(sub k8shelpers.KubeConfigSubscription) {
	// Kept so a failed import can be retried against it. The loop is driven by kubeconfig
	// CHANGES, and the usual cause of an incomplete import — a name still held by a Cluster
	// draining after a delete — clears on its own with no kubeconfig write behind it, so
	// without this the context stays missing until the user edits or restarts.
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

// Paces the retry of an incomplete import, sized for its driving case: a name held by a
// draining Cluster, which clears once its workers stop and GC collects the row.
const importRetryInterval = 2 * time.Second

// errNameHeld means a draining Cluster still holds the context's name — not a failure, just
// an incomplete import the caller should retry against the same snapshot.
var errNameHeld = errors.New("a context's name is held by a Cluster still being deleted")

// ReconcileClusterSet aligns the kubeconfig-sourced Clusters with one snapshot. Safe to call
// any number of times: an unchanged snapshot writes nothing and triggers nothing downstream.
func (im *KubeconfigImporter) ReconcileClusterSet(ctx context.Context, cfg *api.Config) error {
	existing, err := im.coreClient.List(ctx)
	if err != nil {
		return err
	}
	// A departed context's Cluster still references it, so a returning context is found here
	// and skipped — the controller revives it.
	tracked := map[string]bool{}
	for _, obj := range existing {
		if obj.DeletionRequestedAt != nil {
			continue // ignore objects being deleted
		}
		if kc := obj.Spec.Source.Kubeconfig; kc != nil {
			tracked[kc.Context] = true
		}
	}

	// Each context is isolated: contexts come out of a map, so aborting on one failure would
	// skip an ARBITRARY subset, differing run to run — the worst shape a partial import has.
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
			// The name is held by a Cluster on its way out (tracked ignores deletion-pending
			// objects so a re-created context gets a fresh record). Must not be swallowed:
			// this loop is driven by kubeconfig CHANGES, so "the next snapshot creates it"
			// can mean never — errNameHeld asks the caller to retry.
			held = append(held, name)
		default:
			errs = append(errs, fmt.Errorf("create cluster for context %q: %w", name, err))
		}
	}
	if len(held) > 0 {
		slices.Sort(held) // map order; a stable message keeps the log readable
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
