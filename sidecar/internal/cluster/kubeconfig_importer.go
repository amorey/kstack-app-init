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
	"log/slog"
	"sync"

	"github.com/amorey/beehive"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
)

// KubeconfigImporter watches the kubeconfig and is the sole creator of
// kubeconfig-sourced Cluster beehive objects (one per kube-context). Creation-only:
// on each snapshot it creates a Cluster — keyed by the deterministic slug
// "kubeconfig/{context}" — for every context no Cluster yet references, writing only
// the source reference (ClusterSpec.Source). It never updates, orphans, or deletes.
// The deterministic slug means beehive's per-kind slug-uniqueness rules out a
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
	for {
		select {
		case <-im.baseCtx.Done():
			return
		case cfg, ok := <-sub.Chan():
			if !ok {
				return
			}
			if err := im.ReconcileClusterSet(im.baseCtx, cfg); err != nil {
				slog.Error("kubeconfig import failed", "err", err)
			}
		}
	}
}

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
	for name := range cfg.Contexts {
		if tracked[name] {
			continue
		}
		if err := im.createCluster(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// createCluster creates a kubeconfig-sourced Cluster keyed by the deterministic
// slug "kubeconfig/{context}", writing only the source reference. The slug is the
// context's natural key (the importer's reconcile/uniqueness key); the record's
// identity is the ObjectID beehive assigns, not this slug.
func (im *KubeconfigImporter) createCluster(ctx context.Context, contextName string) error {
	_, err := im.coreClient.Create(ctx, ClusterSpec{
		SyncEnabled: true,
		Enabled:     true,
		Source:      ClusterSpecSource{Kubeconfig: &ClusterSpecSourceKubeconfig{Context: contextName}},
	}, beehive.WithSlug(kubeconfigSlug(contextName)))
	return err
}
