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
	"github.com/google/uuid"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
)

// KubeconfigImporter watches the kubeconfig and aligns the kubeconfig-sourced
// Cluster beehive objects with each snapshot. It is the sole creator of
// kubeconfig-sourced clusters (one Cluster per kube-context):
//   - New context → create a Cluster, minting a random UUID and seeding the
//     kubeconfig observation into ClusterCoreSpec.SourceObs.
//   - Changed observation (cluster/user names, IsDefault, presence) → update
//     SourceObs.
//   - Departed context → SourceObs.IsPresent=false (never a deletion).
//   - Returning context → revive in place (same object, IsPresent=true).
//
// Because only the Cluster's own controller (ClusterCoreController) may write its
// status, the importer writes the observation into the spec (SourceObs); the
// ClusterCoreController mirrors it into ClusterCoreStatus.Source.Kubeconfig.
// A future creator (manual, cloud) is a sibling importer writing Cluster
// objects with a different Source variant — they share this one Cluster kind.
type KubeconfigImporter struct {
	cfgSource  KubeConfigSource
	coreClient beehive.Client[ClusterCoreSpec, ClusterCoreStatus]
	baseCtx    context.Context
	baseCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewKubeconfigImporter builds an importer that uses coreClient to manage
// kubeconfig-sourced Cluster objects in beehive.
func NewKubeconfigImporter(cfgSource KubeConfigSource, coreClient beehive.Client[ClusterCoreSpec, ClusterCoreStatus]) *KubeconfigImporter {
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
func (im *KubeconfigImporter) ClusterClient() beehive.Client[ClusterCoreSpec, ClusterCoreStatus] {
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
	// Index live kubeconfig-sourced Clusters by their context name.
	byContext := map[string]*beehive.Object[ClusterCoreSpec, ClusterCoreStatus]{}
	for _, obj := range existing {
		if obj.DeletionRequestedAt != nil {
			continue // ignore objects being deleted
		}
		if kc := obj.Spec.Source.Kubeconfig; kc != nil {
			byContext[kc.Context] = obj
		}
	}

	// Create or update a Cluster per context in the snapshot.
	for name, kctx := range cfg.Contexts {
		obs := KubeconfigStatus{
			Cluster:   kctx.Cluster,
			User:      kctx.AuthInfo,
			IsPresent: true,
			IsDefault: name == cfg.CurrentContext,
		}
		obj, ok := byContext[name]
		if !ok {
			if err := im.createCluster(ctx, name, obs); err != nil {
				return err
			}
			continue
		}
		if obj.Spec.SourceObs != nil && *obj.Spec.SourceObs == obs {
			continue // no change: skip so an unchanged context triggers no reconcile
		}
		spec := obj.Spec
		spec.SourceObs = &obs
		if _, err := im.coreClient.Update(ctx, obj.ID, spec); err != nil {
			return err
		}
	}

	// Orphan Clusters whose context departed (never delete — the record and its
	// owned ClusterCache survive so cache and settings outlive a vanished context).
	for name, obj := range byContext {
		if _, present := cfg.Contexts[name]; present {
			continue // still in kubeconfig
		}
		if obj.Spec.SourceObs == nil || !obj.Spec.SourceObs.IsPresent {
			continue // already orphaned: no write needed
		}
		orphaned := *obj.Spec.SourceObs
		orphaned.IsPresent = false
		orphaned.IsDefault = false
		spec := obj.Spec
		spec.SourceObs = &orphaned
		if _, err := im.coreClient.Update(ctx, obj.ID, spec); err != nil {
			return err
		}
	}
	return nil
}

// createCluster mints a new kubeconfig-sourced Cluster with a random UUID slug,
// seeded with the kubeconfig observation. The UUID is deliberately independent
// of the remote cluster's UID (unknown until the first probe).
func (im *KubeconfigImporter) createCluster(ctx context.Context, contextName string, obs KubeconfigStatus) error {
	id := ClusterID(uuid.NewString())
	_, err := im.coreClient.Create(ctx, ClusterCoreSpec{
		IsSyncEnabled: true,
		IsActive:      true,
		Source:        ClusterSource{Kubeconfig: &ClusterSourceKubeconfig{Context: contextName}},
		SourceObs:     &obs,
	}, beehive.WithSlug(ClusterSlug(id)))
	return err
}
