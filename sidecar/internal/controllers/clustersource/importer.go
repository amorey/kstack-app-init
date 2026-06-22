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

package clustersource

import (
	"context"
	"log/slog"
	"sync"

	"github.com/amorey/beehive"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8shelpers"
)

// KubeconfigImporter watches the kubeconfig and aligns the ClusterSource
// beehive record set with each snapshot:
//   - New context → create ClusterSource with slug "sources/kubeconfig/{name}".
//   - Changed observation (cluster/user names, IsDefault) → update the spec.
//   - Departed context → update IsPresent=false (never a deletion).
//   - Returning context → revive in place (same slug, update back to IsPresent=true).
//
// UUID minting and Cluster child lifecycle live in ClusterSourceController, not
// here. The importer only writes ClusterSource specs; the controller reconciles
// the downstream effects.
type KubeconfigImporter struct {
	cfgSource  controllers.KubeConfigSource
	srcClient  beehive.Client[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus]
	baseCtx    context.Context
	baseCancel context.CancelFunc
	wg         sync.WaitGroup
}

// NewKubeconfigImporter builds an importer that uses srcClient to manage
// ClusterSource objects in beehive.
func NewKubeconfigImporter(cfgSource controllers.KubeConfigSource, srcClient beehive.Client[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus]) *KubeconfigImporter {
	ctx, cancel := context.WithCancel(context.Background())
	return &KubeconfigImporter{
		cfgSource:  cfgSource,
		srcClient:  srcClient,
		baseCtx:    ctx,
		baseCancel: cancel,
	}
}

// SourceClient exposes the underlying beehive client for tests that need to
// inspect the ClusterSource objects after a reconcile.
func (im *KubeconfigImporter) SourceClient() beehive.Client[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus] {
	return im.srcClient
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

// ReconcileClusterSet aligns the ClusterSource beehive objects with one
// kubeconfig snapshot. It is safe to call at any time, any number of times: an
// unchanged snapshot writes nothing, so it triggers nothing downstream.
func (im *KubeconfigImporter) ReconcileClusterSet(ctx context.Context, cfg *api.Config) error {
	existing, err := im.srcClient.List(ctx)
	if err != nil {
		return err
	}
	// Index present ClusterSources by context name.
	byContext := map[string]*beehive.Object[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus]{}
	for _, obj := range existing {
		if obj.DeletionRequestedAt != nil {
			continue // ignore objects being deleted
		}
		byContext[obj.Spec.ContextName] = obj
	}

	// Create or update a ClusterSource per context in the snapshot.
	for name, kctx := range cfg.Contexts {
		desired := controllers.ClusterSourceSpec{
			ContextName: name,
			ClusterName: kctx.Cluster,
			UserName:    kctx.AuthInfo,
			IsPresent:   true,
			IsDefault:   name == cfg.CurrentContext,
		}
		if existing, ok := byContext[name]; ok && existing.Spec == desired {
			continue // no change: skip so an unchanged context triggers no reconcile
		}
		// Create the ClusterSource (deterministic slug) or update the one already
		// carrying that slug — CreateOrUpdate folds both into one atomic upsert.
		if _, err := im.srcClient.CreateOrUpdate(ctx, controllers.ClusterSourceSlug(name), desired); err != nil {
			return err
		}
	}

	// Orphan ClusterSources whose context departed.
	for name, obj := range byContext {
		if _, present := cfg.Contexts[name]; present {
			continue // still in kubeconfig
		}
		if !obj.Spec.IsPresent {
			continue // already orphaned: no write needed
		}
		orphaned := obj.Spec
		orphaned.IsPresent = false
		orphaned.IsDefault = false
		if _, err := im.srcClient.Update(ctx, obj.ID, orphaned); err != nil {
			return err
		}
	}
	return nil
}
