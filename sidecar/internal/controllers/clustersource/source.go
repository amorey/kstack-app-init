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
	"errors"

	"github.com/amorey/beehive"
	"github.com/google/uuid"

	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
)

// ClusterSourceController reconciles ClusterSource beehive objects: it creates
// a Cluster child on first encounter (minting a random UUID), updates the
// Cluster's SourceObs spec field when the source spec changes (reflecting
// presence/default/names), and lets beehive GC handle cascade deletion when
// the ClusterSource is deleted.
//
// Because only a kind's own controller can write its status, the kubeconfig
// observation (ClusterName, UserName, IsPresent, IsDefault) is stored in
// ClusterSpec.SourceObs (a spec field this controller writes via Client.Update).
// The ClusterController mirrors SourceObs into ClusterConnectionStatus.Source
// during its reconcile so the GraphQL layer reads from status as expected.
type ClusterSourceController struct {
	clusterClient beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus]
	controlClient beehive.ControllerClient[controllers.ClusterSourceObjStatus]
}

// NewClusterSourceController builds a controller that will manage Cluster
// children using clusterClient.
func NewClusterSourceController(clusterClient beehive.Client[controllers.ClusterSpec, controllers.ClusterConnectionStatus]) *ClusterSourceController {
	return &ClusterSourceController{clusterClient: clusterClient}
}

// Start stores the ControllerClient for use in Reconcile.
func (c *ClusterSourceController) Start(cl beehive.ControllerClient[controllers.ClusterSourceObjStatus]) error {
	c.controlClient = cl
	return nil
}

// Stop is a no-op — this controller owns no background goroutines.
func (c *ClusterSourceController) Stop(_ context.Context) error { return nil }

// Reconcile converges one ClusterSource object: ensures a Cluster child exists
// and keeps its SourceObs spec field current with the source spec.
func (c *ClusterSourceController) Reconcile(ctx context.Context, obj *beehive.Object[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus]) (beehive.Result, error) {
	if obj.DeletionRequestedAt != nil {
		// Beehive GC cascades deletion to owned Cluster and transitively to
		// ClusterCache. No finalizers to clear.
		return beehive.Result{}, nil
	}

	// --- Ensure a Cluster child exists ---
	clusterID, created, err := c.ensureClusterChild(ctx, obj)
	if err != nil {
		return beehive.Result{}, err
	}

	// --- Update the Cluster's SourceObs spec field ---
	if !created {
		if err := c.syncSourceObs(ctx, clusterID, obj.Spec); err != nil {
			return beehive.Result{}, err
		}
	}

	// Persist ClusterID in our own status once after creating the child.
	if obj.Status == nil || obj.Status.ClusterID == nil {
		return beehive.Result{}, c.controlClient.UpdateStatus(ctx, obj.ID, obj.Generation, controllers.ClusterSourceObjStatus{
			ClusterID: &clusterID,
		})
	}
	return beehive.Result{}, nil
}

// ensureClusterChild creates the Cluster child if it does not already exist,
// returning the child's ClusterID (UUID) and whether it was just created.
func (c *ClusterSourceController) ensureClusterChild(ctx context.Context, obj *beehive.Object[controllers.ClusterSourceSpec, controllers.ClusterSourceObjStatus]) (controllers.ClusterID, bool, error) {
	if obj.Status != nil && obj.Status.ClusterID != nil {
		return *obj.Status.ClusterID, false, nil
	}

	id := controllers.ClusterID(uuid.NewString())
	obs := controllers.KubeconfigStatus{
		Cluster:   obj.Spec.ClusterName,
		User:      obj.Spec.UserName,
		IsPresent: obj.Spec.IsPresent,
		IsDefault: obj.Spec.IsDefault,
	}
	_, err := c.clusterClient.Create(ctx, controllers.ClusterSpec{
		IsSyncEnabled: true,
		IsActive:      true,
		Source: controllers.ClusterSource{
			Kubeconfig: &controllers.ClusterSourceKubeconfig{
				Context: obj.Spec.ContextName,
			},
		},
		SourceObs: &obs,
	}, beehive.WithSlug(controllers.ClusterSlug(id)), beehive.WithOwner(obj.ID))
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// syncSourceObs updates ClusterSpec.SourceObs to reflect the current source
// spec — a no-op when nothing changed.
func (c *ClusterSourceController) syncSourceObs(ctx context.Context, clusterID controllers.ClusterID, spec controllers.ClusterSourceSpec) error {
	clusterObj, err := c.clusterClient.GetBySlug(ctx, controllers.ClusterSlug(clusterID))
	if err != nil {
		if errors.Is(err, beehive.ErrNotFound) {
			return nil // GC race; next reconcile re-creates the child
		}
		return err
	}

	desired := controllers.KubeconfigStatus{
		Cluster:   spec.ClusterName,
		User:      spec.UserName,
		IsPresent: spec.IsPresent,
		IsDefault: spec.IsDefault,
	}
	if clusterObj.Spec.SourceObs != nil && *clusterObj.Spec.SourceObs == desired {
		return nil // no change
	}

	updated := clusterObj.Spec
	updated.SourceObs = &desired
	_, err = c.clusterClient.Update(ctx, clusterObj.ID, updated)
	return err
}
