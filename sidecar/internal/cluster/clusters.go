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

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// List implements Clusters.
func (a clustersAPI) List(ctx context.Context) ([]*domain.Cluster, error) {
	return a.s.listClusters(ctx)
}

// listClusters reads every non-deletion-pending Cluster as a domain Cluster.
// Shared by List and Watch's seed + re-emit.
func (s *Service) listClusters(ctx context.Context) ([]*domain.Cluster, error) {
	objs, err := s.coreClient.List(ctx)
	if err != nil {
		return nil, err
	}
	clusters := make([]*domain.Cluster, 0, len(objs))
	for _, obj := range objs {
		if obj.DeletionRequestedAt != nil {
			continue
		}
		c := s.buildCluster(obj)
		clusters = append(clusters, &c)
	}
	return clusters, nil
}

// Get implements Clusters. An untracked or deletion-pending id is (nil, nil),
// not an error.
func (a clustersAPI) Get(ctx context.Context, id domain.ClusterID) (*domain.Cluster, error) {
	obj, err := a.s.coreClient.Get(ctx, beehive.ObjectID(id))
	if errors.Is(err, beehive.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if obj.DeletionRequestedAt != nil {
		return nil, nil
	}
	c := a.s.buildCluster(obj)
	return &c, nil
}

// Watch implements Clusters: beehive's Cluster-kind WatchList as a delta stream
// (conflated per object, so a slow client converges). Per-probe chatter, the
// countdown, and cache sync status deliberately stream elsewhere (WatchEvents /
// WatchSchedule / Caches().Watch), so a settled disconnected cluster produces
// no churn here.
func (a clustersAPI) Watch(ctx context.Context) (<-chan domain.ClusterChange, error) {
	snap, src, err := a.s.coreClient.WatchList(ctx)
	if err != nil {
		return nil, err
	}
	return watchListChan(ctx, "Cluster", snap, src,
		func(t domain.ChangeType, id beehive.ObjectID, obj *beehive.Object[domain.ClusterSpec, domain.ClusterStatus]) domain.ClusterChange {
			if t == domain.ChangeBookmark {
				return domain.ClusterChange{Type: t}
			}
			if obj == nil {
				return domain.ClusterChange{Type: t, Cluster: &domain.Cluster{ID: domain.ClusterID(id)}}
			}
			c := a.s.buildCluster(obj)
			return domain.ClusterChange{Type: t, Cluster: &c}
		}), nil
}

// ListEvents implements Clusters — the Cluster-kind entrypoint to the generic
// event reader.
func (a clustersAPI) ListEvents(ctx context.Context, id domain.ClusterID, category *string, limit *int) ([]domain.Event, error) {
	return a.s.events(ctx, a.s.coreClient, beehive.ObjectID(id), category, limit)
}

// WatchEvents implements Clusters — the Cluster-kind entrypoint to the generic
// event watch.
func (a clustersAPI) WatchEvents(ctx context.Context, id domain.ClusterID, category *string) (<-chan domain.Event, error) {
	return a.s.watchEvents(ctx, a.s.coreClient, beehive.ObjectID(id), category)
}

// scheduleClient is the schedule-gauge slice of a beehive kind client; tests fake it.
type scheduleClient interface {
	WatchSchedule(ctx context.Context, id beehive.ObjectID) (<-chan beehive.Schedule, error)
}

// toSchedule maps the beehive Schedule gauge to the domain view (zero time → nil).
func toSchedule(s beehive.Schedule) domain.Schedule {
	if s.NextRequeueAt.IsZero() {
		return domain.Schedule{}
	}
	at := s.NextRequeueAt
	return domain.Schedule{NextRequeueAt: &at}
}

// scheduleWatch streams one object's reconcile-schedule gauge — the live source
// for the next-attempt countdown, since a scheduling change fires no WatchList.
func (s *Service) scheduleWatch(ctx context.Context, c scheduleClient, id beehive.ObjectID) (<-chan domain.Schedule, error) {
	src, err := c.WatchSchedule(ctx, id)
	if err != nil {
		return nil, err
	}
	return mapChan(ctx, src, toSchedule), nil
}

// WatchSchedule implements Clusters: the schedule gauge merged with the
// controller's in-flight probe signal — the scheduler owns NextRequeueAt, the
// core controller owns Probing; both are current-on-subscribe.
func (a clustersAPI) WatchSchedule(ctx context.Context, id domain.ClusterID) (<-chan domain.Schedule, error) {
	schedSrc, err := a.s.scheduleWatch(ctx, a.s.coreClient, beehive.ObjectID(id))
	if err != nil {
		return nil, err
	}
	probeSrc := a.s.coreCtrl.WatchProbe(ctx, id)
	return mergeSchedule(ctx, schedSrc, probeSrc), nil
}

// mergeSchedule folds the schedule gauge and the probe signal into one Schedule
// stream, re-emitting the combined latest as either side moves. A closed
// sub-source is nil'd, not stream-ending; out closes when both close or ctx ends.
func mergeSchedule(ctx context.Context, schedSrc <-chan domain.Schedule, probeSrc <-chan bool) <-chan domain.Schedule {
	out := make(chan domain.Schedule, 1)
	go func() {
		defer close(out)
		var cur domain.Schedule
		emit := func() bool { return send(ctx, out, cur) }
		for schedSrc != nil || probeSrc != nil {
			select {
			case <-ctx.Done():
				return
			case sc, ok := <-schedSrc:
				if !ok {
					schedSrc = nil
					continue
				}
				cur.NextRequeueAt = sc.NextRequeueAt
				if !emit() {
					return
				}
			case p, ok := <-probeSrc:
				if !ok {
					probeSrc = nil
					continue
				}
				cur.Probing = p
				if !emit() {
					return
				}
			}
		}
	}()
	return out
}

// updateSpec applies mutate to a copy of the cluster's spec and persists it. The
// spec write bumps the generation, so the connection + cache controllers
// re-reconcile and the change propagates to watchers. It backs the spec-toggle
// mutations below.
func (s *Service) updateSpec(ctx context.Context, id domain.ClusterID, mutate func(*domain.ClusterSpec)) (*domain.Cluster, error) {
	obj, err := s.clusterByID(ctx, id)
	if err != nil {
		return nil, err
	}
	spec := obj.Spec
	mutate(&spec)
	updated, err := s.coreClient.Update(ctx, obj.ID, spec)
	if err != nil {
		return nil, err
	}
	c := s.buildCluster(updated)
	return &c, nil
}

// SetEnabled implements Clusters.
func (a clustersAPI) SetEnabled(ctx context.Context, id domain.ClusterID, enabled bool) (*domain.Cluster, error) {
	return a.s.updateSpec(ctx, id, func(spec *domain.ClusterSpec) { spec.Enabled = enabled })
}

// SetSyncEnabled implements Clusters.
func (a clustersAPI) SetSyncEnabled(ctx context.Context, id domain.ClusterID, enabled bool) (*domain.Cluster, error) {
	return a.s.updateSpec(ctx, id, func(spec *domain.ClusterSpec) { spec.SyncEnabled = enabled })
}

// Delete implements Clusters. Beehive GC cascades to the ClusterCache; a
// still-present kube-context is re-created by the importer under the same
// "kubeconfig/{context}" name.
func (a clustersAPI) Delete(ctx context.Context, id domain.ClusterID) error {
	obj, err := a.s.clusterByID(ctx, id)
	if err != nil {
		return err
	}
	return a.s.coreClient.Delete(ctx, obj.ID)
}

// buildCluster assembles a domain Cluster from one beehive object. Cache children
// are not joined (they stream standalone via Caches().Watch), so no ctx-bound reads.
func (s *Service) buildCluster(obj *beehive.Object[domain.ClusterSpec, domain.ClusterStatus]) domain.Cluster {
	c := domain.Cluster{
		ID:         domain.ClusterID(obj.ID),
		Generation: obj.Generation,
		CreatedAt:  obj.CreatedAt,
		Spec:       obj.Spec,
		// Already liveness-downgraded by the store: a previous process's
		// Connected=True arrives here as Unknown.
		Conditions: obj.Conditions,
	}
	if obj.DeletionRequestedAt != nil {
		t := *obj.DeletionRequestedAt
		c.DeletionRequestedAt = &t
	}
	c.Status = derefOrZero(obj.Status)
	return c
}

// derefOrZero returns *p, or the zero value when nil. beehive leaves Status nil until first
// written, while domain records serve it by value — absent and zeroed say the same thing.
func derefOrZero[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

// mapChan streams src through fn until src closes or ctx ends (out closes on exit).
func mapChan[A, B any](ctx context.Context, src <-chan A, fn func(A) B) <-chan B {
	out := make(chan B, 1)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-src:
				if !ok {
					return
				}
				select {
				case out <- fn(v):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
