package graph

import (
	"context"

	"github.com/kubetail-org/kstack-app/sidecar/graph/model"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clusterdata"
	"github.com/kubetail-org/kstack-app/sidecar/internal/k8ssync"
)

// This file holds the presentation-only mapping between the clusterdata domain
// structs (and k8ssync.Cluster) and the gqlgen-generated model.* types, plus the
// channel relays the subscription resolvers use. Keeping it here lets
// schema.resolvers.go stay a thin set of one-line delegations and keeps all the
// read/query logic in internal/clusterdata.

// toGraphClusters maps a cluster-view snapshot to the GraphQL list. Kept here
// (where k8ssync is already imported) so schema.resolvers.go and the
// clustersWatch stream needn't name the view type.
func toGraphClusters(views []k8ssync.ClusterView) []*model.Cluster {
	return mapSlice(views, toGraphCluster)
}

func toGraphCluster(v k8ssync.ClusterView) *model.Cluster {
	return &model.Cluster{
		UUID:                   v.UUID,
		Name:                   v.Name,
		Context:                v.Context,
		IsCurrent:              v.IsCurrent,
		Enabled:                v.Enabled,
		Present:                v.Present,
		Cached:                 v.Cached,
		CacheBytes:             int(v.CacheBytes),
		LastSyncedAt:           int(v.LastSyncedAt),
		LastSeenInKubeconfigAt: int(v.LastSeenInKubeconfigAt),
	}
}

func toGraphPod(p clusterdata.Pod) *model.Pod {
	return &model.Pod{
		ClusterUUID: p.ClusterUUID,
		Namespace:   p.Namespace,
		Name:        p.Name,
		UID:         p.UID,
		Phase:       p.Phase,
		NodeName:    p.NodeName,
		UpdatedAt:   p.UpdatedAt,
	}
}

func toGraphService(s clusterdata.Service) *model.Service {
	return &model.Service{
		ClusterUUID: s.ClusterUUID,
		Namespace:   s.Namespace,
		Name:        s.Name,
		UID:         s.UID,
		Type:        s.Type,
		ClusterIP:   s.ClusterIP,
		UpdatedAt:   s.UpdatedAt,
	}
}

func toGraphDeployment(d clusterdata.Deployment) *model.Deployment {
	return &model.Deployment{
		ClusterUUID:   d.ClusterUUID,
		Namespace:     d.Namespace,
		Name:          d.Name,
		UID:           d.UID,
		Replicas:      d.Replicas,
		ReadyReplicas: d.ReadyReplicas,
		UpdatedAt:     d.UpdatedAt,
	}
}

func toGraphNode(n clusterdata.Node) *model.Node {
	return &model.Node{
		ClusterUUID: n.ClusterUUID,
		Name:        n.Name,
		UID:         n.UID,
		Ready:       n.Ready,
		UpdatedAt:   n.UpdatedAt,
	}
}

func toGraphEvent(e clusterdata.Event) *model.Event {
	return &model.Event{
		ClusterUUID:       e.ClusterUUID,
		UID:               e.UID,
		Type:              e.Type,
		Reason:            e.Reason,
		Message:           e.Message,
		InvolvedKind:      e.InvolvedKind,
		InvolvedNamespace: e.InvolvedNamespace,
		InvolvedName:      e.InvolvedName,
		FirstSeen:         e.FirstSeen,
		LastSeen:          e.LastSeen,
		Count:             e.Count,
	}
}

// mapSlice maps a domain slice to a model slice (always non-nil so the GraphQL
// non-null list contract holds).
func mapSlice[I, O any](in []I, fn func(I) O) []O {
	out := make([]O, 0, len(in))
	for _, v := range in {
		out = append(out, fn(v))
	}
	return out
}

// relay forwards a channel of domain snapshots into a channel of mapped model
// snapshots, closing the output when the input closes or ctx is cancelled. It is
// the subscription analogue of mapSlice. A nil input channel yields an
// immediately-closed output (the nil-tolerant / no-cache contract).
func relay[I, O any](ctx context.Context, in <-chan []I, fn func(I) O) <-chan []O {
	out := make(chan []O)
	go func() {
		defer close(out)
		if in == nil {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case snap, ok := <-in:
				if !ok {
					return
				}
				select {
				case out <- mapSlice(snap, fn):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
