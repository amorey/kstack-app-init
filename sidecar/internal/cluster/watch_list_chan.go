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

	"github.com/amorey/beehive"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// watchListChan folds a beehive kind watch (snapshot + change stream) into one
// Kubernetes-style delta stream, closing the snapshot with a single Bookmark so a
// consumer can tell an empty collection from one still arriving. A deletion-pending
// object is collapsed to Deleted (List/Get hide tombstones, so the watch removes it
// at once; the trailing hard Deleted repeats idempotently). A stream that fails ends
// after a log line, its reason read off stream.Err(). Out closes on exit.
//
// fn is called with a nil obj two ways, and must tell them apart: on DeltaFrameBookmark
// (once, between snapshot and deltas) it must return a frame carrying NO entity; on a
// Deleted whose final state could not be decoded it returns the removal by id alone.
func watchListChan[Spec, Status, Out any](
	ctx context.Context,
	kind string,
	stream *beehive.ObjectListStream[Spec, Status],
	fn func(domain.DeltaFrameType, beehive.ObjectID, *beehive.Object[Spec, Status]) Out,
) <-chan Out {
	out := make(chan Out, 1)
	go func() {
		defer close(out)
		// beehive.ChangeType and domain.DeltaFrameType share string values by construction.
		domainType := func(t beehive.ChangeType, obj *beehive.Object[Spec, Status]) domain.DeltaFrameType {
			if obj != nil && obj.DeletionRequestedAt != nil {
				return domain.DeltaFrameDeleted
			}
			return domain.DeltaFrameType(t)
		}
		for _, obj := range stream.Objects {
			if !send(ctx, out, fn(domainType(beehive.Added, obj), obj.ID, obj)) {
				return
			}
		}
		// Everything from here is a live change, not initial state.
		if !send(ctx, out, fn(domain.DeltaFrameBookmark, 0, nil)) {
			return
		}
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-stream.Changes:
				if !ok {
					if err := stream.Err(); err != nil && ctx.Err() == nil {
						slog.Warn("clusterservice: object watch ended", "kind", kind, "err", err)
					}
					return
				}
				if !send(ctx, out, fn(domainType(ev.Type, ev.Object), ev.ID, ev.Object)) {
					return
				}
			}
		}
	}()
	return out
}
