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

// defaultEventLimit bounds an events read (or watch snapshot) when the caller
// gives no explicit limit.
const defaultEventLimit = 50

// eventClient is the event-log slice of a beehive kind client; every kind client
// satisfies it, so one reader serves every kind's events surface.
type eventClient interface {
	ListEvents(ctx context.Context, id beehive.ObjectID, opts ...beehive.EventOption) ([]beehive.Event, error)
	WatchEvents(ctx context.Context, id beehive.ObjectID, opts ...beehive.EventOption) (*beehive.EventStream, error)
}

// eventOpts builds the beehive event options: optional category filter + limit.
func eventOpts(category *string, limit int) []beehive.EventOption {
	opts := []beehive.EventOption{beehive.WithEventLimit(limit)}
	if category != nil {
		opts = append(opts, beehive.WithEventCategory(*category))
	}
	return opts
}

// toDomainEvent maps one beehive run to the wire shape.
func toDomainEvent(e beehive.Event) domain.Event {
	return domain.Event{
		ID:       domain.ObjectID(e.ID),
		Category: e.Category,
		Type:     e.Type,
		Reason:   e.Reason,
		Message:  e.Message,
		Count:    e.Count,
		FirstAt:  e.FirstAt,
		LastAt:   e.LastAt,
	}
}

// events reads one object's event timeline (newest first; nil/<=0 limit uses
// defaultEventLimit). A read error yields nil, not an error — a partial status
// read beats none.
func (s *Service) events(ctx context.Context, c eventClient, id beehive.ObjectID, category *string, limit *int) ([]domain.Event, error) {
	n := defaultEventLimit
	if limit != nil && *limit > 0 {
		n = *limit
	}
	evs, err := c.ListEvents(ctx, id, eventOpts(category, n)...)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("clusterservice: list events", "object", id, "category", category, "err", err)
		}
		return nil, nil
	}
	out := make([]domain.Event, 0, len(evs))
	for _, e := range evs {
		out = append(out, toDomainEvent(e))
	}
	return out, nil
}

// watchEvents streams one object's event log as bare runs: snapshot runs first,
// then growth. beehive conflates per run id, so the consumer upserts by Event.ID —
// no add/modify classification. The stream's terminal error is logged.
func (s *Service) watchEvents(ctx context.Context, c eventClient, id beehive.ObjectID, category *string) (<-chan domain.Event, error) {
	stream, err := c.WatchEvents(ctx, id, eventOpts(category, defaultEventLimit)...)
	if err != nil {
		return nil, err
	}
	out := make(chan domain.Event, 1)
	go func() {
		defer close(out)
		emit := func(e beehive.Event) bool { return send(ctx, out, toDomainEvent(e)) }
		for _, e := range stream.Runs {
			if !emit(e) {
				return
			}
		}
		for {
			select {
			case <-ctx.Done():
				return
			case e, ok := <-stream.Events:
				if !ok {
					if err := stream.Err(); err != nil && ctx.Err() == nil {
						slog.Warn("clusterservice: event watch ended", "object", id, "err", err)
					}
					return
				}
				if !emit(e) {
					return
				}
			}
		}
	}()
	return out, nil
}
