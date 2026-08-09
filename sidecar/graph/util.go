package graph

import (
	"context"
	"time"
)

// nilIfZeroTime returns nil for the zero time (an absent timestamp) and a pointer
// to t otherwise — the value→nullable mapping shared by the ClusterDataEvent
// (firstSeen/lastSeen) and ClusterDataObject (creationTimestamp) field resolvers,
// whose domain types keep value time.Time (comparable, required by the delta-watch
// diff) but serialize an absent timestamp as null rather than 0001-01-01.
func nilIfZeroTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// mapStream maps every value from a latest-value source channel onto the
// returned channel until ctx ends or sub closes. It emits no separate snapshot
// — the source is already current-on-subscribe (a watch hub, e.g. the auth
// session stream), so its first value IS the snapshot. unsub runs exactly once
// on teardown; the output channel is closed on teardown.
func mapStream[S, G any](
	ctx context.Context,
	sub <-chan S,
	unsub func(),
	mapFn func(S) G,
) <-chan G {
	out := make(chan G)
	go func() {
		defer close(out)
		defer unsub()

		for {
			select {
			case <-ctx.Done():
				return
			case s, ok := <-sub:
				if !ok {
					return
				}
				select {
				case out <- mapFn(s):
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// ptrSlice maps a value slice onto a slice of pointers into it — the kube
// registry hands out []T snapshots while gqlgen's bindings want []*T.
func ptrSlice[T any](items []T) []*T {
	out := make([]*T, len(items))
	for i := range items {
		out[i] = &items[i]
	}
	return out
}

// ptrStream maps a value stream onto a stream of pointers — the stream counterpart of
// ptrSlice, and what nearly every cluster subscription resolver needs: the service hands
// back a <-chan T while gqlgen's bindings want <-chan *T. The source is already
// current-on-subscribe, so there is nothing to unsubscribe and no snapshot to prepend.
func ptrStream[T any](ctx context.Context, sub <-chan T) <-chan *T {
	return mapStream(ctx, sub, func() {}, func(v T) *T { return &v })
}
