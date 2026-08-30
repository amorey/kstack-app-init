package graph

import (
	"context"
)

// mapStream maps a latest-value source onto the returned channel until ctx ends or sub
// closes. No separate snapshot: the source is current-on-subscribe, so its first value IS
// the snapshot. unsub runs once on teardown, where out is also closed.
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

// ptrSlice maps a value slice onto pointers into it: the services hand out []T while
// gqlgen's bindings want []*T.
func ptrSlice[T any](items []T) []*T {
	out := make([]*T, len(items))
	for i := range items {
		out[i] = &items[i]
	}
	return out
}

// ptrStream is ptrSlice for streams, which nearly every cluster subscription resolver
// needs. The source is current-on-subscribe, so there's nothing to unsubscribe.
func ptrStream[T any](ctx context.Context, sub <-chan T) <-chan *T {
	return mapStream(ctx, sub, func() {}, func(v T) *T { return &v })
}
