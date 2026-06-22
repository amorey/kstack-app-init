package graph

import "context"

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
