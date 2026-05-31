package graph

import "context"

// streamWithSnapshot is the shared body of the snapshot-then-stream
// subscription resolvers (settingsWatch, syncStatusWatch). It emits an
// optional initial snapshot so a new subscriber doesn't wait for the next
// change, then maps every event from sub onto the returned channel until
// ctx ends or sub closes. unsub runs exactly once on teardown; the output
// channel is closed on teardown.
//
// snapshot returns (value, ok); ok=false skips the initial emit (e.g. a
// failed store load — fall straight to live deltas rather than present a
// fabricated zero value as real state).
func streamWithSnapshot[S, G any](
	ctx context.Context,
	sub <-chan S,
	unsub func(),
	mapFn func(S) G,
	snapshot func() (G, bool),
) <-chan G {
	out := make(chan G)
	go func() {
		defer close(out)
		defer unsub()

		if v, ok := snapshot(); ok {
			select {
			case out <- v:
			case <-ctx.Done():
				return
			}
		}

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
