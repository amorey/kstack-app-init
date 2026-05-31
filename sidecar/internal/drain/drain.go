// Package drain provides the one shutdown primitive the sidecar's lifecycle
// types share: wait for some in-flight work to finish, but no longer than a
// caller's deadline.
package drain

import "context"

// WithContext runs wait in a goroutine and blocks until it returns (nil) or ctx
// is done (ctx.Err()). wait must eventually return on its own — typically a
// WaitGroup.Wait whose handlers were already signalled to stop — since there is
// no way to interrupt it; ctx only bounds how long the caller waits, not wait
// itself. Used by graph.Server and grpcserver.Server to drain their streams.
func WithContext(ctx context.Context, wait func()) error {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
