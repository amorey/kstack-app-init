package server

import (
	"context"
	"net"
	"net/http"
	"sync"
)

// AttachGracefulShutdown wires srv so that hijacked WebSocket connections
// receive a clean Close frame when Shutdown begins.
//
// Mechanism: gqlgen's WS transport waits on the request context inside
// `closeOnCancel`, which sends `CloseNormalClosure` when the context
// cancels (websocket.go:374-381 in gqlgen v0.17.90). Setting BaseContext
// to a cancellable context and firing it from RegisterOnShutdown delivers
// the cancel to every active subscription before the server stops.
//
// Returns a handler wrapper that tracks in-flight requests, plus a wait
// function the caller must invoke after Shutdown returns — http.Server
// doesn't drain hijacked connections itself (see net/http docs for
// Server.Shutdown).
func AttachGracefulShutdown(srv *http.Server, handler http.Handler) (http.Handler, func()) {
	wsCtx, wsCancel := context.WithCancel(context.Background())
	srv.BaseContext = func(_ net.Listener) context.Context { return wsCtx }
	srv.RegisterOnShutdown(wsCancel)

	var inflight sync.WaitGroup
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inflight.Add(1)
		defer inflight.Done()
		handler.ServeHTTP(w, r)
	})

	return wrapped, inflight.Wait
}
