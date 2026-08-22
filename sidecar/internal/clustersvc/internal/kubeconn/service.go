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

// Package kubeconn holds the connections everything in this service talks to a cluster over,
// and the probe that keeps them validated.
//
// **Credentials are the unit, not clusters.** What is pooled is one connection per credential
// key, so two kube-contexts aimed at one server as one user are one socket and one probe.
// Nothing in the vocabulary of a cluster record says so: a reader may not assume one entry per
// cluster.
//
// **A connection is scoped to one identity.** Credentials that move arrive under a new key and
// build a new one; an unchanged key answering as a different server, version, or user retires
// the connection and builds another. So Identity is a field rather than a question a holder has
// to keep re-asking.
//
// **It never learns what a cluster is.** The caller names a kube-context; this package speaks
// contexts, credentials, and whatever a probe read back. Whether the server that answered is
// the one the caller meant is the caller's to decide, off Connection.Identity.
//
// **Everything a holder learns comes through its Lease** — which is why the pool needs no index
// from a credential key back out to the contexts sharing it.
//
// **Nothing dials yet.** The lifecycle is settled ahead of anything depending on it; the pool
// and the probe behind Acquire land next.
package kubeconn

import (
	"context"
	"net/http"
	"net/url"

	"github.com/amorey/gobus/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

// kubeconfigService resolves one context to credentials and the key naming them. The key
// excludes the context name, so two contexts aimed at one server as one user share an entry.
type kubeconfigService interface {
	RESTConfig(contextName string) (*rest.Config, string, error)
}

// Identity is what a probe learned about the far end: which server answered, running what, and
// as whom. Every field is optional — a probe that reached the API server succeeded, and reports
// whatever this user was allowed to read.
//
// Comparable, which is what lets a probe tell an unchanged answer from one worth acting on. So
// what belongs here names the far end and nothing about the reading of it — a field that breaks
// comparability puts every caller back on a hand-written helper.
type Identity struct {
	ServerUID     string
	ServerVersion string
	Username      string
}

// Result is one probe's outcome. It names no credentials of its own: it is stored under the
// key it ran against, so a rotation cannot be told the wrong cluster's news.
type Result struct {
	Identity Identity
	// UIDErr is why Identity.ServerUID is empty, when a response came back saying so. An error
	// rather than a bool because 403 (no RBAC on kube-system) and 404 (no such namespace) are
	// different news. Here rather than on Identity: it flaps under a grant and revoke, and what
	// a probe could read is not who answered.
	UIDErr error
	// Err is why the server would not answer at all. A single refused request leaves its
	// Identity field empty instead — the probe reached the API server, which is the thing
	// being probed.
	Err error
}

// State is what is known about one claim: the last probe's outcome, and whether one is
// running. Read together, since a caller deciding what to report must not have a probe finish
// between two reads and conclude none ever ran.
type State struct {
	// Last is the last probe's outcome, or nil when none has landed. Nil is "no news", never
	// "no identity" — credentials nobody has probed read the same as ones nobody has asked
	// about, and a caller folds both as "keep what you have".
	Last *Result
	// Probing with a nil Last is the one state worth reporting on its own: these credentials
	// are being tried for the first time since anything asked.
	Probing bool
}

// StateSubscription carries a claim's State: the current one on attach, then each one that says
// something new. Close it when done — an abandoned one keeps its slot. Its key is the
// credentials the probe ran against, which a holder of one claim has no use for.
//
// Current-on-attach is what makes it race-free: a watcher that read State separately would owe an
// ordering nothing enforces, and a probe landing between the two calls is the one it never hears
// about.
//
// **Every value is a level, never an edge.** The hub keeps the latest per key, so a reader that
// falls behind skips what came between — a gap means it missed some, not that nothing happened.
// Transitions are derived from the record's conditions and event timeline, never counted here.
type StateSubscription = *watch.Receiver[string, State]

// Lease is a claim on one connection, and what keeps its probe running: credentials nobody
// holds are not probed at all. An interface because it crosses out of this package, where a
// caller's test has to stand in for a live cluster.
type Lease interface {
	// Conn is the connection, once a probe has validated it.
	//
	// Credentials whose last probe succeeded answer immediately. Anything else waits for the
	// probe now running and returns *its* outcome, not a successful one, so a down cluster
	// answers rather than hanging and the caller decides whether to ask again. It does not
	// kick: the claim already has the loop probing, on the backoff ladder while the cluster
	// is down, and kicking per call would flatten it.
	Conn(ctx context.Context) (*Connection, error)
	// State is the last probe's outcome and whether one is running. It never dials.
	//
	// What the prober reads: it wants the answer itself rather than a connection, and a
	// failed probe carries a reason no connection could.
	State() State
	// WatchState carries this claim's State, current on attach and again whenever a probe says
	// something new — including a failure whose reason changed. Release ends it.
	//
	// The same hub Conn parks on, so a watcher and a parked caller cannot disagree. Per claim
	// rather than per pool, which is what spares the pool an index from a credential key back
	// out to the contexts sharing it.
	WatchState() StateSubscription
	// Release drops the claim, and with it the probe cadence once nothing else holds these
	// credentials. Idempotent, so it is safe to defer.
	Release()
}

// Connection is one identity and the clients built over the credentials reaching it. The clients
// share one http.Client, so they share one connection pool — with HTTP/2 that is a single TCP
// connection carrying every concurrent request to that API server.
//
// Read-only, and it never changes after it is built. A caller that mutates Config is editing
// what every other holder is using.
type Connection struct {
	// Identity is what the probe that validated this connection read, stable for its life: a
	// probe reading anything different retires it. So a holder that knows which cluster it
	// means compares before it reads. The pool cannot compare — its key is credentials, which
	// do not move when a cluster is rebuilt, upgraded, or re-issues a token for another user.
	Identity Identity
	// Config is what the clients below were built from. Exposed for the client-go
	// constructors that take one and build their own transport (exec, port-forward);
	// anything that can use Dynamic or HTTPClient should.
	Config *rest.Config
	// BaseURL is Config.Host resolved to an absolute URL, carrying the scheme and any path
	// prefix; APIPath is the versioned path beside it. Raw paths join onto them.
	BaseURL *url.URL
	APIPath string
	// HTTPClient is authenticated and pooled. Raw API paths go through it directly, and
	// unthrottled: the QPS bucket lives in rest.RESTClient, so it reaches Dynamic alone.
	HTTPClient *http.Client
	Dynamic    dynamic.Interface

	// done closes when this connection is retired. Nil in a connection nobody built, which
	// reads as never retired.
	done chan struct{}
}

// Done closes when a probe reads a different Identity and this connection is retired.
//
// For the holder nothing else reaches: a long-lived stream is blocked in a read, so a field it
// is not re-reading cannot tell it that what it derived here no longer holds. A caller that asks
// per operation re-calls Lease.Conn instead.
func (c *Connection) Done() <-chan struct{} { return c.done }

// Service is the pool the cluster service dials through.
type Service struct {
	kubecfgSvc kubeconfigService
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfgSvc kubeconfigService) *Service {
	return &Service{kubecfgSvc: kubecfgSvc}
}

// Acquire claims the connection for contextName's credentials and arms their probe cadence,
// building the connection if this is the first caller. It does not dial — Lease.Conn is what
// waits — so a caller may hold a claim across a cluster being down without a retry loop of
// its own. Release the claim.
func (s *Service) Acquire(contextName string) (Lease, error) {
	panic("not implemented")
}

// Start is the lifecycle shape this wears for the cluster service. Nothing runs in the
// background, so its stop func has nothing to end.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

// Close releases the pool. Nothing is held yet.
func (s *Service) Close() error { return nil }
