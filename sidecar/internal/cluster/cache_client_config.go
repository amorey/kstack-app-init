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
	"sync"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/flowcontrol"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/cache/kubesync"
)

const (
	// cacheClientQPS/cacheClientBurst bound the request rate of ONE cache's own traffic —
	// its per-kind syncs and its discovery pass — across every client it builds.
	//
	// client-go defaults to 5 QPS / 10 burst when a config leaves QPS at zero, which every
	// kubeconfig-derived config does. That default is sized for a controller watching a
	// handful of kinds, and it throttles this cache's shape of work badly: a cold start
	// issues at least one LIST per discovered kind — a hundred or more, each paginated into
	// further requests — behind one discovery walk, so the limiter, not the API server,
	// becomes what a first sync waits on (and client-go logs a "Waited for …" warning per
	// throttled request while it happens).
	//
	// The ceiling sits deliberately ABOVE cacheListConcurrency, so the LIST semaphore is
	// what shapes the cold-start burst and this only catches a genuine runaway. The two
	// live in one file for that reason — raise them together or that stops being true.
	cacheClientQPS   = 50
	cacheClientBurst = 100

	// cacheListConcurrency bounds how many of ONE cache's kinds may be inside their LIST
	// phase at once (kubesync.ListLimiter). It is a different axis from gvrSyncConcurrency,
	// which bounds reconciles: a reconcile only starts a worker and returns, so after the
	// startup pass every kind's worker runs concurrently and all of them go straight to a
	// full LIST on a cold cache. Without this bound the peak page-decoding memory and the
	// API-server burst scale with the kind count.
	cacheListConcurrency = 16
)

// cacheClientPolicy is the budget one cache's traffic shares: the token bucket every
// client it builds draws from, and the LIST-phase semaphore its workers contend for.
//
// Both are per cache rather than process-wide so one unresponsive cluster can't starve
// another's, and both are shared across that cache's ~150 kind workers plus its discovery
// pass — which is the only reason either is a bound at all.
type cacheClientPolicy struct {
	rateLimiter flowcontrol.RateLimiter
	listLimiter kubesync.ListLimiter
}

func newCacheClientPolicy() *cacheClientPolicy {
	return &cacheClientPolicy{
		rateLimiter: flowcontrol.NewTokenBucketRateLimiter(cacheClientQPS, cacheClientBurst),
		listLimiter: kubesync.NewListLimiter(cacheListConcurrency),
	}
}

// config returns the rest config a client for this cache is built from: the cluster's
// credentials, this cache's shared rate limiter, and the idle-read watchdog.
//
// It sets RateLimiter rather than QPS/Burst, and that distinction is the whole point:
// client-go builds a FRESH token bucket per REST client whenever RateLimiter is nil, so
// QPS fields would hand each of a cache's clients — two per kind worker (dynamic +
// metadata), plus discovery, so ~300 of them — its own private budget and bound nothing in
// aggregate. Handing every client the same limiter is what makes this a per-cache ceiling.
//
// It COPIES rather than mutating, because the config it is handed is the
// ConnectionManager's, shared with the connection controller's probe/health path and its
// liveness sentinel — a few requests per cluster per cadence plus one long-lived watch,
// which has no reason to draw on the sync's budget. The copy costs nothing extra: client-go
// keys its transport cache on the TLS/auth fields, which the copy leaves untouched, so the
// copy itself never splits a pool. Neither does the Wrap below — wrapping happens around
// whatever transport the cache returned.
//
// Pool sharing is NOT universal, though, and it is a property of the kubeconfig rather than
// of anything here: client-go cannot compare proxy funcs, so a config carrying a Proxy (a
// context with proxy-url) is uncacheable and every client built from it gets its own
// transport. For such a cluster a cache's ~300 clients mean ~300 pools. The rate limiter
// above still bounds the REQUEST rate, which is what protects the API server; what is lost
// is connection reuse.
func (p *cacheClientPolicy) config(cfg *rest.Config) *rest.Config {
	out := rest.CopyConfig(cfg)
	out.RateLimiter = p.rateLimiter
	// Cancel a request that stalls mid-flight. Watches are exempt (see the wrapper), so
	// this covers exactly the request shapes that can wedge while holding something
	// scarce: a LIST page holding a listLimiter slot, or a discovery request against a
	// registered-but-unreachable aggregated APIService holding a reconcile worker.
	out.Wrap(newIdleTimeoutWrapper(cacheListIdleTimeout))
	return out
}

// cacheClientPolicies hands out one policy per cache, shared by every controller that
// talks to that cache's cluster on its behalf — the per-kind sync workers and the
// discovery pass. It lives on the controllerRuntime because those are two different
// controllers that must draw on ONE budget.
//
// Entries are never reclaimed. They are bounded by the caches this process has opened
// (tens), each holds two small structs, and dropping one while a worker was about to take
// its slot would hand that worker a private budget — the same reasoning as the core
// controller's per-cluster lock map.
type cacheClientPolicies struct {
	mu      sync.Mutex
	byCache map[int64]*cacheClientPolicy
}

func newCacheClientPolicies() *cacheClientPolicies {
	return &cacheClientPolicies{byCache: make(map[int64]*cacheClientPolicy)}
}

// get returns the policy for one cache, creating it on first use.
func (r *cacheClientPolicies) get(cacheID int64) *cacheClientPolicy {
	r.mu.Lock()
	defer r.mu.Unlock()
	if p, ok := r.byCache[cacheID]; ok {
		return p
	}
	p := newCacheClientPolicy()
	r.byCache[cacheID] = p
	return p
}
