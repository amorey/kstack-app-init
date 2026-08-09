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
	// cacheClientQPS/cacheClientBurst bound ONE cache's total request rate (per-kind
	// syncs + discovery). client-go's zero-QPS default of 5/10 would throttle a cold
	// start's one-LIST-per-kind burst. Deliberately ABOVE cacheListConcurrency so the
	// LIST semaphore shapes the burst and this only catches a runaway — raise them
	// together or that stops being true.
	cacheClientQPS   = 50
	cacheClientBurst = 100

	// cacheListConcurrency bounds how many of ONE cache's kinds may be in their LIST
	// phase at once (kubesync.ListLimiter). A different axis from gvrSyncConcurrency
	// (which bounds reconciles; a reconcile only starts a worker): without this, peak
	// page-decoding memory and API-server burst scale with the kind count.
	cacheListConcurrency = 16
)

// cacheClientPolicy is one cache's shared traffic budget: the token bucket every client
// it builds draws from, and the LIST-phase semaphore its workers contend for. Per cache
// (not process-wide) so one unresponsive cluster can't starve another; shared across the
// cache's ~150 kind workers plus discovery — the only reason either is a bound at all.
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
// Sets RateLimiter, NOT QPS/Burst: nil RateLimiter gives each of a cache's ~300 REST
// clients its own token bucket, bounding nothing in aggregate. COPIES rather than
// mutating — the input is the ConnectionManager's config, shared with the probe path and
// sentinel, which must not draw on the sync budget; the copy leaves the TLS/auth fields
// client-go keys its transport cache on untouched, so it never splits a pool (a
// proxy-carrying kubeconfig is uncacheable regardless — its clients get private
// transports either way). See docs/adr/2026-08-09-connection-probing.md.
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

// cacheClientPolicies hands out one policy per cache. Lives on the controllerRuntime
// because sync workers and discovery are two controllers that must draw on ONE budget.
//
// Entries are never reclaimed: bounded (tens of caches, two small structs each), and
// dropping one under a worker about to take its slot would hand it a private budget —
// same reasoning as the core controller's per-cluster lock map.
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
