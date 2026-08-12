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

package controllers

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

// A kubeconfig-derived config leaves QPS at zero, which client-go reads as its 5 QPS / 10
// burst default — sized for a controller watching a handful of kinds, not for a cache
// cold-starting a hundred-plus per-kind LISTs behind a discovery walk.
//
// The ceiling must arrive as a shared RateLimiter, not as QPS/Burst fields: client-go
// builds a fresh token bucket per REST client whenever RateLimiter is nil, so fields would
// give each of a cache's ~300 clients its own budget and bound nothing in aggregate.
func TestCacheClientPolicySharesOneRateLimiter(t *testing.T) {
	base := &rest.Config{Host: "https://example.test"}
	p := newCacheClientPolicy()

	first := p.config(base)
	second := p.config(base)

	require.NotNil(t, first.RateLimiter)
	assert.Same(t, first.RateLimiter, second.RateLimiter,
		"every client of one cache must draw on the same token bucket")
	assert.Zero(t, first.QPS, "the bucket is the limiter; a QPS field would build a second one")
	assert.Zero(t, first.Burst)
}

// The LIST semaphore is the governor and the token bucket only a runaway backstop, so the
// bucket has to sit above it. They live in one file so this stays true.
func TestCacheClientBudgetsAreOrdered(t *testing.T) {
	assert.Greater(t, cacheClientBurst, cacheListConcurrency,
		"the client-go ceiling must sit above the cache's own in-flight LIST bound")
}

// A stalled LIST holds a listLimiter slot, so the cache's clients must carry the idle-read
// watchdog that eventually releases it.
func TestCacheClientPolicyInstallsTheIdleTimeout(t *testing.T) {
	p := newCacheClientPolicy()
	got := p.config(&rest.Config{Host: "https://example.test"})

	assert.NotNil(t, got.WrapTransport, "cache clients must carry the idle-read timeout")
}

// The config handed in is the connection manager's, shared with the connection controller's
// probe/health path and its liveness sentinel — traffic with no reason to draw on the
// sync's budget. Copying, not mutating, is what keeps those independent.
func TestCacheClientPolicyDoesNotMutateTheSharedConfig(t *testing.T) {
	base := &rest.Config{Host: "https://example.test"}

	got := newCacheClientPolicy().config(base)
	require.NotSame(t, base, got)

	assert.Nil(t, base.RateLimiter, "the shared config must keep client-go's default")
	assert.Nil(t, base.WrapTransport)
}

// Pool sharing is the KUBECONFIG's to decide, not this policy's: client-go keys its
// transport cache on the TLS/auth fields (so attaching this cache's limiter never splits a
// pool) but cannot compare proxy funcs, so a context with proxy-url is uncacheable and every
// client built from it gets its own transport. The comment above config used to claim the
// sharing unconditionally; this pins what the policy actually does — carry the field through
// untouched, adding nothing and removing nothing.
func TestCacheClientPolicyCarriesTheKubeconfigsProxyThrough(t *testing.T) {
	p := newCacheClientPolicy()

	plain := p.config(&rest.Config{Host: "https://example.test"})
	assert.Nil(t, plain.Proxy, "nothing here introduces a proxy")

	proxy := func(*http.Request) (*url.URL, error) { return nil, nil }
	got := p.config(&rest.Config{Host: "https://example.test", Proxy: proxy})
	assert.NotNil(t, got.Proxy, "a context's proxy-url must survive the copy")
}

// One budget per cache: shared by that cache's kind workers AND its discovery pass (two
// different controllers), but never across caches, so one unresponsive cluster can't
// starve another's.
func TestCacheClientPoliciesAreScopedPerCache(t *testing.T) {
	r := newCacheClientPolicies()

	first := r.get(7)
	assert.Same(t, first, r.get(7), "one cache must resolve to one policy")
	assert.NotSame(t, first, r.get(8), "separate caches must not share a budget")
	assert.NotEqual(t, first.listLimiter, r.get(8).listLimiter,
		"each cache gets its own LIST semaphore, not a shared channel")
}

// Both controllers are built from one runtime and must land on the same registry — that
// sharing is the only reason the budget bounds anything.
func TestControllerRuntimeSharesOnePolicyRegistry(t *testing.T) {
	rt := &controllerRuntime{}

	first := rt.policies()
	assert.Same(t, first, rt.policies())
	assert.Same(t, first.get(1), rt.policies().get(1))
}
