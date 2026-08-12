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
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/kubetail-org/kstack-app/sidecar/internal/cluster/domain"
)

// SentinelWatchFunc establishes one long-lived liveness watch against cfg; the result
// channel closing (or establish failing) is the connection-loss signal. Tests inject a
// fake; production uses watchKubeSystem.
type SentinelWatchFunc func(ctx context.Context, cfg *rest.Config) (watch.Interface, error)

// connSentinel is the handle on one cluster's running liveness watch: the cancel for
// its goroutine, and the fingerprint it started with so a credential rotation restarts
// it rather than leaving a stale watch on expired creds.
type connSentinel struct {
	cancel      context.CancelFunc
	fingerprint string
}

// SetSentinelWatcher overrides the sentinel watch function (tests only). Call once,
// before the control plane starts.
func (c *ClusterCoreController) SetSentinelWatcher(f SentinelWatchFunc) {
	c.sentinelWatch = f
}

// ensureSentinel guarantees a liveness watch runs for a just-connected cluster. No-op
// on the same fingerprint; a fingerprint change (rotation) restarts it. Called from
// converge after a successful probe, so the sentinel exists exactly while connected.
// Nil bgCtx skips the launch — before StartBackground there's nowhere to anchor the
// goroutine, and after StopBackground it's the shutdown gate: a reconcile racing
// teardown must not sentinelWG.Add once StopBackground's Wait has begun.
func (c *ClusterCoreController) ensureSentinel(id domain.ClusterID, cfg *rest.Config, fingerprint string) {
	c.sentinelMu.Lock()
	defer c.sentinelMu.Unlock()

	if c.bgCtx == nil {
		return
	}
	if existing, ok := c.sentinels[id]; ok {
		if existing.fingerprint == fingerprint {
			return
		}
		existing.cancel() // credential rotation — drop the stale-config watch
	}

	ctx, cancel := context.WithCancel(c.bgCtx)
	s := &connSentinel{cancel: cancel, fingerprint: fingerprint}
	c.sentinels[id] = s
	c.sentinelWG.Add(1)
	go c.runSentinel(ctx, id, s, cfg)
}

// stopSentinel cancels and forgets id's liveness watch (if any) — the connection is
// gone, so its sentinel must not linger and re-probe.
func (c *ClusterCoreController) stopSentinel(id domain.ClusterID) {
	c.sentinelMu.Lock()
	defer c.sentinelMu.Unlock()
	if s, ok := c.sentinels[id]; ok {
		s.cancel()
		delete(c.sentinels, id)
	}
}

// teardownConnection drops the cached connection and stops the sentinel — the common
// cleanup for every disconnected path in converge (ineligible, resolve-fail,
// probe-fail), kept in one place so the two steps can't drift apart.
func (c *ClusterCoreController) teardownConnection(id domain.ClusterID) {
	if c.connMgr != nil {
		c.connMgr.Delete(id)
	}
	c.stopSentinel(id)
}

// runSentinel holds one long-lived watch open and fires a single out-of-band re-probe
// when it closes — the earliest loss signal (HTTP/2 keepalive tears a silently-dead
// connection down in ~15s). On success converge starts a fresh sentinel; on failure
// beehive's backoff owns the cadence and no sentinel runs while disconnected. A benign
// server-side watch timeout also closes the stream; the idempotent re-probe is cheap.
// One-shot — exits after firing, so a persistently-down cluster never spins here.
// See docs/adr/2026-08-09-connection-probing.md.
func (c *ClusterCoreController) runSentinel(ctx context.Context, id domain.ClusterID, self *connSentinel, cfg *rest.Config) {
	defer c.sentinelWG.Done()
	defer c.sentinelExited(id, self)

	w, err := c.sentinelWatch(ctx, cfg)
	if err != nil {
		// Establish failed — connection down (fireSentinelReprobe stays quiet on shutdown).
		c.fireSentinelReprobe(ctx, id, self)
		return
	}
	defer w.Stop()

	// Drain and ignore events — only the stream closing matters. Selecting on ctx.Done()
	// tears the goroutine down even if the watch implementation ignores ctx.
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-w.ResultChan():
			if !ok {
				c.fireSentinelReprobe(ctx, id, self)
				return
			}
		}
	}
}

// fireSentinelReprobe requests an out-of-band re-probe of id, unless ctx is cancelled
// (shutdown or deliberate stop). Clears the map entry first so the re-probe's converge
// sees no live sentinel — else ensureSentinel would treat the just-exited entry as
// running and skip the restart.
func (c *ClusterCoreController) fireSentinelReprobe(ctx context.Context, id domain.ClusterID, self *connSentinel) {
	if ctx.Err() != nil {
		return
	}
	c.sentinelExited(id, self)
	c.Reprobe(id)
}

// sentinelExited removes self from the map only if still current (pointer identity),
// so a replaced/cleared sentinel never deletes its successor. Idempotent.
func (c *ClusterCoreController) sentinelExited(id domain.ClusterID, self *connSentinel) {
	c.sentinelMu.Lock()
	defer c.sentinelMu.Unlock()
	if c.sentinels[id] == self {
		delete(c.sentinels, id)
	}
}

// watchKubeSystem (the production SentinelWatchFunc) opens a long-lived watch on the
// kube-system namespace — present on every cluster — purely as a liveness probe. The
// per-request timeout is cleared; liveness is left to the HTTP/2 keepalive.
func watchKubeSystem(ctx context.Context, cfg *rest.Config) (watch.Interface, error) {
	wcfg := rest.CopyConfig(cfg)
	wcfg.Timeout = 0 // long-lived stream; rely on HTTP/2 keepalive for liveness
	clientset, err := kubernetes.NewForConfig(wcfg)
	if err != nil {
		return nil, err
	}
	return clientset.CoreV1().Namespaces().Watch(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=kube-system",
	})
}
