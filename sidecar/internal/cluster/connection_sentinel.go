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
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// SentinelWatchFunc establishes one long-lived liveness watch against cfg. The
// returned watch's result channel staying open means the connection is alive; it
// closing (or a re-establish failing) is the connection-loss signal the sentinel
// reacts to. Tests inject a fake; production uses watchKubeSystem.
type SentinelWatchFunc func(ctx context.Context, cfg *rest.Config) (watch.Interface, error)

// connSentinel is the controller's handle on one cluster's running liveness
// watch: the cancel that tears its goroutine down, and the config fingerprint it
// was started with so a credential rotation can restart it (vs. leaving a stale
// one watching with expired creds).
type connSentinel struct {
	cancel      context.CancelFunc
	fingerprint string
}

// SetSentinelWatcher overrides the connection-sentinel watch function — for tests
// (production defaults to watchKubeSystem). Call once, before the control plane
// starts.
func (c *ClusterCoreController) SetSentinelWatcher(f SentinelWatchFunc) {
	c.sentinelWatch = f
}

// ensureSentinel guarantees a liveness watch is running for a just-connected
// cluster, started with the given connection config. It is a no-op when one is
// already running on the same config fingerprint; a fingerprint change (credential
// rotation) restarts it. Called from converge after a successful probe, so the
// sentinel exists exactly while the cluster is connected. A nil bgCtx skips the
// launch: before StartBackground supplies the base context there is nowhere to
// anchor the goroutine's lifetime (the next reconcile starts it once the worker is
// up), and after StopBackground clears bgCtx under sentinelMu it is the shutdown
// gate — a reconcile racing teardown must not sentinelWG.Add after StopBackground's
// Wait has begun.
func (c *ClusterCoreController) ensureSentinel(id ClusterID, cfg *rest.Config, fingerprint string) {
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

// stopSentinel cancels and forgets the liveness watch for id (if any). Called from
// converge when a cluster becomes ineligible or its probe/resolve fails — the
// connection is gone, so its sentinel must not linger and re-probe.
func (c *ClusterCoreController) stopSentinel(id ClusterID) {
	c.sentinelMu.Lock()
	defer c.sentinelMu.Unlock()
	if s, ok := c.sentinels[id]; ok {
		s.cancel()
		delete(c.sentinels, id)
	}
}

// teardownConnection drops the cluster's cached connection and stops its liveness
// sentinel — the common cleanup every "no longer connected" path in converge runs
// (ineligible, resolve-fail, probe-fail), kept in one place so the two steps can't
// drift apart.
func (c *ClusterCoreController) teardownConnection(id ClusterID) {
	if c.connMgr != nil {
		c.connMgr.Delete(id)
	}
	c.stopSentinel(id)
}

// runSentinel holds one long-lived watch open and, when it closes, fires a single
// out-of-band re-probe of the cluster. A watch's result channel closing is the
// earliest connection-loss signal — the HTTP/2 keepalive (ConfigureKubeHTTP2Keepalive)
// tears a silently-dead connection down in ~15s, closing the stream. The re-probe
// (Reprobe → reprobeOne) re-confirms the connection and, on success, converge starts
// a fresh sentinel; on failure, beehive's backoff owns the retry cadence and no
// sentinel runs while disconnected. A benign server-side watch timeout also closes
// the stream — the resulting re-probe is idempotent and far cheaper than the health
// poll, so it is not worth distinguishing. The watch is one-shot: it exits after
// firing (or on ctx cancel / establish failure), so a persistently-down cluster
// never spins here.
func (c *ClusterCoreController) runSentinel(ctx context.Context, id ClusterID, self *connSentinel, cfg *rest.Config) {
	defer c.sentinelWG.Done()
	defer c.sentinelExited(id, self)

	w, err := c.sentinelWatch(ctx, cfg)
	if err != nil {
		// Could not even establish the watch — the connection is down (unless we are
		// shutting down, in which case fireSentinelReprobe stays quiet).
		c.fireSentinelReprobe(ctx, id, self)
		return
	}
	defer w.Stop()

	// Drain (and ignore) events — we watch for the stream closing, not its contents.
	// Selecting on ctx.Done() too means a deliberate stop (shutdown / stopSentinel)
	// tears the goroutine down even if the watch implementation does not honor ctx.
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

// fireSentinelReprobe requests an out-of-band re-probe of id, unless the sentinel's
// context has been cancelled (shutdown or a deliberate stopSentinel). It clears the
// sentinel's own map entry first so the re-probe's converge sees no live sentinel and
// starts a fresh one on success (otherwise ensureSentinel would treat the
// just-exited entry as still running and skip the restart).
func (c *ClusterCoreController) fireSentinelReprobe(ctx context.Context, id ClusterID, self *connSentinel) {
	if ctx.Err() != nil {
		return
	}
	c.sentinelExited(id, self)
	c.Reprobe(id)
}

// sentinelExited removes self from the sentinels map if it is still the current
// entry (pointer identity), so a sentinel replaced by ensureSentinel or cleared by
// stopSentinel does not delete its successor. Idempotent — safe to call from both
// fireSentinelReprobe and the run goroutine's defer.
func (c *ClusterCoreController) sentinelExited(id ClusterID, self *connSentinel) {
	c.sentinelMu.Lock()
	defer c.sentinelMu.Unlock()
	if c.sentinels[id] == self {
		delete(c.sentinels, id)
	}
}

// watchKubeSystem opens a long-lived, single-object watch on the kube-system
// namespace — a resource present on every cluster — purely as a connection-liveness
// probe: the controller reacts to the stream closing, never to its contents. The
// per-request client timeout is cleared so the watch is not torn down on a timer;
// liveness is left to the HTTP/2 keepalive. It is the production SentinelWatchFunc.
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
