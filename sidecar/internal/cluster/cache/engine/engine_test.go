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

package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

// recordingSink captures every Report for assertion; statuses() snapshots them.
type recordingSink struct {
	mu       sync.Mutex
	reports  []EngineStatus
	reported chan struct{}
}

func newRecordingSink() *recordingSink {
	return &recordingSink{reported: make(chan struct{}, 64)}
}

func (s *recordingSink) Report(st EngineStatus) {
	s.mu.Lock()
	s.reports = append(s.reports, st)
	s.mu.Unlock()
	select {
	case s.reported <- struct{}{}:
	default:
	}
}

func (s *recordingSink) statuses() []EngineStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]EngineStatus(nil), s.reports...)
}

// An engine whose cluster is unreachable reports Syncing (the attempt) then
// Errored, and retries with growing backoff until stopped.
func TestEngineReportsErroredAndRetriesWithBackoff(t *testing.T) {
	cdb := migratedCDB(t)
	sink := newRecordingSink()
	rec, snapshot := recordingSleep()

	// A host nothing listens on: client construction succeeds, discovery fails.
	cfg := &rest.Config{Host: "https://127.0.0.1:1", Timeout: 50 * time.Millisecond}
	e := newEngineWithOptions(cfg, cdb, sink, withEngineSleep(rec))
	e.Start()

	waitFor(t, func() bool { return len(snapshot()) >= 3 }, "three retries recorded")
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, e.Stop(stopCtx))

	sleeps := snapshot()
	require.Equal(t, e.backoffInit, sleeps[0], "first backoff is the initial")
	require.Equal(t, 2*e.backoffInit, sleeps[1], "second backoff doubled")

	var sawSyncing, sawErrored bool
	for _, st := range sink.statuses() {
		switch st.State {
		case EngineSyncing:
			sawSyncing = true
			require.Empty(t, st.LastError, "Syncing reports carry no error")
		case EngineErrored:
			sawErrored = true
			require.NotEmpty(t, st.LastError, "Errored reports carry the failure")
		}
	}
	require.True(t, sawSyncing, "the attempt is reported before its failure")
	require.True(t, sawErrored)
}

// The freshness tracker stamps cache write pings and flushes them through the
// sink on the coarse cadence, preserving the engine's current state fields.
func TestEngineFreshnessFlush(t *testing.T) {
	cdb := migratedCDB(t)
	sink := newRecordingSink()

	at := time.Date(2026, 6, 12, 10, 0, 0, 0, time.UTC)
	e := newEngineWithOptions(nil, cdb, sink,
		withFlushInterval(time.Millisecond),
		withEngineNow(func() time.Time { return at }))

	// Drive the freshness loop alone — the run loop needs a live cluster. The
	// subscription is taken synchronously (as Start does) so the Notify below
	// cannot race it.
	pings, cancelSub := cdb.Subscribe()
	defer cancelSub()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.freshnessLoop(e.baseCtx, pings)
	}()

	cdb.Notify()
	waitFor(t, func() bool {
		for _, st := range sink.statuses() {
			if st.LastSyncedAt != nil && st.LastSyncedAt.Equal(at) {
				return true
			}
		}
		return false
	}, "freshness stamp flushed")

	e.baseCtxCancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("freshness loop did not stop on cancel")
	}
}

// A ping landing just before shutdown is flushed on the way out — the final
// stamp must not be lost to the 30s cadence.
func TestEngineFreshnessFinalFlushOnStop(t *testing.T) {
	cdb := migratedCDB(t)
	sink := newRecordingSink()

	at := time.Date(2026, 6, 12, 11, 0, 0, 0, time.UTC)
	e := newEngineWithOptions(nil, cdb, sink,
		withFlushInterval(time.Hour), // far past the test's lifetime
		withEngineNow(func() time.Time { return at }))

	pings, cancelSub := cdb.Subscribe()
	defer cancelSub()
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.freshnessLoop(e.baseCtx, pings)
	}()

	// The ping sits in the subscription's slot (taken before the loop ran), so
	// the shutdown drain must carry it into the final flush even if the loop
	// never consumed it before the cancel.
	cdb.Notify()
	e.baseCtxCancel()
	<-done

	statuses := sink.statuses()
	require.NotEmpty(t, statuses, "the final stamp flushes on shutdown")
	last := statuses[len(statuses)-1]
	require.NotNil(t, last.LastSyncedAt)
	require.True(t, last.LastSyncedAt.Equal(at))
}

// Stop joins both loops within its deadline.
func TestEngineStopJoins(t *testing.T) {
	cdb := migratedCDB(t)
	cfg := &rest.Config{Host: "https://127.0.0.1:1", Timeout: 50 * time.Millisecond}
	e := NewEngine(cfg, cdb, newRecordingSink())
	e.Start()

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, e.Stop(stopCtx))
}

// ConfigFingerprint must change when any auth-related field is edited — including
// exec/auth-provider/impersonation — so a kubeconfig edit while the app runs
// restarts sync instead of leaving drivers on stale credentials.
func TestConfigFingerprintCoversAuthFields(t *testing.T) {
	base := func() *rest.Config {
		return &rest.Config{Host: "https://x:6443", BearerToken: "tok"}
	}
	fp := ConfigFingerprint(base(), "")
	require.Equal(t, fp, ConfigFingerprint(base(), ""), "identical configs hash equal")

	// proxy-url lives in the kubeconfig, not rest.Config's hashable fields, so it
	// is passed alongside: a changed proxy must restart sync.
	require.NotEqual(t, fp, ConfigFingerprint(base(), "http://proxy:8080"), "proxy-url change must change the fingerprint")

	edits := map[string]func(*rest.Config){
		"exec command": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token"}
		},
		"exec args": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token", Args: []string{"--v2"}}
		},
		"exec env": func(c *rest.Config) {
			c.ExecProvider = &clientcmdapi.ExecConfig{Command: "get-token", Env: []clientcmdapi.ExecEnvVar{{Name: "K", Value: "V"}}}
		},
		"auth provider": func(c *rest.Config) {
			c.AuthProvider = &clientcmdapi.AuthProviderConfig{Name: "oidc", Config: map[string]string{"client-id": "abc"}}
		},
		"impersonate user": func(c *rest.Config) {
			c.Impersonate = rest.ImpersonationConfig{UserName: "admin"}
		},
	}
	for name, edit := range edits {
		t.Run(name, func(t *testing.T) {
			c := base()
			edit(c)
			require.NotEqual(t, fp, ConfigFingerprint(c, ""), "edit must change the fingerprint")
		})
	}

	// Editing an existing exec field (not just adding the block) must also change it.
	e1, e2 := base(), base()
	e1.ExecProvider = &clientcmdapi.ExecConfig{Command: "t", Args: []string{"--a"}}
	e2.ExecProvider = &clientcmdapi.ExecConfig{Command: "t", Args: []string{"--b"}}
	require.NotEqual(t, ConfigFingerprint(e1, ""), ConfigFingerprint(e2, ""), "editing exec args changes the fingerprint")
}

// ContextProxyURL resolves the proxy-url through the context's cluster entry,
// tolerating absent contexts/clusters.
func TestContextProxyURL(t *testing.T) {
	cfg := &clientcmdapi.Config{
		Contexts: map[string]*clientcmdapi.Context{
			"a": {Cluster: "a-cluster"},
			"b": {Cluster: "missing"},
		},
		Clusters: map[string]*clientcmdapi.Cluster{
			"a-cluster": {Server: "https://a", ProxyURL: "http://proxy:8080"},
		},
	}
	require.Equal(t, "http://proxy:8080", ContextProxyURL(cfg, "a"))
	require.Equal(t, "", ContextProxyURL(cfg, "b"), "dangling cluster reference")
	require.Equal(t, "", ContextProxyURL(cfg, "nope"), "absent context")
}

// The driver invokes onWatch exactly once — the first time it enters its watch
// phase — which is what the engine's Syncing→Watching countdown rides on.
func TestDriverOnWatchFiresOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cdb := migratedCDB(t)
	store := newObjectsStore(ctx, "c1", podGVK(), cdb.Writer(), cdb)
	fs := &fakeSource{
		watchers: make(chan *watch.FakeWatcher, 4),
		// The post-watch re-sync needs a successful metadata diff to reach the
		// second watch phase (p1 unchanged at its applied RV).
		metas:  []objMeta{{UID: "p1", Namespace: "default", Name: "p1", ResourceVersion: "101"}},
		metaRV: "150",
	}

	var mu sync.Mutex
	calls := 0
	noSleep := func(context.Context, time.Duration) error { return nil }
	d := newKindDriverWithOptions(fs, store, podGVK(), "100", withSleep(noSleep))
	d.onWatch = func() {
		mu.Lock()
		calls++
		mu.Unlock()
	}

	done := make(chan struct{})
	go func() { defer close(done); _ = d.Run(ctx) }()

	// First watch establishes (onWatch fires), then ends cleanly with progress
	// → re-sync → second watch (onWatch must not fire again).
	fw := <-fs.watchers
	fw.Add(uObj("p1", "v1", "Pod", "default", "p1", "101"))
	waitFor(t, func() bool { return metaValue(t, cdb, "v1/Pod.last_list_rv") == "101" }, "delta applied")
	fw.Stop()
	<-fs.watchers // second watch established

	mu.Lock()
	got := calls
	mu.Unlock()
	require.Equal(t, 1, got, "onWatch fires exactly once per driver")

	cancel()
	<-done
}
