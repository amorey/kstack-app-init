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

// The five probes over one kube-context's server: what each asks and how the answers classify.
// Scheduling belongs to the engine (internal/probe) — each probe behind reachability declares its
// dependency on the connection rather than testing it.
//
// Only the connection probe is implemented. For the four behind it the engine records
// DependencyFailed while the connection has not succeeded — their correct behavior once they
// exist.
package kubeconn

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// Registration names are how probes are addressed everywhere: the edges, Wake, and every read
// take one. A probe body reads a sibling with probe.Get[T](snap, nameConnection).
const (
	nameConnection    = "connection"
	nameReadiness     = "readiness"
	nameServerUID     = "serverUID"
	nameServerVersion = "serverVersion"
	namePrincipal     = "principal"
)

// probeNames is the full set in registration order, for readers that walk every probe.
var probeNames = [...]string{nameConnection, nameReadiness, nameServerUID, nameServerVersion, namePrincipal}

// keyConnection reads the connection's observable — the only one another probe needs, so the
// only one with a key.
var keyConnection = probe.NewKey[connInfo](nameConnection)

// connInfo is the connection probe's observable: the context's standing in the kubeconfig, and
// the connection reaching its server yields.
//
// Comparable, which the commit-only-on-a-change guard rests on: a rebuilt connection is a new
// pointer and moves the value, a re-dialed one is not.
type connInfo struct {
	// departed is what the probe's last read of the kubeconfig said: true once the file
	// stops naming this context.
	departed bool
	// conn is the connection the probes run over, nil while the context resolves to nothing.
	conn *Connection
	// endpoint is the API server URL that answered.
	endpoint string
	// fingerprint describes the credentials conn was built from — what tells a rotation
	// from a kubeconfig write that changed nothing.
	fingerprint string
}

func registerProbes(e *probe.Engine, kubecfg kubeconfigService) {
	// A reachability check that has taken ten seconds has answered, so the timeout is
	// shorter than the engine's default (a whole interval).
	probe.Register(e, nameConnection, &connectionProbe{kubecfgSvc: kubecfg},
		probe.WithInterval(30*time.Second), probe.WithTimeout(10*time.Second))

	// The four behind reachability declare both edges on it: they cannot run without a
	// connection, and a connection that moves must re-run them.
	dependsOnConn, watchesConn := probe.WithDependencies(nameConnection), probe.WithWatches(nameConnection)
	probe.Register(e, nameReadiness, unimplemented[ComponentStatus]{"readiness"}, probe.WithInterval(30*time.Second), dependsOnConn, watchesConn)
	probe.Register(e, nameServerUID, unimplemented[string]{"serverUID"}, probe.WithInterval(10*time.Minute), dependsOnConn, watchesConn)
	probe.Register(e, nameServerVersion, unimplemented[VersionInfo]{"serverVersion"}, probe.WithInterval(5*time.Minute), dependsOnConn, watchesConn)
	probe.Register(e, namePrincipal, unimplemented[Principal]{"principal"}, probe.WithInterval(5*time.Minute), dependsOnConn, watchesConn)
}

// apiDiscoveryPath is the one request this probe makes: the cheapest that proves the whole path
// (DNS, TCP, TLS, authentication), the only endpoint of the five probes' that can answer 401 or
// 403, and the one whose body tells a Kubernetes API server from a captive portal answering 200
// to anything.
const apiDiscoveryPath = "/api"

// connectionProbe answers whether the server behind the context can be reached. It also owns the
// context's lifecycle: resolving the kubeconfig is the first step of reaching a server, so a
// context that left the file is this probe's to find. The kubeconfig watch wakes it on every
// change.
type connectionProbe struct {
	kubecfgSvc kubeconfigService
}

// Run resolves the context, keeps its connection current, and dials the server once.
func (p *connectionProbe) Run(ctx context.Context, pass *probe.Pass[connInfo]) probe.Result {
	cfg, fingerprint, err := p.kubecfgSvc.RESTConfig(pass.Subject())
	if errors.Is(err, kubeconfig.ErrNotRead) {
		// An unread kubeconfig names nothing; saying so would report every context gone
		// for as long as the first read takes. The watch wakes us when it is read.
		return probe.Skip()
	}

	next := pass.Prev()
	// Deferred so no return path can skip it: a connection built by a run that then failed
	// must still be committed, or nothing can reach it to retire it. Only on a change — an
	// unconditional commit would re-run the four probes watching this value every cycle.
	defer func() {
		if next != pass.Prev() {
			pass.Commit(next)
		}
	}()

	if errors.Is(err, kubeconfig.ErrContextNotFound) {
		next = connInfo{departed: true}
		return probe.Suspend(ReasonContextNotFound, "kubeconfig no longer names this context")
	}
	next.departed = false
	if err != nil {
		// The file still names the context, so this is not a departure — it is a file the
		// user has to fix, retried on the ladder because a fix can be invisible to the
		// kubeconfig watch (a CA path that now opens). The connection stands: a read that
		// failed says nothing about the credentials behind it, and an editor saving
		// non-atomically fails reads for a moment. Credentials that really moved come back
		// as a changed fingerprint.
		return probe.Fail(ReasonResolveFailed, err)
	}

	// Rebuild on a changed fingerprint or no connection — never the fingerprint alone: a
	// failed build commits the fingerprint with nothing behind it, and a fingerprint-only
	// check would find it unchanged and never build again.
	if next.conn == nil || next.fingerprint != fingerprint {
		conn, err := newConnection(cfg)
		next = connInfo{conn: conn, fingerprint: fingerprint}
		if err != nil {
			// Nothing was dialed; the remedy is the same file fix a context that will
			// not resolve needs.
			return probe.Fail(ReasonResolveFailed, err)
		}
		next.endpoint = conn.BaseURL.String()
	}

	var discovery struct {
		Versions []string `json:"versions"`
	}
	if err := next.conn.getJSON(ctx, apiDiscoveryPath, &discovery); err != nil {
		if errors.Is(err, context.Canceled) {
			// The caller went away: nothing about the cluster to record. The engine
			// hands the committed value back through Discard.
			return probe.Skip()
		}
		return probe.Fail(classify(err), err)
	}
	if len(discovery.Versions) == 0 {
		return probe.Fail(ReasonMalformed, fmt.Errorf("%s: answered without API versions", apiDiscoveryPath))
	}
	return probe.Succeeded()
}

// Discard retires a connection the engine dropped — a run whose commit it refused because the
// context was released mid-run, one that concluded Skip, or one that panicked. Such a value
// reaches neither a snapshot nor an entry, so nothing else can retire what it built.
//
// Retiring outright is safe because a committed connInfo always carries a connection this run
// built: every change either clears the connection (a departure) or rebuilds it (a changed
// fingerprint), so a value that moved never carries a connection someone else holds.
func (p *connectionProbe) Discard(v connInfo) {
	if v.conn != nil {
		v.conn.retire()
	}
}

// unimplemented stands in for a probe with no request behind it yet. Unreachable while nothing
// dials — each needs a connection that never succeeds — and it records rather than going quiet,
// so a run that does reach it says so instead of looking suspended for no reason.
type unimplemented[T any] struct {
	name string
}

func (u unimplemented[T]) Run(context.Context, *probe.Pass[T]) probe.Result {
	return probe.Suspend(ReasonInternal, u.name+" probe is not implemented")
}
