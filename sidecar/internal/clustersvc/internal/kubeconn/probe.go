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

// The five probes over one kube-context's server. What is asked and how the answers classify
// lives here; when anything runs is the engine's (internal/probe) — the schedule is derived from
// what each probe records, and every probe behind reachability declares that dependency rather
// than testing it.
//
// **Nothing dials yet.** The connection probe resolves the kubeconfig and reports a context that
// left it; the four behind it are unimplemented, and the engine records DependencyFailed for
// them while the connection has not succeeded — which is their correct behavior once they exist.
package kubeconn

import (
	"context"
	"errors"
	"time"

	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/kubetail-org/kstack-app/sidecar/internal/probe"
)

// numProbes sizes what is tracked per context, one entry per field of probeHandles.
const numProbes = 5

// probeHandles is what registration returned: the typed reads State is assembled through, and
// the IDs Wake addresses probes by. A cluster's UID outlives everything else here; its readiness
// outlives nothing — which is why the intervals below differ.
type probeHandles struct {
	connection    probe.Handle[connInfo]
	readiness     probe.Handle[ComponentStatus]
	serverUID     probe.Handle[string]
	serverVersion probe.Handle[VersionInfo]
	principal     probe.Handle[Principal]
}

// connInfo is the connection probe's observable: the context's standing in the kubeconfig, and
// the connection reaching its server yields. The four probes behind reachability read it through
// the connection handle.
type connInfo struct {
	// departed is what the probe's last read of the kubeconfig said: false while the file
	// names this context, true once it stops. Flipping it is news the holders are told.
	departed bool
	// conn is the connection the probes run over. Nothing builds one yet.
	conn *Connection
	// endpoint is the API server URL that answered.
	endpoint string
}

func registerProbes(e *probe.Engine, kubecfg kubeconfigService) probeHandles {
	var p probeHandles
	p.connection = probe.Register(e, "connection", &connectionProbe{kubecfg: kubecfg},
		probe.WithInterval(30*time.Second))
	p.readiness = probe.Register(e, "readiness", unimplemented[ComponentStatus]{"readiness"},
		probe.WithInterval(30*time.Second), probe.Needs(p.connection.ID()))
	p.serverUID = probe.Register(e, "serverUID", unimplemented[string]{"serverUID"},
		probe.WithInterval(10*time.Minute), probe.Needs(p.connection.ID()))
	p.serverVersion = probe.Register(e, "serverVersion", unimplemented[VersionInfo]{"serverVersion"},
		probe.WithInterval(5*time.Minute), probe.Needs(p.connection.ID()))
	p.principal = probe.Register(e, "principal", unimplemented[Principal]{"principal"},
		probe.WithInterval(5*time.Minute), probe.Needs(p.connection.ID()))
	return p
}

// connectionProbe answers whether the server behind the context can be reached, and owns the
// context's lifecycle on the way there: resolving the kubeconfig is the first step of reaching a
// server, so a context that left the file is this probe's to find.
//
// Nothing dials yet, so a context that resolves suspends with ReasonResolved — resolving is the
// precondition, not an answer about the server, and Phase reads it as still pending. The
// kubeconfig watch wakes this probe on every change, which is what re-asks every one of these
// answers.
type connectionProbe struct {
	kubecfg kubeconfigService
}

func (p *connectionProbe) Run(_ context.Context, contextName string, prev connInfo, _ probe.View) (probe.Result, *connInfo) {
	_, _, err := p.kubecfg.RESTConfig(contextName)
	switch {
	case errors.Is(err, kubeconfig.ErrNotRead):
		// An unread kubeconfig names nothing, and saying so would report every context
		// gone for as long as the first read takes. The watch wakes us when it is read.
		return probe.Skip(), nil

	case errors.Is(err, kubeconfig.ErrContextNotFound):
		next := prev
		next.departed = true
		return probe.Suspend(ReasonContextNotFound, "kubeconfig no longer names this context"), &next

	case err != nil:
		// The file still names it, so this is not a departure — it is a file the user
		// has to fix, and the retry ladder is for a fix the kubeconfig watch cannot see,
		// such as a CA path that now opens.
		next := prev
		next.departed = false
		return probe.Fail(ReasonResolveFailed, err), &next

	default:
		next := prev
		next.departed = false
		return probe.Suspend(ReasonResolved, "resolved; nothing dials yet"), &next
	}
}

// unimplemented stands in for a probe with no request behind it yet. Unreachable while nothing
// dials — every one of these needs a connection that never succeeds — and it records rather
// than going quiet, so a run that does reach it says so instead of looking suspended for a
// reason nobody wrote down.
type unimplemented[T any] struct {
	name string
}

func (u unimplemented[T]) Run(context.Context, string, T, probe.View) (probe.Result, *T) {
	return probe.Suspend(ReasonInternal, u.name+" probe is not implemented"), nil
}
