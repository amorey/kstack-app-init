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

package objectsync

import (
	"database/sql"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// This file is the ONLY kind-specific code in the write path, and it exists for the
// materialized columns on the objects table — status_summary plus ready/total/restart
// counts and host.
//
// Those columns are not what the dashboard renders: the frontend derives its per-kind
// cells client-side from the object's own body (which is why raw_json is served verbatim),
// and nothing here feeds that. They exist to make the cache QUERYABLE in SQL — "which pods
// in this namespace are not ready", "which nodes are NotReady" — without unpacking a blob
// per row, which is the shape an agent asking questions about a cluster needs.
//
// Being wrong here is a display/query nuance, never a correctness problem: every column is
// nullable, the body beside it is authoritative, and an unrecognized kind falls back to the
// conventional conditions form rather than guessing.
//
// **It runs on the hot write path** — every watch delta and every relist page item, across
// a cache's hundred-plus kind workers — so it reads the body through NestedFieldNoCopy
// rather than the Nested{Slice,Map} helpers, which deep-copy what they return. Nothing here
// mutates what it reads.

// statusReading is what one object's status projects to. A struct rather than five return
// values because five of the readings below produce only a summary: as a tuple they each
// had to spell out three nil counts and an empty host, and adding a column meant editing
// every signature.
//
// The counts are NullInt64 rather than *int: the columns are nullable (a count that does
// not apply to a kind must stay NULL, not become a 0 that reads as "none ready"), and a
// value type keeps this allocation-free on a path that runs per object.
type statusReading struct {
	Summary string
	Ready   sql.NullInt64
	Total   sql.NullInt64
	Restart sql.NullInt64
	Host    string
}

func count(n int64) sql.NullInt64 { return sql.NullInt64{Int64: n, Valid: true} }

// summaryOnly is the reading for a kind whose status has no count worth materializing.
func summaryOnly(summary string) statusReading { return statusReading{Summary: summary} }

// readyOverTotal is the shared shape of the workload kinds: an "x/y" summary carrying a
// verdict, plus the two counts behind it.
func readyOverTotal(verdict string, ready, total int64) statusReading {
	return statusReading{
		Summary: verdict + " " + strconv.FormatInt(ready, 10) + "/" + strconv.FormatInt(total, 10),
		Ready:   count(ready),
		Total:   count(total),
	}
}

// boolReading is the shape of the kinds whose readiness is one bit (a Node, a PVC): the
// summary the caller computed, counted as 0-or-1 of 1 so the same "not ready" query spans
// them and the workload kinds.
func boolReading(summary string, ok bool) statusReading {
	ready := int64(0)
	if ok {
		ready = 1
	}
	return statusReading{Summary: summary, Ready: count(ready), Total: count(1)}
}

// statusReaders maps a built-in's identity to its reading. Keyed on the api GROUP and the
// Kind, never the Kind alone: "Pod", "Job" and "Node" are not reserved words, and a CRD
// carrying one of those names in its own group is that user's kind — it must reach the
// conditions fallback rather than have a built-in's reader look for fields it doesn't have.
// (Same rule as the Secret check in the write path, and as isEventsKind in the controller.)
var statusReaders = map[struct{ group, kind string }]func(*unstructured.Unstructured) statusReading{
	{"", "Pod"}:                   statusPod,
	{"", "Service"}:               statusService,
	{"", "Node"}:                  statusNode,
	{"", "Namespace"}:             statusNamespace,
	{"", "PersistentVolumeClaim"}: statusPVC,
	{"apps", "Deployment"}:        statusReplicaController,
	{"apps", "StatefulSet"}:       statusReplicaController,
	{"apps", "ReplicaSet"}:        statusReplicaController,
	{"apps", "DaemonSet"}:         statusDaemonSet,
	{"batch", "Job"}:              statusJob,
	{"batch", "CronJob"}:          statusCronJob,
}

// extractStatus projects one object's status. For a kind with no reader — most CRDs — it
// falls back to the conventional .status.conditions[].type+status form, which covers the
// large majority of operator CRDs (ArgoCD, Cert-Manager, Crossplane, Knative all follow it).
//
// The identity comes from the BODY, not from the worker's configured kind, so how a
// collection was addressed can't change how its objects are read.
func extractStatus(u *unstructured.Unstructured) statusReading {
	if read, ok := statusReaders[struct{ group, kind string }{apiGroup(u.GetAPIVersion()), u.GetKind()}]; ok {
		return read(u)
	}
	return summaryOnly(statusFromConditions(u))
}

// apiGroup returns the group half of an apiVersion ("apps/v1" → "apps"); the core group's
// unqualified "v1" has none.
func apiGroup(apiVersion string) string {
	group, _, ok := strings.Cut(apiVersion, "/")
	if !ok {
		return ""
	}
	return group
}

// nestedSlice reads a slice without apimachinery's deep copy. The callers below only read
// strings, bools and ints out of the elements, so the shared view is safe.
func nestedSlice(obj map[string]any, fields ...string) []any {
	v, found, err := unstructured.NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return nil
	}
	s, _ := v.([]any)
	return s
}

// containerScan is what one list of container statuses contributes to a pod's reading.
type containerScan struct {
	ready    int64
	total    int64
	restarts int64
	// reason is the first waiting/terminated reason found, which is what the summary
	// surfaces — "CrashLoopBackOff" is far more useful than "Running" for a pod whose only
	// container is crashing.
	reason string
}

// scanContainers reads one containerStatuses-shaped list. Shared by the app and init
// passes, which differ only in what their caller does with the result — the
// waiting-then-terminated-unless-Completed precedence is subtle enough that two copies
// would drift.
func scanContainers(list []any) containerScan {
	out := containerScan{total: int64(len(list))}
	for _, c := range list {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if r, _, _ := unstructured.NestedBool(m, "ready"); r {
			out.ready++
		}
		if n, _, _ := unstructured.NestedInt64(m, "restartCount"); n > 0 {
			out.restarts += n
		}
		if out.reason != "" {
			continue
		}
		// state carries at most one of waiting/running/terminated; one lookup of it serves
		// both reason probes. A terminated "Completed" is a normal exit, not a reason.
		state, _, _ := unstructured.NestedFieldNoCopy(m, "state")
		sm, _ := state.(map[string]any)
		if reason, _, _ := unstructured.NestedString(sm, "waiting", "reason"); reason != "" {
			out.reason = reason
		} else if reason, _, _ := unstructured.NestedString(sm, "terminated", "reason"); reason != "" && reason != "Completed" {
			out.reason = reason
		}
	}
	return out
}

func statusPod(u *unstructured.Unstructured) statusReading {
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	host, _, _ := unstructured.NestedString(u.Object, "spec", "nodeName")

	app := scanContainers(nestedSlice(u.Object, "status", "containerStatuses"))
	// A pod blocked in init reports its real reason under initContainerStatuses while
	// containerStatuses still looks plain Pending. An init failure takes priority over any
	// app-container reason and is prefixed "Init:" to match kubectl; a completed init
	// container is normal and skipped, but its restarts still count.
	init := scanContainers(nestedSlice(u.Object, "status", "initContainerStatuses"))

	worstReason := app.reason
	if init.reason != "" {
		worstReason = "Init:" + init.reason
	}
	summary := phase
	if worstReason != "" && phase != "Succeeded" {
		summary = worstReason
	}
	return statusReading{
		Summary: summary,
		Ready:   count(app.ready),
		Total:   count(app.total),
		Restart: count(app.restarts + init.restarts),
		Host:    host,
	}
}

func statusReplicaController(u *unstructured.Unstructured) statusReading {
	// Use desired (spec.replicas), not status.replicas: the observed count lags desired
	// during scale-up, so a desired-3 workload would otherwise render as "ScaledToZero
	// 0/0". spec.replicas defaults to 1 when omitted.
	desired, found, _ := unstructured.NestedInt64(u.Object, "spec", "replicas")
	if !found {
		desired = 1
	}
	ready, _, _ := unstructured.NestedInt64(u.Object, "status", "readyReplicas")

	verdict := "Progressing"
	switch {
	case desired == 0:
		verdict = "ScaledToZero"
	case ready == desired:
		verdict = "Available"
	}
	return readyOverTotal(verdict, ready, desired)
}

func statusDaemonSet(u *unstructured.Unstructured) statusReading {
	desired, _, _ := unstructured.NestedInt64(u.Object, "status", "desiredNumberScheduled")
	ready, _, _ := unstructured.NestedInt64(u.Object, "status", "numberReady")
	verdict := "Progressing"
	if ready == desired && desired > 0 {
		verdict = "Available"
	}
	return readyOverTotal(verdict, ready, desired)
}

func statusNode(u *unstructured.Unstructured) statusReading {
	isReady := false
	var pressure string
	for _, c := range nestedSlice(u.Object, "status", "conditions") {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		t, _, _ := unstructured.NestedString(m, "type")
		s, _, _ := unstructured.NestedString(m, "status")
		switch t {
		case "Ready":
			isReady = s == string(corev1.ConditionTrue)
		case "DiskPressure", "MemoryPressure", "PIDPressure", "NetworkUnavailable":
			if s == string(corev1.ConditionTrue) {
				pressure = t
			}
		}
	}
	summary := "NotReady"
	if isReady {
		summary = "Ready"
	}
	if pressure != "" {
		summary += " (" + pressure + ")"
	}
	return boolReading(summary, isReady)
}

func statusService(u *unstructured.Unstructured) statusReading {
	t, _, _ := unstructured.NestedString(u.Object, "spec", "type")
	ip, _, _ := unstructured.NestedString(u.Object, "spec", "clusterIP")
	if t == "" {
		t = "ClusterIP"
	}
	return summaryOnly(t + " " + ip)
}

func statusNamespace(u *unstructured.Unstructured) statusReading {
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	return summaryOnly(phase)
}

func statusPVC(u *unstructured.Unstructured) statusReading {
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	return boolReading(phase, phase == "Bound")
}

func statusJob(u *unstructured.Unstructured) statusReading {
	succeeded, _, _ := unstructured.NestedInt64(u.Object, "status", "succeeded")
	failed, _, _ := unstructured.NestedInt64(u.Object, "status", "failed")
	for _, c := range nestedSlice(u.Object, "status", "conditions") {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		t, _, _ := unstructured.NestedString(m, "type")
		s, _, _ := unstructured.NestedString(m, "status")
		if s != string(corev1.ConditionTrue) {
			continue
		}
		switch t {
		case "Complete":
			return boolReading("Complete", true)
		case "Failed":
			return boolReading("Failed ("+strconv.FormatInt(failed, 10)+")", false)
		}
	}
	return boolReading("Running ("+strconv.FormatInt(succeeded, 10)+" succeeded)", false)
}

func statusCronJob(u *unstructured.Unstructured) statusReading {
	active := strconv.Itoa(len(nestedSlice(u.Object, "status", "active")))
	last, _, _ := unstructured.NestedString(u.Object, "status", "lastScheduleTime")
	if last != "" {
		return summaryOnly(active + " active, last " + last)
	}
	return summaryOnly(active + " active")
}

// statusFromConditions is the CRD fallback. Most operator CRDs put their state in
// .status.conditions[] using the Kubernetes condition convention. We surface the type of
// the most-recently-true condition, optionally suffixed with reason if the condition is
// False (e.g. "Synced (OutOfSync)").
func statusFromConditions(u *unstructured.Unstructured) string {
	var lastTrue, lastTrueAt, lastFalseType, lastFalseReason string
	haveTrue := false
	for _, c := range nestedSlice(u.Object, "status", "conditions") {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		t, _, _ := unstructured.NestedString(m, "type")
		s, _, _ := unstructured.NestedString(m, "status")
		switch s {
		case string(corev1.ConditionTrue):
			// Take the first True condition unconditionally, then prefer any later one
			// with a newer timestamp. Without the haveTrue guard a True condition that
			// omits (or malforms) lastTransitionTime would never beat the empty
			// lastTrueAt and would be dropped — falling through to a False/empty summary
			// despite being True.
			//
			// Compared as a STRING, not a parsed time: Kubernetes serializes
			// lastTransitionTime as fixed-width RFC3339 in UTC, which orders
			// lexicographically, and this runs per condition per object on the fallback
			// path every non-built-in kind takes.
			lt, _, _ := unstructured.NestedString(m, "lastTransitionTime")
			if !haveTrue || lt > lastTrueAt {
				haveTrue = true
				lastTrueAt = lt
				lastTrue = t
			}
		case string(corev1.ConditionFalse):
			if lastFalseType == "" {
				lastFalseType = t
				lastFalseReason, _, _ = unstructured.NestedString(m, "reason")
			}
		}
	}
	switch {
	case lastTrue != "":
		return lastTrue
	case lastFalseType != "" && lastFalseReason != "":
		return lastFalseType + " (" + lastFalseReason + ")"
	default:
		return lastFalseType
	}
}
