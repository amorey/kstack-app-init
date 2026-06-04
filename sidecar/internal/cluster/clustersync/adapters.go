package clustersync

import (
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// All adapters operate on *unstructured.Unstructured so the same code
// path serves both built-in kinds and CRDs — the dynamic client returns
// unstructured for everything, and that's deliberate: it's the only way
// to handle CRDs uniformly without code-genned typed clients.
//
// The universal schema lives in three tables (objects, owner_refs,
// labels). The adapter extracts a single ObjectRow per object; the
// store writes one row to each of the three tables (and optionally
// status_history) in one transaction.

// OwnerRef is a row in owner_refs, pre-extracted from
// metadata.ownerReferences.
type OwnerRef struct {
	OwnerUID     string
	IsController bool
}

// ObjectRow is the per-write payload the store persists. Pointer fields
// are nullable in SQLite — populated by status extractors where they
// apply (Pod/Deployment/Node/…), left nil for kinds with no clear
// interpretation (most CRDs).
type ObjectRow struct {
	UID             string
	APIVersion      string
	Kind            string
	Namespace       string
	Name            string
	ResourceVersion string
	Generation      int64
	CreatedAt       int64 // unix millis from creationTimestamp
	StatusSummary   string
	ReadyCount      *int
	TotalCount      *int
	RestartCount    *int
	Host            string
	RawJSON         []byte
	OwnerRefs       []OwnerRef
	Labels          map[string]string
}

// extractObject pulls the fields the universal schema cares about out of
// an unstructured object. Everything not extracted lives in RawJSON.
//
// Returns (row, isEvent). Events get routed to the events table instead
// of objects so the caller can branch on this.
func extractObject(u *unstructured.Unstructured) (ObjectRow, bool, error) {
	if u == nil || u.Object == nil {
		return ObjectRow{}, false, fmt.Errorf("nil unstructured")
	}
	kind := u.GetKind()
	apiVersion := u.GetAPIVersion()
	// Events from core/v1 ("Event") and events.k8s.io/v1 ("Event") both
	// land here — we want them in the dedicated events table. Discriminate
	// by group (core/v1 -> "v1", events.k8s.io/v1 -> "events.k8s.io/v1").
	if kind == "Event" && (apiVersion == "v1" || apiVersion == "events.k8s.io/v1") {
		return ObjectRow{}, true, nil
	}

	// Secrets carry sensitive values in .data (base64-encoded) and
	// .stringData. The agent's troubleshooting use case wants to know
	// "this Secret exists, what type, what keys does it expose" — never
	// the values. Redact in-place on a deep copy before marshaling so
	// raw_json never holds the plaintext on disk. Keys are preserved so
	// the agent can still answer "does this Pod's mounted Secret have
	// the expected keys?"
	source := u.Object
	if kind == "Secret" && apiVersion == "v1" {
		source = redactSecret(u.Object)
	}

	rawJSON, err := json.Marshal(source)
	if err != nil {
		return ObjectRow{}, false, err
	}

	row := ObjectRow{
		UID:             string(u.GetUID()),
		APIVersion:      apiVersion,
		Kind:            kind,
		Namespace:       u.GetNamespace(),
		Name:            u.GetName(),
		ResourceVersion: u.GetResourceVersion(),
		Generation:      u.GetGeneration(),
		CreatedAt:       u.GetCreationTimestamp().UnixMilli(),
		RawJSON:         rawJSON,
		Labels:          u.GetLabels(),
	}
	if row.UID == "" {
		// Some Reflector frames briefly carry empty UID (e.g. server-side
		// dry-runs); skip rather than wedge the PK.
		return ObjectRow{}, false, fmt.Errorf("object %s/%s has empty UID", row.Namespace, row.Name)
	}

	for _, o := range u.GetOwnerReferences() {
		row.OwnerRefs = append(row.OwnerRefs, OwnerRef{
			OwnerUID:     string(o.UID),
			IsController: o.Controller != nil && *o.Controller,
		})
	}

	row.StatusSummary, row.ReadyCount, row.TotalCount, row.RestartCount, row.Host = extractStatus(u)
	return row, false, nil
}

// extractStatus is the only kind-specific code in the persistence path.
// Returns the dashboard-ready summary plus four cross-kind materialized
// fields. For unknown kinds (most CRDs) we fall back to the conventional
// .status.conditions[].type+status form, which covers ~90% of operator
// CRDs (ArgoCD, Cert-Manager, Crossplane, Knative, etc. all follow it).
func extractStatus(u *unstructured.Unstructured) (summary string, ready, total, restart *int, host string) {
	kind := u.GetKind()
	switch kind {
	case "Pod":
		return statusPod(u)
	case "Deployment", "StatefulSet", "ReplicaSet":
		return statusReplicaController(u)
	case "DaemonSet":
		return statusDaemonSet(u)
	case "Node":
		return statusNode(u)
	case "Service":
		return statusService(u)
	case "PersistentVolumeClaim":
		return statusPVC(u)
	case "Job":
		return statusJob(u)
	case "CronJob":
		return statusCronJob(u)
	case "Namespace":
		phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
		return phase, nil, nil, nil, ""
	default:
		// Generic fallback: the most recent True condition's Type.
		return statusFromConditions(u), nil, nil, nil, ""
	}
}

func ptrInt(n int) *int { return &n }

func statusPod(u *unstructured.Unstructured) (string, *int, *int, *int, string) {
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	host, _, _ := unstructured.NestedString(u.Object, "spec", "nodeName")

	cs, _, _ := unstructured.NestedSlice(u.Object, "status", "containerStatuses")
	total := len(cs)
	ready := 0
	restarts := 0
	// Surface the worst container state as the headline if any container
	// isn't Ready — that's what the user wants to see ("CrashLoopBackOff"
	// is far more useful than "Running" for a pod whose only container
	// is in CrashLoopBackOff).
	var worstReason string
	for _, c := range cs {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if r, _, _ := unstructured.NestedBool(m, "ready"); r {
			ready++
		}
		if n, _, _ := unstructured.NestedInt64(m, "restartCount"); n > 0 {
			restarts += int(n)
		}
		// state has at most one of waiting/running/terminated. Capture
		// the waiting/terminated reason if present.
		if reason, _, _ := unstructured.NestedString(m, "state", "waiting", "reason"); reason != "" && worstReason == "" {
			worstReason = reason
		} else if reason, _, _ := unstructured.NestedString(m, "state", "terminated", "reason"); reason != "" && worstReason == "" && reason != "Completed" {
			worstReason = reason
		}
	}

	// Init containers run (and can fail) before app containers start, so a
	// pod blocked at Init:CrashLoopBackOff / Init:ImagePullBackOff reports
	// the real reason under initContainerStatuses while containerStatuses
	// still looks plain Pending. Their failure takes priority over any app
	// container reason and is prefixed "Init:" to match kubectl. A completed
	// init container (terminated/Completed) is normal and skipped; restarts
	// of a crash-looping init container still matter, so count them.
	ics, _, _ := unstructured.NestedSlice(u.Object, "status", "initContainerStatuses")
	var initReason string
	for _, c := range ics {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if n, _, _ := unstructured.NestedInt64(m, "restartCount"); n > 0 {
			restarts += int(n)
		}
		if reason, _, _ := unstructured.NestedString(m, "state", "waiting", "reason"); reason != "" && initReason == "" {
			initReason = reason
		} else if reason, _, _ := unstructured.NestedString(m, "state", "terminated", "reason"); reason != "" && initReason == "" && reason != "Completed" {
			initReason = reason
		}
	}
	if initReason != "" {
		worstReason = "Init:" + initReason
	}

	summary := phase
	if worstReason != "" && phase != "Succeeded" {
		summary = worstReason
	}
	return summary, ptrInt(ready), ptrInt(total), ptrInt(restarts), host
}

func statusReplicaController(u *unstructured.Unstructured) (string, *int, *int, *int, string) {
	// Total is the desired count (spec.replicas), not status.replicas: while a
	// workload scales up or is freshly created, status.replicas is the observed
	// count and lags desired, so a desired-3 workload with status.replicas=0
	// would otherwise render as "ScaledToZero 0/0". spec.replicas defaults to 1
	// when omitted.
	desired, found, _ := unstructured.NestedInt64(u.Object, "spec", "replicas")
	if !found {
		desired = 1
	}
	ready, _, _ := unstructured.NestedInt64(u.Object, "status", "readyReplicas")

	verdict := "Progressing"
	if desired == 0 {
		verdict = "ScaledToZero"
	} else if ready == desired {
		verdict = "Available"
	}
	return fmt.Sprintf("%s %d/%d", verdict, ready, desired), ptrInt(int(ready)), ptrInt(int(desired)), nil, ""
}

func statusDaemonSet(u *unstructured.Unstructured) (string, *int, *int, *int, string) {
	desired, _, _ := unstructured.NestedInt64(u.Object, "status", "desiredNumberScheduled")
	ready, _, _ := unstructured.NestedInt64(u.Object, "status", "numberReady")
	verdict := "Progressing"
	if ready == desired && desired > 0 {
		verdict = "Available"
	}
	return fmt.Sprintf("%s %d/%d", verdict, ready, desired), ptrInt(int(ready)), ptrInt(int(desired)), nil, ""
}

func statusNode(u *unstructured.Unstructured) (string, *int, *int, *int, string) {
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	isReady := false
	var pressure string
	for _, c := range conds {
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
	ready := 0
	if isReady {
		summary = "Ready"
		ready = 1
	}
	if pressure != "" {
		summary += " (" + pressure + ")"
	}
	return summary, ptrInt(ready), ptrInt(1), nil, ""
}

func statusService(u *unstructured.Unstructured) (string, *int, *int, *int, string) {
	t, _, _ := unstructured.NestedString(u.Object, "spec", "type")
	ip, _, _ := unstructured.NestedString(u.Object, "spec", "clusterIP")
	if t == "" {
		t = "ClusterIP"
	}
	return t + " " + ip, nil, nil, nil, ""
}

func statusPVC(u *unstructured.Unstructured) (string, *int, *int, *int, string) {
	phase, _, _ := unstructured.NestedString(u.Object, "status", "phase")
	ready := 0
	if phase == "Bound" {
		ready = 1
	}
	return phase, ptrInt(ready), ptrInt(1), nil, ""
}

func statusJob(u *unstructured.Unstructured) (string, *int, *int, *int, string) {
	succeeded, _, _ := unstructured.NestedInt64(u.Object, "status", "succeeded")
	failed, _, _ := unstructured.NestedInt64(u.Object, "status", "failed")
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		t, _, _ := unstructured.NestedString(m, "type")
		s, _, _ := unstructured.NestedString(m, "status")
		if s == string(corev1.ConditionTrue) {
			switch t {
			case "Complete":
				return "Complete", ptrInt(1), ptrInt(1), nil, ""
			case "Failed":
				return fmt.Sprintf("Failed (%d)", failed), ptrInt(0), ptrInt(1), nil, ""
			}
		}
	}
	return fmt.Sprintf("Running (%d succeeded)", succeeded), ptrInt(0), ptrInt(1), nil, ""
}

func statusCronJob(u *unstructured.Unstructured) (string, *int, *int, *int, string) {
	active, _, _ := unstructured.NestedSlice(u.Object, "status", "active")
	last, _, _ := unstructured.NestedString(u.Object, "status", "lastScheduleTime")
	if last != "" {
		return fmt.Sprintf("%d active, last %s", len(active), last), nil, nil, nil, ""
	}
	return fmt.Sprintf("%d active", len(active)), nil, nil, nil, ""
}

// statusFromConditions is the CRD fallback. Most operator CRDs put their
// state in .status.conditions[] using the Kubernetes condition convention.
// We surface the type of the most-recently-true condition, optionally
// suffixed with reason if the condition is False (e.g. "Synced (OutOfSync)").
func statusFromConditions(u *unstructured.Unstructured) string {
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	var lastTrue, lastFalseType, lastFalseReason string
	var lastTrueAt time.Time
	haveTrue := false
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		t, _, _ := unstructured.NestedString(m, "type")
		s, _, _ := unstructured.NestedString(m, "status")
		reason, _, _ := unstructured.NestedString(m, "reason")
		lt, _, _ := unstructured.NestedString(m, "lastTransitionTime")
		var at time.Time
		if lt != "" {
			at, _ = time.Parse(time.RFC3339, lt)
		}
		if s == string(corev1.ConditionTrue) {
			// Take the first True condition unconditionally, then prefer any
			// later one with a newer timestamp. Without the haveTrue guard a
			// True condition that omits (or malforms) lastTransitionTime has a
			// zero `at`, never beats the zero lastTrueAt, and would be dropped —
			// falling through to a False/empty summary despite being True.
			if !haveTrue || at.After(lastTrueAt) {
				haveTrue = true
				lastTrueAt = at
				lastTrue = t
			}
		} else if s == string(corev1.ConditionFalse) && lastFalseType == "" {
			lastFalseType = t
			lastFalseReason = reason
		}
	}
	if lastTrue != "" {
		return lastTrue
	}
	if lastFalseType != "" && lastFalseReason != "" {
		return lastFalseType + " (" + lastFalseReason + ")"
	}
	if lastFalseType != "" {
		return lastFalseType
	}
	return ""
}

// lastAppliedAnnotation is kubectl's record of the manifest from the last
// `kubectl apply`. For a Secret applied this way it holds the full original
// object — including .data/.stringData — so we must scrub it alongside the
// top-level secret fields or raw_json would still leak the values.
const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

// redactSecret returns a shallow-modified copy of a Secret's raw map
// with .data and .stringData values replaced by "<redacted>". Keys are
// preserved (key existence isn't sensitive; the values are). The input
// map is not mutated — the caller's *unstructured.Unstructured retains
// its original payload for in-process use (we just don't persist it).
//
// We also scrub metadata.annotations["kubectl.kubernetes.io/last-applied-
// configuration"]: kubectl itself stores the entire applied Secret (data and
// all) there, so it's a first-class leak, not a user mistake. We still don't
// chase "secrets" users hand-place in ConfigMaps or other annotations —
// pretending to redact those would create a false sense of security. The
// contract stays precise: Kubernetes-typed Secrets get their .data/.stringData
// and the kubectl last-applied snapshot scrubbed.
func redactSecret(obj map[string]any) map[string]any {
	out := make(map[string]any, len(obj))
	for k, v := range obj {
		switch k {
		case "data", "stringData":
			if m, ok := v.(map[string]any); ok {
				redacted := make(map[string]any, len(m))
				for key := range m {
					redacted[key] = "<redacted>"
				}
				out[k] = redacted
				continue
			}
			out[k] = v
		case "metadata":
			out[k] = redactSecretMetadata(v)
		default:
			out[k] = v
		}
	}
	return out
}

// redactSecretMetadata returns metadata with the kubectl last-applied annotation
// scrubbed. It copies only when that annotation is present, so the common case
// shares the original map and never mutates the caller's object.
func redactSecretMetadata(v any) any {
	meta, ok := v.(map[string]any)
	if !ok {
		return v
	}
	anns, ok := meta["annotations"].(map[string]any)
	if !ok {
		return v
	}
	if _, has := anns[lastAppliedAnnotation]; !has {
		return v
	}
	annsCopy := make(map[string]any, len(anns))
	for k, val := range anns {
		annsCopy[k] = val
	}
	annsCopy[lastAppliedAnnotation] = "<redacted>"
	metaCopy := make(map[string]any, len(meta))
	for k, val := range meta {
		metaCopy[k] = val
	}
	metaCopy["annotations"] = annsCopy
	return metaCopy
}

// EventRow is the events-table equivalent of ObjectRow.
type EventRow struct {
	UID          string
	InvolvedUID  string
	InvolvedKind string
	InvolvedNS   string
	InvolvedName string
	Type         string
	Reason       string
	Message      string
	FirstSeen    int64
	LastSeen     int64
	Count        int
	RawJSON      []byte
}

// extractEvent supports both core/v1 Event and events.k8s.io/v1 Event,
// which use different field names for the same data. The two API groups
// emit semantically identical data with renamed fields; we normalise.
func extractEvent(u *unstructured.Unstructured) (EventRow, error) {
	rawJSON, err := json.Marshal(u.Object)
	if err != nil {
		return EventRow{}, err
	}
	row := EventRow{
		UID:     string(u.GetUID()),
		RawJSON: rawJSON,
	}
	if row.UID == "" {
		return EventRow{}, fmt.Errorf("event has empty UID")
	}

	// Common: involvedObject is the same on both groups for legacy reasons
	// (events.k8s.io/v1 retains it as `regarding`, but kubelet still emits
	// core/v1 events; check both).
	// Branch on which field group is actually present, not on whether the UID
	// happens to be set: involvedObject.uid is optional (name-only references
	// are valid), so a missing UID must not be mistaken for an events.k8s.io
	// object and clobber a real involvedObject identity.
	if _, ok := u.Object["involvedObject"]; ok {
		row.InvolvedUID, _, _ = unstructured.NestedString(u.Object, "involvedObject", "uid")
		row.InvolvedKind, _, _ = unstructured.NestedString(u.Object, "involvedObject", "kind")
		row.InvolvedNS, _, _ = unstructured.NestedString(u.Object, "involvedObject", "namespace")
		row.InvolvedName, _, _ = unstructured.NestedString(u.Object, "involvedObject", "name")
	} else {
		// events.k8s.io/v1 spelling
		row.InvolvedUID, _, _ = unstructured.NestedString(u.Object, "regarding", "uid")
		row.InvolvedKind, _, _ = unstructured.NestedString(u.Object, "regarding", "kind")
		row.InvolvedNS, _, _ = unstructured.NestedString(u.Object, "regarding", "namespace")
		row.InvolvedName, _, _ = unstructured.NestedString(u.Object, "regarding", "name")
	}

	row.Type, _, _ = unstructured.NestedString(u.Object, "type")
	row.Reason, _, _ = unstructured.NestedString(u.Object, "reason")
	row.Message, _, _ = unstructured.NestedString(u.Object, "message")
	if row.Message == "" {
		// events.k8s.io/v1 spelling
		row.Message, _, _ = unstructured.NestedString(u.Object, "note")
	}
	// Read count from whichever spelling is actually present (core/v1 count,
	// events.k8s.io/v1 series.count, or deprecatedCount) rather than treating
	// a genuine 0 as "field absent". Only when no spelling carries the field
	// do we default to 1 (a singleton event reports no count).
	count, found, _ := unstructured.NestedInt64(u.Object, "count")
	if !found {
		count, found, _ = unstructured.NestedInt64(u.Object, "series", "count")
	}
	if !found {
		count, found, _ = unstructured.NestedInt64(u.Object, "deprecatedCount")
	}
	if !found {
		count = 1
	}
	row.Count = int(count)

	if first, _, _ := unstructured.NestedString(u.Object, "firstTimestamp"); first != "" {
		if t, err := time.Parse(time.RFC3339, first); err == nil {
			row.FirstSeen = t.UnixMilli()
		}
	}
	if last, _, _ := unstructured.NestedString(u.Object, "lastTimestamp"); last != "" {
		if t, err := time.Parse(time.RFC3339, last); err == nil {
			row.LastSeen = t.UnixMilli()
		}
	}
	if row.LastSeen == 0 {
		// events.k8s.io/v1 spellings
		if last, _, _ := unstructured.NestedString(u.Object, "series", "lastObservedTime"); last != "" {
			if t, err := time.Parse(time.RFC3339, last); err == nil {
				row.LastSeen = t.UnixMilli()
			}
		} else if last, _, _ := unstructured.NestedString(u.Object, "deprecatedLastTimestamp"); last != "" {
			if t, err := time.Parse(time.RFC3339, last); err == nil {
				row.LastSeen = t.UnixMilli()
			}
		} else if et, _, _ := unstructured.NestedString(u.Object, "eventTime"); et != "" {
			if t, err := time.Parse(time.RFC3339Nano, et); err == nil {
				row.LastSeen = t.UnixMilli()
			}
		}
	}
	if row.FirstSeen == 0 {
		row.FirstSeen = row.LastSeen
	}
	return row, nil
}
