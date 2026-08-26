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

package kubestore

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func obj(m map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: m}
}

// A crashing container's reason beats the phase: "Running" is what the phase says
// while the only container restarts forever.
func TestStatusPodReportsTheContainerReasonOverThePhase(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"spec": map[string]any{"nodeName": "node-a"},
		"status": map[string]any{
			"phase": "Running",
			"containerStatuses": []any{
				map[string]any{
					"ready": false, "restartCount": int64(7),
					"state": map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
				},
			},
		},
	})

	got := extractStatus(u)

	assert.Equal(t, "CrashLoopBackOff", got.Summary)
	assert.Equal(t, int64(0), got.Ready.Int64)
	assert.Equal(t, int64(1), got.Total.Int64)
	assert.Equal(t, int64(7), got.Restart.Int64)
	assert.Equal(t, "node-a", got.Host)
}

// spec.replicas, not status.replicas: the observed count lags a scale-up, and a
// desired-3 workload must not read as scaled to zero.
func TestStatusDeploymentCountsAgainstTheDesiredReplicas(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"spec":   map[string]any{"replicas": int64(3)},
		"status": map[string]any{"readyReplicas": int64(1), "replicas": int64(1)},
	})

	got := extractStatus(u)

	assert.Equal(t, "Progressing 1/3", got.Summary)
}

func TestStatusNodeNamesThePressureBehindNotReady(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "v1", "kind": "Node",
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "False"},
			map[string]any{"type": "DiskPressure", "status": "True"},
		}},
	})

	got := extractStatus(u)

	assert.Equal(t, "NotReady (DiskPressure)", got.Summary)
	assert.Equal(t, int64(0), got.Ready.Int64)
}

// A kind with no reader falls back to the conditions convention, which is what most
// CRDs follow.
func TestStatusFallsBackToTheConditionsConvention(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Application",
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Synced", "status": "False", "reason": "OutOfSync"},
		}},
	})

	assert.Equal(t, "Synced (OutOfSync)", extractStatus(u).Summary)
}

// Readers key on api GROUP and Kind, never Kind alone: a CRD named Pod in its own
// group is that user's kind and must reach the fallback rather than a built-in
// reader hunting fields it lacks.
func TestStatusReadersKeyOnTheGroupToo(t *testing.T) {
	u := obj(map[string]any{
		"apiVersion": "example.com/v1", "kind": "Pod",
		"status": map[string]any{"phase": "Running"},
	})

	got := extractStatus(u)

	assert.Empty(t, got.Summary)
	assert.False(t, got.Total.Valid)
}
