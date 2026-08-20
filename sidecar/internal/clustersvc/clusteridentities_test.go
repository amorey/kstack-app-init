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

package clustersvc

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/amorey/beehive"
	"github.com/kubetail-org/kstack-app/sidecar/internal/clustersvc/internal/kubeidentity"
	"github.com/kubetail-org/kstack-app/sidecar/internal/kubeconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubIdentityClient captures what a probe pass reports. The embedded interface is nil:
// the pass calls nothing else on it.
type stubIdentityClient struct {
	beehive.ControllerClient[ClusterIdentityStatus]
	conditions []Condition
	updated    *ClusterIdentityStatus
	setErr     error
	updateErr  error
}

func (c *stubIdentityClient) SetCondition(_ context.Context, cond Condition) error {
	c.conditions = append(c.conditions, cond)
	return c.setErr
}

func (c *stubIdentityClient) UpdateStatus(_ context.Context, status ClusterIdentityStatus) error {
	c.updated = &status
	return c.updateErr
}

// Within runs fn inline: the pass groups its two writes so a watcher never sees half of
// them, and what this stub records is that both were attempted.
func (c *stubIdentityClient) Within(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

// identityObj is the record a probe pass would be handed for contextName.
func identityObj(contextName string, enabled bool) *beehive.Object[ClusterIdentitySpec, ClusterIdentityStatus] {
	return &beehive.Object[ClusterIdentitySpec, ClusterIdentityStatus]{
		ID:   1,
		Name: ClusterIdentityName(contextName),
		Spec: ClusterIdentitySpec{Context: contextName, Enabled: enabled},
	}
}

// fakeKubeidentity answers per context from a map, and records who was asked for and
// who was forgotten.
type fakeKubeidentity struct {
	states    map[string]answer
	asked     []string
	forgotten []string
}

// answer pairs what the service knows with whether it knows anything, so a test can
// hand back "asked, unanswered" as easily as an answer.
type answer struct {
	state kubeidentity.State
	known bool
}

func (f *fakeKubeidentity) Get(contextName string) (kubeidentity.State, bool) {
	f.asked = append(f.asked, contextName)
	s := f.states[contextName]
	return s.state, s.known
}

func (f *fakeKubeidentity) Forget(contextName string) {
	f.forgotten = append(f.forgotten, contextName)
}

func (f *fakeKubeidentity) Subscribe() kubeidentity.Subscription {
	panic("the cluster identity pass reads, it does not subscribe")
}

// identityControllerOver returns a controller whose identity service answers with what,
// and whose kubeconfig service is never read: resolving is the service's now.
func identityControllerOver(t *testing.T, what *fakeKubeidentity) *clusterIdentityController {
	t.Helper()
	d := newTestDeps(t)
	d.kubeidentitySvc = what
	return &clusterIdentityController{deps: d}
}

// answering is a service that answers for "prod" with an identity and an error.
func answering(id kubeidentity.Identity, err error) *fakeKubeidentity {
	return &fakeKubeidentity{states: map[string]answer{
		"prod": {state: kubeidentity.State{Identity: id, Err: err}, known: true},
	}}
}

// unanswered is a service that has been asked and has nothing yet.
func unanswered() *fakeKubeidentity { return &fakeKubeidentity{states: map[string]answer{}} }

// reportedCondition is the one condition a pass wrote, or a failure when it wrote none.
func reportedCondition(t *testing.T, client *stubIdentityClient) Condition {
	t.Helper()
	require.Len(t, client.conditions, 1, "a pass reports once")
	return client.conditions[0]
}

// Every condition here describes process-scoped state, so a previous process's write
// must read as Unknown until this one re-confirms it.
func TestIdentityReconcileReportsLiveness(t *testing.T) {
	c := identityControllerOver(t, answering(kubeidentity.Identity{ServerUID: "uid-1"}, nil))
	client := &stubIdentityClient{}

	c.Reconcile(context.Background(), client, identityObj("prod", true))

	assert.True(t, reportedCondition(t, client).Liveness)
}

// The write the whole subtree waits on: a probed identity is what a cluster projects
// and what its cache is named for.
func TestIdentityReconcileWritesWhatTheProbeFound(t *testing.T) {
	c := identityControllerOver(t, answering(kubeidentity.Identity{
		ServerUID: "uid-1", ServerVersion: "v1.29.3", Username: "admin@example",
	}, nil))
	client := &stubIdentityClient{}

	res := c.Reconcile(context.Background(), client, identityObj("prod", true))

	require.NotNil(t, client.updated)
	require.NotNil(t, client.updated.ServerUID)
	assert.Equal(t, "uid-1", *client.updated.ServerUID)
	assert.Equal(t, "v1.29.3", *client.updated.ServerVersion)
	assert.Equal(t, "admin@example", *client.updated.Username)

	cond := reportedCondition(t, client)
	assert.Equal(t, string(ConditionConnected), cond.Type)
	assert.Equal(t, ConditionTrue, cond.Status)
	assert.Equal(t, ReasonConnected, cond.Reason)
	assert.Equal(t, beehive.Settled(), res)
}

// A fact the probe could not read stays nil rather than empty: nil is what a cluster's
// projection reads as "no better answer than the one I have". Connected stays true
// through it — a user refused kube-system reached a cluster that is up.
func TestIdentityReconcileWritesNoFieldTheProbeMissed(t *testing.T) {
	c := identityControllerOver(t, answering(kubeidentity.Identity{ServerVersion: "v1.29.3"}, nil))
	client := &stubIdentityClient{}

	c.Reconcile(context.Background(), client, identityObj("prod", true))

	require.NotNil(t, client.updated)
	assert.Nil(t, client.updated.ServerUID)
	assert.Nil(t, client.updated.Username)
	assert.Equal(t, ConditionTrue, reportedCondition(t, client).Status)
}

// A cluster that is down is not this pass's failure. Settling leaves the retry to the
// cadence this kind is registered with, rather than to a backoff ladder climbing for a
// server that is simply switched off.
func TestIdentityReconcileReportsAFailedProbe(t *testing.T) {
	c := identityControllerOver(t, answering(kubeidentity.Identity{},
		fmt.Errorf("%w: connection refused", kubeidentity.ErrProbe)))
	client := &stubIdentityClient{}

	res := c.Reconcile(context.Background(), client, identityObj("prod", true))

	cond := reportedCondition(t, client)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonProbeFailed, cond.Reason)
	assert.Contains(t, cond.Message, "connection refused")
	assert.Nil(t, client.updated, "a failed probe improves on nothing")
	assert.Equal(t, beehive.Settled(), res)
}

// A store that refuses the write is this pass's failure: settling would report a
// verdict nothing recorded.
func TestIdentityReconcileReportsAFailedStatusWrite(t *testing.T) {
	boom := errors.New("boom")
	c := identityControllerOver(t, answering(kubeidentity.Identity{ServerUID: "uid-1"}, nil))

	res := c.Reconcile(context.Background(), &stubIdentityClient{updateErr: boom}, identityObj("prod", true))

	assert.Equal(t, fmt.Errorf("report cluster identity: %w", boom), res.Err())
}

// A cluster the user switched off is not probed, and says so rather than looking like
// one that failed to connect.
func TestIdentityReconcileReportsInactiveWhenDisabled(t *testing.T) {
	svc := unanswered()
	c := identityControllerOver(t, svc)
	client := &stubIdentityClient{}

	res := c.Reconcile(context.Background(), client, identityObj("prod", false))

	assert.Empty(t, svc.asked, "asking is what starts a probe, so a disabled one is not asked for")
	// Not asking is not enough: a cluster switched off after it was on has a loop running
	// from the passes that did ask, and only this ends it.
	assert.Equal(t, []string{"prod"}, svc.forgotten)

	cond := reportedCondition(t, client)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonInactive, cond.Reason)
	assert.Equal(t, beehive.Settled(), res)
}

// A context the user removed is not a broken one: there is nothing to connect to until
// they put it back, which reads differently from a file that will not resolve.
func TestIdentityReconcileReportsInactiveForADepartedContext(t *testing.T) {
	c := identityControllerOver(t, answering(kubeidentity.Identity{}, kubeconfig.ErrContextNotFound))
	client := &stubIdentityClient{}

	res := c.Reconcile(context.Background(), client, identityObj("prod", true))

	cond := reportedCondition(t, client)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonInactive, cond.Reason)
	assert.Equal(t, beehive.Settled(), res)
}

// The context is there and its entries do not resolve — a file the user has to fix. The
// record reports it rather than failing the pass, and carries what went wrong.
func TestIdentityReconcileReportsAFailedResolve(t *testing.T) {
	c := identityControllerOver(t, answering(kubeidentity.Identity{}, errors.New("no such certificate authority file")))
	client := &stubIdentityClient{}

	res := c.Reconcile(context.Background(), client, identityObj("prod", true))

	cond := reportedCondition(t, client)
	assert.Equal(t, ConditionFalse, cond.Status)
	assert.Equal(t, ReasonResolveFailed, cond.Reason)
	assert.Contains(t, cond.Message, "certificate authority")
	assert.Equal(t, beehive.Settled(), res)
}

// The read asks for the probe and returns whatever is known, which on the first pass is
// nothing. Neither connected nor failed, and the signal the probe publishes is what
// brings this record back.
func TestIdentityReconcileReportsConnectingUntilAnswered(t *testing.T) {
	svc := unanswered()
	c := identityControllerOver(t, svc)
	client := &stubIdentityClient{}

	res := c.Reconcile(context.Background(), client, identityObj("prod", true))

	assert.Equal(t, []string{"prod"}, svc.asked, "asking is what queues the probe")
	cond := reportedCondition(t, client)
	assert.Equal(t, ConditionUnknown, cond.Status)
	assert.Equal(t, ReasonConnecting, cond.Reason)
	assert.Equal(t, beehive.Settled(), res)
}

// A record on its way out is collected with its owner, so a condition written here is
// one the GC is already coming for.
func TestIdentityReconcileReportsNothingForADyingRecord(t *testing.T) {
	svc := answering(kubeidentity.Identity{ServerUID: "uid-1"}, nil)
	c := identityControllerOver(t, svc)
	obj := identityObj("prod", true)
	now := time.Now()
	obj.DeletionRequestedAt = &now
	client := &stubIdentityClient{}

	res := c.Reconcile(context.Background(), client, obj)

	assert.Empty(t, client.conditions)
	// The record is going and the probe behind it would not: the GC collects rows, not
	// the work a pass asked for.
	assert.Equal(t, []string{"prod"}, svc.forgotten)
	assert.Equal(t, beehive.Settled(), res)
}

// A condition the store refuses is this pass's failure, not the cluster's: leaving it
// settled would report a verdict nothing recorded.
func TestIdentityReconcileReportsAFailedConditionWrite(t *testing.T) {
	boom := errors.New("boom")
	c := identityControllerOver(t, unanswered())

	res := c.Reconcile(context.Background(), &stubIdentityClient{setErr: boom}, identityObj("prod", false))

	assert.Equal(t, fmt.Errorf("set cluster identity condition: %w", boom), res.Err())
}
