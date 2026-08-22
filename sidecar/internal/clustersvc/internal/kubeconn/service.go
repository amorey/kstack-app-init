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

// Package kubeconn holds the connections everything in this service talks to a cluster over.
//
// **Credentials are the unit, not clusters.** What is pooled is one connection per credential
// key, so two kube-contexts aimed at one server as one user are one socket. Nothing in the
// vocabulary of a cluster record expresses that sharing, which is why it is worth stating
// where the pool lives: a reader may not assume one entry per cluster.
//
// The kubeconfig service is what turns a context into those credentials and the key naming
// them. A caller therefore names a context, never credentials — the resolve belongs here, so
// what a connection was built from and what it is stored under cannot disagree.
//
// **Nothing dials yet.** The service is built and driven so its lifecycle is settled before
// anything depends on it; the pool, the leases, and the probe are what land next.
package kubeconn

import (
	"context"

	"k8s.io/client-go/rest"
)

// kubeconfigService resolves one context to credentials and the key naming them. The key
// excludes the context name, so two contexts aimed at one server as one user share an entry.
type kubeconfigService interface {
	RESTConfig(contextName string) (*rest.Config, string, error)
}

// Service is the pool the cluster service dials through.
type Service struct {
	kubecfgSvc kubeconfigService
}

// New returns a Service over the one reader of the user's kubeconfig.
func New(kubecfgSvc kubeconfigService) *Service {
	return &Service{kubecfgSvc: kubecfgSvc}
}

// Start is the lifecycle shape this wears for the cluster service. Nothing runs in the
// background, so its stop func has nothing to end.
func (s *Service) Start(context.Context) (func(context.Context) error, error) {
	return func(context.Context) error { return nil }, nil
}

// Close releases the pool. Nothing is held yet.
func (s *Service) Close() error { return nil }
