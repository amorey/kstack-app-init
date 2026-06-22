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

// Hand-written models that live alongside the generated models_gen.go.

package model

import (
	"github.com/kubetail-org/kstack-app/sidecar/internal/controllers"
)

// ClusterStatus backs the GraphQL ClusterStatus type. controllers.ClusterStatus
// can't bind directly: the cache field resolver needs the cluster's ID to
// query its sub-API, and a child resolver only sees its own obj. The
// embedded status binds the durable fields (conditions included) as usual;
// ClusterID is the one piece the wrapper adds.
type ClusterStatus struct {
	controllers.ClusterStatus
	ClusterID controllers.ClusterID
}
