// Copyright 2026 The Kubetail Authors
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

package model

import (
	"encoding/json"
	"io"
	"log/slog"

	"github.com/99designs/gqlgen/graphql"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd/api"

	"github.com/kubetail-org/kstack-app/sidecar/graph/errors"
)

// Overload k8s.io/client-go/tools/clientcmd/api.Config
type KubeConfig struct {
	*api.Config
}

// Overload k8s.io/client-go/tools/clientcmd/api.AuthInfo
type KubeConfigAuthInfo struct {
	*api.AuthInfo
	Name string
}

// Overload k8s.io/client-go/tools/clientcmd/api.Cluster
type KubeConfigCluster struct {
	*api.Cluster
	Name string
}

// Overload k8s.io/client-go/tools/clientcmd/api.Context
type KubeConfigContext struct {
	*api.Context
	Name string
}

// KubeConfigExtensions scalar
func MarshalKubeConfigExtensions(val map[string]runtime.Object) graphql.Marshaler {
	return graphql.WriterFunc(func(w io.Writer) {
		err := json.NewEncoder(w).Encode(val)
		if err != nil {
			slog.Error("encode KubeConfigExtensions", "err", err)
		}
	})
}

func UnmarshalKubeConfigExtensions(v interface{}) (map[string]runtime.Object, error) {
	if m, ok := v.(map[string]runtime.Object); ok {
		return m, nil
	}
	return nil, errors.NewValidationError("kubeconfigextensions", "Expected json-encoded string representing map[string]runtime.Object")
}
