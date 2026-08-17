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

package kubeconn

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeepaliveTightensTheHealthCheck(t *testing.T) {
	// t.Setenv restores on cleanup but leaves the var set, so unset it here: the
	// unset path is the one production takes.
	t.Setenv(readIdleTimeoutEnv, "")
	t.Setenv(pingTimeoutEnv, "")
	os.Unsetenv(readIdleTimeoutEnv)
	os.Unsetenv(pingTimeoutEnv)

	configureHTTP2Keepalive()

	assert.Equal(t, "10", os.Getenv(readIdleTimeoutEnv))
	assert.Equal(t, "5", os.Getenv(pingTimeoutEnv))
}

// The values stay tunable: 0 disables the health check, which is how an operator turns
// it off.
func TestKeepalivePreservesAnOverride(t *testing.T) {
	t.Setenv(readIdleTimeoutEnv, "0")
	t.Setenv(pingTimeoutEnv, "42")

	configureHTTP2Keepalive()

	assert.Equal(t, "0", os.Getenv(readIdleTimeoutEnv))
	assert.Equal(t, "42", os.Getenv(pingTimeoutEnv))
}

// Building the pool is what applies it — the call cannot be forgotten by whoever wires
// the composition root, which is how it went missing before.
func TestNewConfiguresTheKeepalive(t *testing.T) {
	t.Setenv(readIdleTimeoutEnv, "")
	os.Unsetenv(readIdleTimeoutEnv)

	svc := newTestService()
	defer svc.Close()

	assert.Equal(t, "10", os.Getenv(readIdleTimeoutEnv))
}
