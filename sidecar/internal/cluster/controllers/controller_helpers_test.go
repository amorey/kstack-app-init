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

package controllers

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConfigureKubeHTTP2KeepaliveSetsUnsetVars verifies the tightened HTTP/2
// keepalive timeouts are applied to the env vars client-go reads when they are
// unset.
func TestConfigureKubeHTTP2KeepaliveSetsUnsetVars(t *testing.T) {
	t.Setenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS", "")
	t.Setenv("HTTP2_PING_TIMEOUT_SECONDS", "")
	// t.Setenv to "" still leaves the var *set*; unset it so we test the unset path.
	os.Unsetenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS")
	os.Unsetenv("HTTP2_PING_TIMEOUT_SECONDS")

	ConfigureKubeHTTP2Keepalive()

	assert.Equal(t, "10", os.Getenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS"))
	assert.Equal(t, "5", os.Getenv("HTTP2_PING_TIMEOUT_SECONDS"))
}

// TestConfigureKubeHTTP2KeepalivePreservesOverride verifies an operator override
// (already-set env var) is left untouched, so the values stay tunable.
func TestConfigureKubeHTTP2KeepalivePreservesOverride(t *testing.T) {
	t.Setenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS", "0") // 0 disables the health check
	t.Setenv("HTTP2_PING_TIMEOUT_SECONDS", "42")

	ConfigureKubeHTTP2Keepalive()

	assert.Equal(t, "0", os.Getenv("HTTP2_READ_IDLE_TIMEOUT_SECONDS"),
		"an existing override must be preserved")
	assert.Equal(t, "42", os.Getenv("HTTP2_PING_TIMEOUT_SECONDS"),
		"an existing override must be preserved")
}
