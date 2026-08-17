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
	"strconv"
)

// The env vars apimachinery's transport defaults read, and the values we want. Against
// client-go's 30s/15s defaults these turn a silently-dropped API-server connection into
// a ~15s detection instead of ~45s, which is how fast anything watching a cluster
// notices it is gone. → docs/adr/2026-08-09-connection-probing.md.
const (
	readIdleTimeoutEnv = "HTTP2_READ_IDLE_TIMEOUT_SECONDS"
	pingTimeoutEnv     = "HTTP2_PING_TIMEOUT_SECONDS"

	readIdleTimeoutSeconds = 10
	pingTimeoutSeconds     = 5
)

// configureHTTP2Keepalive tightens client-go's HTTP/2 health check, only where the
// operator has not set a value. The vars are read lazily per transport build, so New
// calling this is enough for every connection the pool goes on to build.
func configureHTTP2Keepalive() {
	setEnvIfUnset(readIdleTimeoutEnv, strconv.Itoa(readIdleTimeoutSeconds))
	setEnvIfUnset(pingTimeoutEnv, strconv.Itoa(pingTimeoutSeconds))
}

func setEnvIfUnset(key, val string) {
	if _, ok := os.LookupEnv(key); !ok {
		_ = os.Setenv(key, val)
	}
}
