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

package safe_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/kubetail-org/kstack-app/sidecar/internal/safe"
)

// Each case names what must go and what must survive: a renderer that returns "error" is
// safe and useless, so every shape is asserted from both sides.
func TestSafeStripsCredentialShapesAndKeepsTheDiagnostic(t *testing.T) {
	cases := []struct {
		name string
		in   string
		gone []string
		kept []string
	}{
		{
			name: "a URL's query and userinfo",
			in: `Get "https://svc:hunter2@api.example.com/readyz?token=SEKRIT&verbose=true": ` +
				`dial tcp 10.0.0.1:443: connect: connection refused`,
			gone: []string{"SEKRIT", "hunter2", "token="},
			kept: []string{"api.example.com", "/readyz", "connection refused"},
		},
		{
			name: "a URL's userinfo without a query",
			in:   `dial "https://svc:hunter2@api.example.com/healthz": no route to host`,
			gone: []string{"hunter2", "svc:"},
			kept: []string{"api.example.com", "/healthz", "no route to host"},
		},
		{
			name: "an echoed Authorization header",
			in: "oauth2: cannot fetch token: 401 Unauthorized\n" +
				"Authorization: Basic SEKRIT\n" +
				`Response: {"error":"invalid_grant"}`,
			gone: []string{"SEKRIT"},
			kept: []string{"401 Unauthorized", "invalid_grant"},
		},
		{
			name: "a header written with = rather than :",
			in:   "request rejected\nAuthorization=Basic SEKRIT",
			gone: []string{"SEKRIT"},
			kept: []string{"request rejected", "Authorization"},
		},
		{
			name: "a header whose value carries its own =",
			in:   "Cookie=session=SEKRIT; Path=/\nunexpected redirect",
			gone: []string{"SEKRIT"},
			kept: []string{"unexpected redirect", "Cookie"},
		},
		{
			name: "an echoed Set-Cookie header",
			in:   "Set-Cookie: session=SEKRIT; Path=/\nunexpected redirect",
			gone: []string{"SEKRIT"},
			kept: []string{"unexpected redirect"},
		},
		{
			name: "a bearer token in free text",
			in:   "the server rejected Bearer SEKRIT as expired",
			gone: []string{"SEKRIT"},
			kept: []string{"the server rejected", "as expired"},
		},
		{
			name: "a JWT in free text",
			in:   "id_token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl failed verification",
			gone: []string{"eyJzdWIiOiJhbGljZSJ9", "c2lnbmF0dXJl"},
			kept: []string{"id_token", "failed verification"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := safe.Safe(errors.New(c.in))

			for _, s := range c.gone {
				assert.NotContains(t, got, s)
			}
			for _, s := range c.kept {
				assert.Contains(t, got, s)
			}
		})
	}
}

// A hostname is dots and dashes too, and a bare path is not a URL; nothing here is a
// credential shape, so an ordinary error keeps every character it had.
func TestSafeLeavesAnOrdinaryErrorAlone(t *testing.T) {
	in := "kubestore: open cluster cache 7: database is locked (svc.kube-system.example)"

	assert.Equal(t, in, safe.Safe(errors.New(in)))
}

func TestSafeBoundsALongMessage(t *testing.T) {
	got := safe.Safe(fmt.Errorf("readyz: %s", strings.Repeat("x", 8*1024)))

	assert.LessOrEqual(t, len(got), safe.MaxLen+len("…"))
	assert.True(t, strings.HasSuffix(got, "…"))
	assert.Contains(t, got, "readyz:")
}

func TestSafeRendersANilErrorAsEmpty(t *testing.T) {
	assert.Empty(t, safe.Safe(nil))
}
