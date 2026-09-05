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

package errors_test

import (
	"testing"

	gqlerrors "github.com/kubetail-org/kstack-app/sidecar/graph/errors"
)

// The `code` extension is the stable half of a GraphQL error — the message is
// prose a client must not branch on.
func TestNewErrorCarriesItsCode(t *testing.T) {
	err := gqlerrors.NewError("KSTACK_TEAPOT", "I'm a teapot")
	if err.Message != "I'm a teapot" {
		t.Errorf("Message = %q, want %q", err.Message, "I'm a teapot")
	}
	if got := err.Extensions["code"]; got != "KSTACK_TEAPOT" {
		t.Errorf("code = %v, want KSTACK_TEAPOT", got)
	}
}

// A validation error names the failing rule beside the shared validation code,
// so a client can tell which input it was without parsing the message.
func TestNewValidationErrorNamesTheRule(t *testing.T) {
	err := gqlerrors.NewValidationError("nonEmpty", "name is required")
	if got := err.Extensions["code"]; got != gqlerrors.ErrValidationError.Extensions["code"] {
		t.Errorf("code = %v, want the shared validation code", got)
	}
	if got := err.Extensions["rule"]; got != "nonEmpty" {
		t.Errorf("rule = %v, want nonEmpty", got)
	}
}
