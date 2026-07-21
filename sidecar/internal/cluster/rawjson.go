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

package cluster

import (
	"encoding/json"
	"fmt"
	"io"
)

// RawJSON is the Go binding for the GraphQL `JSON` scalar — a full JSON value that
// crosses the wire verbatim. Its underlying type is a string holding already-
// serialized JSON, deliberately: it stays comparable (so a projection carrying a
// RawJSON body still satisfies cacheDeltaWatch's `T comparable` diff, which is what
// makes an in-place object edit surface as Modified), while MarshalGQL writes the
// stored bytes straight to the response. So the sidecar forwards a cached object's
// decompressed raw_json as-is instead of re-marshaling a parsed map — the behavior
// is explicit and byte-preserving. The empty value is the absent body → null.
type RawJSON string

// MarshalGQL writes the stored JSON bytes verbatim (the value is already serialized
// JSON), or `null` for the empty/absent body.
func (r RawJSON) MarshalGQL(w io.Writer) {
	if r == "" {
		io.WriteString(w, "null")
		return
	}
	io.WriteString(w, string(r))
}

// UnmarshalGQL re-serializes a decoded GraphQL/JSON value back to its JSON bytes so
// RawJSON also works as an input scalar. A nil value is the absent body (empty
// RawJSON). RawJSON is output-only in the current schema, so this exists to satisfy
// gqlgen's scalar contract (a scalar's model must be both Marshaler and Unmarshaler).
func (r *RawJSON) UnmarshalGQL(v any) error {
	if v == nil {
		*r = ""
		return nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("invalid JSON scalar: %w", err)
	}
	*r = RawJSON(b)
	return nil
}
