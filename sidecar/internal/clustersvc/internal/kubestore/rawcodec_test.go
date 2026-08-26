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

package kubestore

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompressRoundTrips(t *testing.T) {
	body := []byte(`{"kind":"Pod","metadata":{"name":"api-0"}}`)

	packed, err := compressRaw(body)
	require.NoError(t, err)
	out, err := decompressRaw(packed)
	require.NoError(t, err)

	assert.Equal(t, body, out)
}

// The stored blob is self-identifying: a zlib stream opens 0x78, plain JSON '{'.
// That is what lets a version prefix be retrofitted without a migration.
func TestCompressWritesAZlibStream(t *testing.T) {
	packed, err := compressRaw([]byte(`{"a":1}`))
	require.NoError(t, err)

	assert.Equal(t, byte(0x78), packed[0])
}

// Both codecs pool their flate machinery, so a reused instance must carry no state
// from the call before it.
func TestCodecsAreReusable(t *testing.T) {
	for _, body := range [][]byte{[]byte(`{"a":1}`), []byte(strings.Repeat("x", 4096)), {}} {
		packed, err := compressRaw(body)
		require.NoError(t, err)
		out, err := decompressRaw(packed)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(body, out))
	}
}

func TestDecompressRejectsGarbage(t *testing.T) {
	_, err := decompressRaw([]byte("not zlib"))

	assert.Error(t, err)
}
