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

package store

import (
	"bytes"
	"compress/zlib"
	"io"
)

// raw_json blobs (the full Kubernetes object as JSON) are the cache's largest
// column and compress ~4-5x, so we store them zlib-compressed.
//
// zlib not gzip: its output is deterministic (gzip embeds an mtime, so the same
// input would yield different bytes each write) and its frame is ~6 bytes vs gzip's
// ~18. No version prefix is stored — a zlib stream begins with 0x78 while plain JSON
// begins with '{' (0x7B), so the format is self-identifying and a version scheme can
// be retrofitted later without a migration.

// CompressRaw zlib-compresses b. The result begins with the zlib header byte
// 0x78.
func CompressRaw(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecompressRaw reverses CompressRaw. It returns an error if b is not a valid
// zlib stream.
func DecompressRaw(b []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}
