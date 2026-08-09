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
	"sync"
)

// raw_json blobs are the cache's largest column and compress ~4-5x. zlib not gzip: its
// output is deterministic (gzip embeds an mtime) and its frame is ~6 bytes vs ~18. No
// version prefix: a zlib stream starts 0x78 and plain JSON '{' (0x7B), so the format is
// self-identifying and a version scheme can be retrofitted without a migration.

// Both codecs are per-object hot paths, and a flate writer carries a ~325KB window
// (reader ~40KB), so they're pooled. Both are Reset onto the call's buffers, so a
// pooled instance carries no state between calls.
var (
	zlibWriters = sync.Pool{New: func() any { return zlib.NewWriter(io.Discard) }}
	zlibReaders sync.Pool
)

// CompressRaw zlib-compresses b; the result begins with the 0x78 header byte.
func CompressRaw(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := zlibWriters.Get().(*zlib.Writer)
	defer zlibWriters.Put(w)
	w.Reset(&buf)
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	// Close flushes; the writer stays reusable through Reset.
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecompressRaw reverses CompressRaw, erroring if b is not a valid zlib stream.
func DecompressRaw(b []byte) ([]byte, error) {
	src := bytes.NewReader(b)
	r, _ := zlibReaders.Get().(io.ReadCloser)
	if r == nil {
		var err error
		if r, err = zlib.NewReader(src); err != nil {
			return nil, err
		}
	} else if err := r.(zlib.Resetter).Reset(src, nil); err != nil {
		return nil, err
	}
	// A malformed stream's reader is dropped rather than pooled.
	out, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	zlibReaders.Put(r)
	return out, nil
}
