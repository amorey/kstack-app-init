package clustercache

import (
	"bytes"
	"compress/zlib"
	"io"
)

// raw_json blobs (the full Kubernetes object marshaled to JSON) are the largest
// column in the cache and compress ~4-5x. We store them zlib-compressed.
//
// zlib (not gzip) because its output is deterministic — gzip embeds an mtime,
// so the same input would yield different bytes each write — and its frame is
// ~6 bytes vs gzip's ~18. We deliberately store no version prefix: a zlib
// stream begins with 0x78 while plain JSON begins with '{' (0x7B), so the
// format is self-identifying and a version scheme can be added later without a
// migration of existing rows.

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
