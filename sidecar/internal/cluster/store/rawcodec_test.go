package store

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCompressRawRoundTrip(t *testing.T) {
	cases := map[string][]byte{
		"empty":        {},
		"small object": []byte(`{"kind":"Pod","metadata":{"name":"x"}}`),
		"large repetitive": []byte(`{"items":[` +
			strings.Repeat(`{"kind":"Pod","status":"Running"},`, 2000) + `null]}`),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			compressed, err := CompressRaw(in)
			require.NoError(t, err)
			got, err := DecompressRaw(compressed)
			require.NoError(t, err)
			require.Equal(t, in, got)
		})
	}
}

// Deterministic output is the reason we use zlib rather than gzip: gzip embeds
// an mtime, so the same input would compress to different bytes each time.
func TestCompressRawDeterministic(t *testing.T) {
	in := []byte(`{"kind":"Deployment","spec":{"replicas":3}}`)
	a, err := CompressRaw(in)
	require.NoError(t, err)
	b, err := CompressRaw(in)
	require.NoError(t, err)
	require.Equal(t, a, b)
}

// The stored blob is self-identifying: a zlib stream starts with 0x78, while
// plain JSON starts with '{' (0x7B). That distinction is what lets us skip a
// version prefix and still retrofit one later without a migration.
func TestCompressRawZlibHeader(t *testing.T) {
	compressed, err := CompressRaw([]byte(`{"kind":"Pod"}`))
	require.NoError(t, err)
	require.NotEmpty(t, compressed)
	require.Equal(t, byte(0x78), compressed[0])
}

func TestCompressRawShrinksLargeBlob(t *testing.T) {
	in := bytes.Repeat([]byte(`{"managedFields":[{"manager":"kubelet"}]}`), 1000)
	compressed, err := CompressRaw(in)
	require.NoError(t, err)
	require.Less(t, len(compressed), len(in))
}

func TestDecompressRawRejectsInvalid(t *testing.T) {
	_, err := DecompressRaw([]byte(`{"not":"zlib"}`))
	require.Error(t, err)
}
