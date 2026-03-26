package cdc

import (
	"testing"

	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestJpegChunker_SOSFound(t *testing.T) {
	// SOS marker found in a single Next() call.
	// First returned chunk must span exactly preamble + SOS bytes; the rest equals CDC on the suffix.
	preambleLen := 100
	suffix := testutil.PseudoRandomData(1, 500)
	data := append(make([]byte, preambleLen), 0xFF, 0xDA)
	data = append(data, suffix...)

	chunker := &JpegChunker{normSizeBits: 4}
	chunks := append(chunker.Next(data), chunker.Flush()...)

	require.Equal(t, preambleLen+2, chunks[0])
	ref := NewCDC(4)
	require.Equal(t, append(ref.Next(suffix), ref.Flush()...), chunks[1:])
}

func TestJpegChunker_SOSWithVariousSplits(t *testing.T) {
	// Same data fed in different splits must always produce the same chunks.
	preambleLen := 50
	suffix := testutil.PseudoRandomData(2, 500)
	data := make([]byte, preambleLen+2+len(suffix))
	data[preambleLen] = 0xFF
	data[preambleLen+1] = 0xDA
	copy(data[preambleLen+2:], suffix)

	ref := NewCDC(4)
	expectedChunks := append([]int{preambleLen + 2}, append(ref.Next(suffix), ref.Flush()...)...)

	testCases := []struct {
		name  string
		split int
	}{
		{"just before FF", preambleLen - 1},
		{"at FF (endsWithFF edge case)", preambleLen},
		{"between FF and DA", preambleLen + 1},
		{"just after DA", preambleLen + 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &JpegChunker{normSizeBits: 4}
			chunks := append(c.Next(data[:tc.split]), c.Next(data[tc.split:])...)
			chunks = append(chunks, c.Flush()...)
			require.Equal(t, expectedChunks, chunks)
		})
	}
}

func TestJpegChunker_FFNotDA(t *testing.T) {
	// A 0xFF not followed by 0xDA must not trigger SOS detection; SOS found later.
	preamble := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0xFF, 0x01, 0x00} // 0xFF not followed by 0xDA
	suffix := testutil.PseudoRandomData(3, 200)
	data := append(append(preamble, 0xFF, 0xDA), suffix...)

	chunker := &JpegChunker{normSizeBits: 4}
	chunks := append(chunker.Next(data), chunker.Flush()...)

	require.Equal(t, len(preamble)+2, chunks[0]) // preamble + FF + DA
}

func TestJpegChunker_FallbackToCDC_WhenSOSBeyondLimit(t *testing.T) {
	// If SOS is not found within 256 kB, the chunker must fall back to plain CDC.
	// Replace all 0xFF bytes to prevent accidental SOS detection.
	raw := testutil.PseudoRandomData(99, 300*1024)
	data := make([]byte, len(raw))
	for i, b := range raw {
		if b != 0xFF {
			data[i] = b
		}
	}

	chunker := &JpegChunker{normSizeBits: 4}
	got := append(chunker.Next(data), chunker.Flush()...)

	ref := NewCDC(4)
	expected := append(ref.Next(data), ref.Flush()...)

	require.Equal(t, expected, got)
}
