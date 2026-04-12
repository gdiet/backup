package cdc_test

import (
	"testing"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
)

var expectedChunks = []cdc.LengthHash{
	{1071508, []uint8{0xb4, 0x31, 0x6a, 0x15, 0x40, 0xba, 0x31, 0x4c, 0xe0, 0x3c, 0x3, 0x47, 0xcb, 0xd4, 0x2c, 0xca, 0x7a, 0x2b, 0x71, 0x93}},
	{1189740, []uint8{0xd4, 0xa5, 0xfb, 0x8e, 0x85, 0x77, 0x67, 0x89, 0x89, 0x83, 0x79, 0xbf, 0x9, 0x88, 0x10, 0xa1, 0xa8, 0x8e, 0x13, 0xef}},
	{850402, []uint8{0xa5, 0xbd, 0x7b, 0x85, 0xcd, 0xb4, 0x58, 0x4c, 0xae, 0x33, 0x67, 0x3d, 0x73, 0xd5, 0x41, 0x61, 0x2e, 0x32, 0xcf, 0x8b}},
	{1430966, []uint8{0xca, 0x41, 0xfe, 0xb0, 0xb0, 0x50, 0xd6, 0xf8, 0x61, 0x8e, 0x49, 0x50, 0xda, 0xf8, 0xb9, 0x38, 0xa, 0x54, 0xbb, 0xac}},
	{864507, []uint8{0xe, 0xb3, 0x55, 0xce, 0x6, 0xd0, 0x4a, 0x1d, 0xce, 0xd2, 0xef, 0xee, 0x67, 0x42, 0xe4, 0x55, 0x77, 0x7e, 0xa2, 0x7c}},
	{1842503, []uint8{0xf9, 0xd7, 0x84, 0xff, 0xb1, 0xf8, 0xa9, 0xa0, 0xbb, 0xb3, 0x72, 0xc5, 0x51, 0x1c, 0x2a, 0xfe, 0xf0, 0x79, 0x5e, 0xe1}},
	{90406, []uint8{0xc0, 0x71, 0xeb, 0x9f, 0x8e, 0xa6, 0x2b, 0x29, 0xc, 0x47, 0xed, 0xf, 0x24, 0x7b, 0x6f, 0x38, 0xe3, 0xe1, 0x36, 0x2f}},
}

func TestExpectedValues(t *testing.T) {
	expectedChunkSizes := []int{1071508, 1189740, 850402, 1430966, 864507, 1842503, 90406}
	for i, _ := range expectedChunks {
		require.Equal(t, expectedChunkSizes[i], expectedChunks[i].Length)
	}
}

func TestHash_basic(t *testing.T) {
	// Verify chunking in the most basic case.
	config, err := cdc.NewCdcConfig(20)
	require.NoError(t, err)
	cdcChunker := config.NewFileSpecificChunker()
	chunker := cdc.NewHashingChunker(cdcChunker)
	data := testutil.PseudoRandomData(42, 7*1024*1024)
	chunks := append(chunker.Next(data), chunker.Flush()...)
	require.Equal(t, expectedChunks, chunks)
}

func TestHash_multipartInput(t *testing.T) {
	// Verify chunking if the input data is provided in multiple parts, e.g. as it is read from a file.
	testCases := []struct {
		name  string
		input []int
	}{
		{
			name:  "in one piece",
			input: []int{},
		},
		{
			name:  "split before minSize",
			input: []int{1155789 + 1000},
		},
		{
			name:  "split at minSize border",
			input: []int{1155789 + 1<<18 - 1, 1, 1},
		},
		{
			name:  "split at normSize border",
			input: []int{1155789 + 1<<20 - 1, 1, 1},
		},
		{
			name:  "split at chunk border",
			input: []int{1155789 + 1045751 - 1, 1, 1},
		},
	}

	data := testutil.PseudoRandomData(42, 7*1024*1024)
	config, err := cdc.NewCdcConfig(20)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cdcChunker := config.NewFileSpecificChunker()
			chunker := cdc.NewHashingChunker(cdcChunker)
			remaining := data
			var chunks []cdc.LengthHash
			for _, split := range tc.input {
				chunks = append(chunks, chunker.Next(remaining[:split])...)
				remaining = remaining[split:]
			}
			chunks = append(chunks, chunker.Next(remaining)...)
			chunks = append(chunks, chunker.Flush()...)
			require.Equal(t, expectedChunks, chunks)
		})
	}
}

func BenchmarkHash(b *testing.B) {
	config, err := cdc.NewCdcConfig(20)
	require.NoError(b, err)
	cdcChunker := config.NewFileSpecificChunker()
	data := testutil.PseudoRandomData(42, 7*1024*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunker := cdc.NewHashingChunker(cdcChunker)
		chunker.Next(data)
		chunker.Flush()
	}
}
