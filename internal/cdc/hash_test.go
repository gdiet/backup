package cdc_test

import (
	"testing"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
)

var expectedChunks = []cdc.LengthHash{
	{1606795, []uint8{0x82, 0x06, 0x1a, 0x7f, 0x0c, 0x42, 0xc2, 0x3f, 0x94, 0x0f, 0x09, 0xbe, 0x6c, 0xd8, 0xb1, 0x0b, 0xd4, 0x0c, 0x7e, 0x19}},
	{697894, []uint8{0xae, 0x5c, 0x6e, 0x29, 0xc3, 0x30, 0x10, 0x67, 0x18, 0xc0, 0x1c, 0x1a, 0xb2, 0x70, 0x1a, 0xab, 0x61, 0xdb, 0x81, 0x6f}},
	{638611, []uint8{0xfb, 0x8a, 0xc7, 0x9c, 0x4c, 0x86, 0xd1, 0x9c, 0x44, 0xa8, 0x4c, 0x92, 0x83, 0x1a, 0xff, 0x7c, 0xa5, 0x9e, 0x55, 0x03}},
	{642966, []uint8{0x73, 0xdb, 0xc1, 0x8a, 0x7b, 0x22, 0x99, 0x85, 0xe6, 0x07, 0x4f, 0xae, 0xd9, 0xd7, 0x77, 0x3b, 0xc0, 0xe0, 0x01, 0x3c}},
	{857992, []uint8{0x20, 0x71, 0xc8, 0xfe, 0x31, 0xae, 0xca, 0x66, 0x72, 0xe1, 0x02, 0xf9, 0xe1, 0xfc, 0xbd, 0x57, 0x84, 0x79, 0x58, 0x4b}},
	{829401, []uint8{0xdd, 0x55, 0x0b, 0x3e, 0xf6, 0xa1, 0xd3, 0x5f, 0xd9, 0x36, 0x8b, 0xdd, 0x10, 0xdd, 0x67, 0x8d, 0xac, 0xdc, 0x3f, 0x54}},
	{524432, []uint8{0xd9, 0x89, 0xbb, 0x02, 0x13, 0x9c, 0x9c, 0x43, 0xe9, 0xd2, 0x04, 0x47, 0x4c, 0x0f, 0xb1, 0x6f, 0xac, 0x6a, 0xc0, 0x3d}},
	{730375, []uint8{0x12, 0x54, 0x68, 0xc3, 0x7a, 0x5d, 0x00, 0x55, 0x3c, 0xbd, 0x79, 0x15, 0x33, 0x9c, 0x29, 0x04, 0x64, 0xf8, 0xaa, 0x43}},
	{811566, []uint8{0x47, 0x25, 0xf5, 0x47, 0x87, 0x19, 0x3c, 0x1d, 0x2a, 0xeb, 0x07, 0x7c, 0x6b, 0x12, 0xd6, 0xaf, 0xa6, 0x89, 0xa3, 0x70}},
}

func TestExpectedValues(t *testing.T) {
	expectedChunkSizes := []int{1606795, 697894, 638611, 642966, 857992, 829401, 524432, 730375, 811566}
	for i, _ := range expectedChunks {
		require.Equal(t, expectedChunkSizes[i], expectedChunks[i].Length)
	}
}

func TestHash_basic(t *testing.T) {
	// Verify chunking in the most basic case.
	config, err := cdc.NewConfig(20)
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
	config, err := cdc.NewConfig(20)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cdcChunker := config.NewCDC()
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
	config, err := cdc.NewConfig(20)
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
