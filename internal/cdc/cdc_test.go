package cdc_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/SaveTheRbtz/fastcdc-go"
	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
)

var data = testutil.PseudoRandomData(42, 7*1024*1024)
var expectedChunkSizes = []int{1155789, 1045751, 1216779, 1139656, 1056756, 1192330, 532971}

func TestFastcdc_reference(t *testing.T) {
	// Verify that the github.com/SaveTheRbtz/fastcdc-go reference chunker produces the expected chunk sizes for the given data and options.
	// The reference chunks were calculated using the v0.3.0 release of fastcdc-go.
	chunker, err := fastcdc.NewChunker(bytes.NewReader(data), fastcdc.Options{AverageSize: 1024 * 1024})
	require.NoError(t, err)
	var chunkSizes []int
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		chunkSizes = append(chunkSizes, chunk.Length)
	}
	requireSumIs(t, chunkSizes, 7*1024*1024)
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestCdc_basic(t *testing.T) {
	// Verify chunking in the most basic case.
	chunker := cdc.NewChunker(20)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	requireSumIs(t, chunkSizes, 7*1024*1024)
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestCdc_text(t *testing.T) {
	// Verify chunking of text-like data.
	chunker := cdc.NewChunker(20)
	data := testutil.PseudoRandomText(42, 7*1024*1024)
	chunkSizes := append(chunker.Next([]byte(data)), chunker.Flush()...)
	requireSumIs(t, chunkSizes, 7*1024*1024)
	require.Equal(t, []int{1293604, 1114149, 1278265, 1085032, 1241440, 1132243, 195299}, chunkSizes)
}

func TestCdc_small(t *testing.T) {
	// Verify chunking with very small average chunk sizes.
	chunker := cdc.NewChunker(4)
	chunkSizes := append(chunker.Next(data[:64]), chunker.Flush()...)
	requireSumIs(t, chunkSizes, 64)
	require.Equal(t, []int{14, 29, 21}, chunkSizes)
}

func TestCdc_maxsizeChunk(t *testing.T) {
	// Verify chunking if a chunk in the middle of the data reaches the maximum size.
	modifiedData := append([]byte{}, data...)
	for i := 2 * 1024 * 1024; i < len(modifiedData); i++ {
		modifiedData[i] = byte(i % 256)
	}
	chunker := cdc.NewChunker(20)
	chunkSizes := append(chunker.Next(modifiedData), chunker.Flush()...)
	requireSumIs(t, chunkSizes, 7*1024*1024)
	require.Equal(t, []int{1155789, 1 << 22, 1989939}, chunkSizes)
}

func TestCdc_maxsizeChunkAtEnd(t *testing.T) {
	// Verify chunking if a chunk at the end of the data reaches the maximum size.
	modifiedData := append([]byte{}, data[:1155789+1<<22]...)
	for i := 2 * 1024 * 1024; i < len(modifiedData); i++ {
		modifiedData[i] = byte(i % 256)
	}
	chunker := cdc.NewChunker(20)
	chunkSizes := append(chunker.Next(modifiedData), chunker.Flush()...)
	requireSumIs(t, chunkSizes, 1155789+1<<22)
	require.Equal(t, []int{1155789, 1 << 22}, chunkSizes)
}

func TestCdc_multipartInput(t *testing.T) {
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

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chunker := cdc.NewChunker(20)
			remaining := data
			var chunkSizes []int
			for _, split := range tc.input {
				chunkSizes = append(chunkSizes, chunker.Next(remaining[:split])...)
				remaining = remaining[split:]
			}
			chunkSizes = append(chunkSizes, chunker.Next(remaining)...)
			chunkSizes = append(chunkSizes, chunker.Flush()...)
			requireSumIs(t, chunkSizes, 7*1024*1024)
			require.Equal(t, expectedChunkSizes, chunkSizes)
		})
	}
}

func requireSumIs(t *testing.T, numbers []int, expected int) {
	sum := 0
	for _, size := range numbers {
		sum += size
	}
	require.Equal(t, expected, sum)
}

func BenchmarkCdc(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunker := cdc.NewChunker(20)
		chunker.Next(data)
	}
}
