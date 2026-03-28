package cdc_test

import (
	"testing"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/stretchr/testify/require"
)

func TestStandardCdcChunking(t *testing.T) {
	// Verify chunking in the most basic case.
	chunker := cdc.NewFileSpecificChunker(20)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	requireSumIs(t, chunkSizes, 7*1024*1024)
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestSixByteData(t *testing.T) {
	chunker := cdc.NewFileSpecificChunker(20)
	chunkSizes := append(chunker.Next(data[:6]), chunker.Flush()...)
	requireSumIs(t, chunkSizes, 6)
	require.Equal(t, []int{6}, chunkSizes)
	// Flushing empty data should return no chunks
	require.Equal(t, 0, len(chunker.Flush()))
}
