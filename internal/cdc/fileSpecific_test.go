package cdc_test

import (
	"testing"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestStandardCdcChunking(t *testing.T) {
	// Verify chunking in the most basic case.
	chunker, err := cdc.NewFileSpecificChunker(20)
	require.NoError(t, err)
	data := testutil.PseudoRandomData(42, 7*1024*1024)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	expectedChunkSizes := []int{1071508, 1189740, 850402, 1430966, 864507, 1842503, 90406}
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestSixByteData(t *testing.T) {
	chunker, err := cdc.NewFileSpecificChunker(20)
	require.NoError(t, err)
	data := testutil.PseudoRandomData(42, 6)
	chunkSizes := append(chunker.Next(data[:6]), chunker.Flush()...)
	require.Equal(t, []int{6}, chunkSizes)
	// Flushing empty data should return no chunks
	require.Equal(t, 0, len(chunker.Flush()))
}
