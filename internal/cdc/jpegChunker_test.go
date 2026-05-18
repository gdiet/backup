package cdc_test

import (
	"testing"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestStandardCdcChunking(t *testing.T) {
	// Verify chunking in the most basic case.
	config, err := cdc.NewConfig(20)
	require.NoError(t, err)
	chunker := config.NewJpegChunker()
	data := testutil.PseudoRandomData(42, 7*1024*1024)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	expectedChunkSizes := []int{1606795, 697894, 638611, 642966, 857992, 829401, 524432, 730375, 811566}
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestSixByteData(t *testing.T) {
	config, err := cdc.NewConfig(20)
	require.NoError(t, err)
	chunker := config.NewJpegChunker()
	data := testutil.PseudoRandomData(42, 6)
	chunkSizes := append(chunker.Next(data[:6]), chunker.Flush()...)
	require.Equal(t, []int{6}, chunkSizes)
	// Flushing empty data should return no chunks
	require.Equal(t, 0, len(chunker.Flush()))
}
