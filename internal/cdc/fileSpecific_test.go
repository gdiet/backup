package cdc_test

import (
	"io"
	"os"
	"testing"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
	"lukechampine.com/blake3"
)

func TestWithFile(t *testing.T) {
	// this is a broken mp4 file with a large next box size
	// that triggers "panic: runtime error: slice bounds out of range [:-561920]"
	// in hash.go line 47
	// see switchToISO() in fileSpecific.go line 107
	file := "/home/georg/c-privat/cdc/raw-data/724579.mp4"
	f, err := os.Open(file)
	require.NoError(t, err)
	defer f.Close()
	config, err := cdc.NewConfig(20)
	require.NoError(t, err)
	chunker := config.NewFileSpecificChunker()
	fChunker := cdc.NewHashingChunker(blake3.New(20, nil), chunker)
	buf := make([]byte, 64*1024)
	i := 0
	for {
		n, err := f.Read(buf)
		i++
		t.Logf("%d: read %d bytes\n", i, n)
		fChunker.Next(buf[:n])
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}
}

func TestStandardCdcChunking(t *testing.T) {
	// Verify chunking in the most basic case.
	config, err := cdc.NewConfig(20)
	require.NoError(t, err)
	chunker := config.NewFileSpecificChunker()
	data := testutil.PseudoRandomData(42, 7*1024*1024)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	expectedChunkSizes := []int{1606795, 697894, 638611, 642966, 857992, 829401, 524432, 730375, 811566}
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestSixByteData(t *testing.T) {
	config, err := cdc.NewConfig(20)
	require.NoError(t, err)
	chunker := config.NewFileSpecificChunker()
	data := testutil.PseudoRandomData(42, 6)
	chunkSizes := append(chunker.Next(data[:6]), chunker.Flush()...)
	require.Equal(t, []int{6}, chunkSizes)
	// Flushing empty data should return no chunks
	require.Equal(t, 0, len(chunker.Flush()))
}
