package cdc_test

import (
	"testing"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestCdc_basic(t *testing.T) {
	// Verify chunking in the most basic case, chunk size 1 MB.
	config, err := cdc.NewCdcConfig(20)
	require.NoError(t, err)
	chunker := config.NewCDC()
	data := testutil.PseudoRandomData(42, 7*1024*1024)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	expectedChunkSizes := []int{1606795, 697894, 638611, 642966, 857992, 829401, 524432, 730375, 811566}
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestCdc_text(t *testing.T) {
	// Verify chunking of text-like data, chunk size 1 kB.
	config, err := cdc.NewCdcConfig(10)
	require.NoError(t, err)
	chunker := config.NewCDC()
	data := testutil.PseudoRandomText(42, 7*1024)
	chunkSizes := append(chunker.Next([]byte(data)), chunker.Flush()...)
	expectedChunkSizes := []int{1633, 1205, 1184, 1535, 1168, 443}
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestCdc_small(t *testing.T) {
	// Verify chunking with very small average chunk sizes (64 B).
	config, err := cdc.NewCdcConfig(6)
	require.NoError(t, err)
	chunker := config.NewFileSpecificChunker()
	data := testutil.PseudoRandomData(42, 7*64)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	expectedChunkSizes := []int{80, 56, 38, 39, 40, 102, 93}
	require.Equal(t, expectedChunkSizes, chunkSizes)
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
			name:  "split at chunk border",
			input: []int{1071508 - 1, 1, 1},
		},
		{
			name:  "split before minSize",
			input: []int{1071508 + 1000},
		},
		{
			name:  "split at minSize border",
			input: []int{1071508 + 1<<19 - 1, 1, 1},
		},
	}

	data := testutil.PseudoRandomData(42, 7*1024*1024)
	expectedChunkSizes := []int{1606795, 697894, 638611, 642966, 857992, 829401, 524432, 730375, 811566}

	config, err := cdc.NewCdcConfig(20)
	require.NoError(t, err)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chunker := config.NewCDC()

			remaining := data
			var chunkSizes []int
			for _, split := range tc.input {
				chunkSizes = append(chunkSizes, chunker.Next(remaining[:split])...)
				remaining = remaining[split:]
			}
			chunkSizes = append(chunkSizes, chunker.Next(remaining)...)
			chunkSizes = append(chunkSizes, chunker.Flush()...)
			require.Equal(t, expectedChunkSizes, chunkSizes)
		})
	}
}

func TestCdc_chunkEndDetection(t *testing.T) {
	// Use a known chunk end pattern to verify that chunks end at the expected positions.
	testCases := []struct {
		name               string
		patternEndsAt      []int
		expectedChunkSizes []int
	}{
		{
			name:               "no chunk end added",
			patternEndsAt:      []int{},
			expectedChunkSizes: []int{1658, 1588, 1536, 1338, 1048},
		},
		//{
		//	// TODO it would be nicer if the minimum chunk size was 512, not 513
		//	name:               "chunk end added right before the start of the first data partition",
		//	patternEndsAt:      []int{512},
		//	expectedChunkSizes: []int{512, 1187, 1547, 1536, 1338, 1048},
		//},
		{
			name:               "chunk end added at the start of the first data partition",
			patternEndsAt:      []int{513},
			expectedChunkSizes: []int{513, 1186, 1547, 1536, 1338, 1048},
		},
		{
			name:               "chunk end added at the start of the mask switch",
			patternEndsAt:      []int{1024},
			expectedChunkSizes: []int{1024, 928, 1294, 1536, 1338, 1048},
		},
		{
			name:               "chunk end added immediately after the mask switch",
			patternEndsAt:      []int{1025},
			expectedChunkSizes: []int{1025, 927, 1294, 1536, 1338, 1048},
		},
	}

	data := testutil.PseudoRandomData(42, 7*1024)

	config, err := cdc.NewCdcConfig(10)
	require.NoError(t, err)

	a := []byte("1234567890")
	b := []byte("xxx")
	copy(a[5-len(b):], b)
	t.Logf("a: %v", string(a))

	pattern := "kR9MVTnItt1y6KUcekTf,wO-ymFECPi"
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chunker := config.NewCDC()
			testdata := make([]byte, len(data))
			copy(testdata, data)
			for _, endAt := range tc.patternEndsAt {
				copy(testdata[endAt-len(pattern):], pattern)
			}
			chunkSizes := append(chunker.Next(testdata), chunker.Flush()...)
			require.Equal(t, tc.expectedChunkSizes, chunkSizes)
		})
	}
}

// go test -bench=BenchmarkCdc -count=11 ./internal/cdc
func BenchmarkCdc(b *testing.B) {
	config, err := cdc.NewCdcConfig(20)
	require.NoError(b, err)
	chunker := config.NewCDC()
	var data = testutil.PseudoRandomData(42, 7*1024*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunker.Next(data)
		chunker.Flush()
	}
}
