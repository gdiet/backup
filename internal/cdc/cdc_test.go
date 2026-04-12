package cdc_test

import (
	"testing"

	"github.com/gdiet/backup/internal/cdc"
	"github.com/gdiet/backup/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestCdc_basic(t *testing.T) {
	// Verify chunking in the most basic case, chunk size 1 MB.
	chunker, err := cdc.NewCDC(20)
	require.NoError(t, err)
	data := testutil.PseudoRandomData(42, 7*1024*1024)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	expectedChunkSizes := []int{1071508, 1189740, 850402, 1430966, 864507, 1842503, 90406}
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestCdc_text(t *testing.T) {
	// Verify chunking of text-like data, chunk size 1 kB.
	chunker, err := cdc.NewCDC(10)
	require.NoError(t, err)
	data := testutil.PseudoRandomText(42, 7*1024)
	chunkSizes := append(chunker.Next([]byte(data)), chunker.Flush()...)
	expectedChunkSizes := []int{2025, 714, 540, 969, 1394, 830, 696}
	require.Equal(t, expectedChunkSizes, chunkSizes)
}

func TestCdc_small(t *testing.T) {
	// Verify chunking with very small average chunk sizes (64 B).
	chunker, err := cdc.NewCDC(6)
	require.NoError(t, err)
	data := testutil.PseudoRandomData(42, 7*64)
	chunkSizes := append(chunker.Next(data), chunker.Flush()...)
	expectedChunkSizes := []int{74, 43, 49, 75, 35, 48, 37, 40, 39, 8}
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
	expectedChunkSizes := []int{1071508, 1189740, 850402, 1430966, 864507, 1842503, 90406}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chunker, err := cdc.NewCDC(20)
			require.NoError(t, err)

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

// go test -bench=BenchmarkCdc -count=11 ./internal/cdc
func BenchmarkCdc(b *testing.B) {
	var data = testutil.PseudoRandomData(42, 7*1024*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunker, _ := cdc.NewCDC(20)
		chunker.Next(data)
		chunker.Flush()
	}
}
