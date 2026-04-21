//go:build CDC_distribution

package cdc

import (
	cryptoRand "crypto/rand"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCdc_chunkSizeDistribution(t *testing.T) {
	targetSizeBits := 16
	targetSize := 1 << targetSizeBits

	config, err := NewConfig(targetSizeBits)
	require.NoError(t, err)
	chunker := config.NewCDC()

	seed := [32]byte{}
	cryptoRand.Read(seed[:])
	rnd := rand.NewChaCha8(seed)
	data := make([]byte, 1024*1024)

	results := map[int]int{}
	for i := 0; i*len(data) < 1000000*targetSize; i++ {
		_, err = rnd.Read(data)
		require.NoError(t, err)
		for _, chunkSize := range chunker.Next(data) {
			key := 2 * chunkSize / targetSize
			results[key] = results[key] + 1
		}
	}
	t.Log(results)
}
