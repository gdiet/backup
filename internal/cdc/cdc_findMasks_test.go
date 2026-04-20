//go:build CDC_findMasks

package cdc

import (
	"encoding/hex"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCdc_findMasks(t *testing.T) {
	for bits := 6; bits <= 30; bits++ {
		rnd := rand.NewChaCha8([32]byte{})
		data := make([]byte, 1024*1024+30)
		fingerprint := 0
		_, _ = rnd.Read(data)
		for i := 0; i < 30; i++ {
			// Warm up fingerprint
			fingerprint = fingerprint>>1 ^ table[data[i]]
		}
		mask := 1<<bits - 1
		mask2 := 2<<bits - 1
	outer:
		for {
			for i := 30; i < len(data); i++ {
				fingerprint = fingerprint>>1 ^ table[data[i]]
				if fingerprint&mask == 0 && fingerprint&mask2 != 0 {
					t.Logf("bits=%d, i=%d, fingerprint=%d, hex=%s", bits, i, fingerprint, hex.EncodeToString(data[i-30:i+1]))
					break outer
				}
			}
			copy(data, data[1024*1024:])
			_, _ = rnd.Read(data[31:])
		}
	}
}

func TestCdc_verifyMask(t *testing.T) {
	pattern, _ := hex.DecodeString("1bb4d1cf6f5cad88d681634f3c086029cc67542cc09884831794325ab15390")
	cfg, _ := NewCdcConfig(10)
	cdc := cfg.NewCDC()
	data := make([]byte, 1024*1024)
	_ = cdc.Next(data)
	copy(data, pattern)
	require.Equal(t, []int{}, cdc.Next(data))
}
