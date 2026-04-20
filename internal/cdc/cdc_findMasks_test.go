//go:build CDC_findMasks

package cdc

import (
	"encoding/hex"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

/*
bits= 6, fingerprint= 885530688, hex=c76b1bfb1fa37c41a11ea46add6a48d894744d2e566f8ddd78f34cf4929ef5
bits= 7, fingerprint= 458567552, hex=e32c840ec3c0fd581243cea3e828a15c70ce9a7f3b59f9a2aae3eb28cb670f
bits= 8, fingerprint= 222203136, hex=840ec3c0fd581243cea3e828a15c70ce9a7f3b59f9a2aae3eb28cb670f0e97
bits= 9, fingerprint=1137081856, hex=80e29f79c7ead1f14912f238a56d59736908f3fe1ad87a97e8c2cd89d8a479
bits=10, fingerprint= 594332672, hex=c85e4a4772d1e236b3687c343042f6a609fd5f8607214378dd9a6dc1eec900
bits=11, fingerprint=1812879360, hex=3884929f0a59bc2fc9d340117fe954454e84b35ef42a0fac157954dcd75a1a
bits=12, fingerprint=1009160192, hex=c4dd226ef70872f8c93cd4415be5cc8b05643b71438aa6a38f66c629b60b6d
bits=13, fingerprint= 189276160, hex=e27a4fac966cf3f072844d5d9321e36014df3e45220c6024afd6b9613a67ab
bits=14, fingerprint= 693518336, hex=bd78a83f7ab1663dc208963fa3c247973d46c100efc68d0f7a0bd4077c120a
bits=15, fingerprint=1071022080, hex=210132df18bb4a9d49f2d6082dc5c18e2de48bdac180ba4b04ee142f3eee22
bits=16, fingerprint=1846738944, hex=186bac6e9de04619f5ab83781a40ac0f3a8342741349d01991b78bbc401e50
bits=17, fingerprint=2102263808, hex=667a45e8ab7a32a7bdaa2ab25922b69da99ddf8021b37d73fed013dcefc85a
bits=18, fingerprint=1324089344, hex=2f5ecb63ea21f7a7cb9c4eaca3fb271b2ea9d533cb59b81df59da1ad80179a
bits=19, fingerprint= 631767040, hex=56da1e9b41b0c451d0f514035f8a64d86d438092bc0613f0f2c547080cf566
bits=20, fingerprint=1445986304, hex=02ce04bf01b7f928a405740640b0e6dff7ac41393a3df1fc9765d800f175a8
bits=21, fingerprint=1927282688, hex=d8ae2999b133f4fbe11567e57ad83aacc1d96ec3e64c5d19c870707c43bd7d
bits=22, fingerprint=2118123520, hex=1eee79113889bca028067a0a904865a4f841520c91b989fcf437442613f72f
bits=23, fingerprint= 864026624, hex=f3434d919296c0e36a07e4862e15a580fe4894080f79d7272dab74fc4ad53f
bits=24, fingerprint=1358954496, hex=6462daa292cadf5c884a2d91c8c8712ed0a9ee82a555e0148c98dd36ef6421
bits=25, fingerprint=1040187392, hex=416028ccb2bdd1d0aba98cf8d62a72d1920c5d337fc67c808ecb76e86d1cf0
bits=26, fingerprint=1811939328, hex=0ace1e5fa3847452770f789b5182d4354fde390b59e6d02abd5c884005b424
bits=27, fingerprint= 939524096, hex=f8ef85228923de76ebd77f945f2b29cc8bf8f77a3063f2032a5c8432029384
bits=28, fingerprint=1879048192, hex=8c8c6c69234ac9ba773ab5ca0357ea65ae0c9b10a0b4121ac6cb5052e2eab0
bits=29, fingerprint=1610612736, hex=d8d3fccc1f28af42983c9965c77e9c641212d3b3c241ad622de1b1f2b01ff9
bits=30, fingerprint=1073741824, hex=1bb4d1cf6f5cad88d681634f3c086029cc67542cc09884831794325ab15390
bits=31, fingerprint=         0, hex=c1e4c85714eff62c19d6399112736f3d82bc1f15494286dab830c581b78a5e
*/
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

func TestCdc_findLargeMasks(t *testing.T) {
	rnd := rand.NewChaCha8([32]byte{})
	data := make([]byte, 1024*1024+30)
	fingerprint := 0
	_, _ = rnd.Read(data)
	for i := 0; i < 30; i++ {
		// Warm up fingerprint
		fingerprint = fingerprint>>1 ^ table[data[i]]
	}
	mask := 1<<29 - 1
outer:
	for {
		for i := 30; i < len(data); i++ {
			fingerprint = fingerprint>>1 ^ table[data[i]]
			if fingerprint&mask == 0 {
				t.Logf("fingerprint>>29=%d, hex=%s", fingerprint>>29, hex.EncodeToString(data[i-30:i+1]))
				if fingerprint == 0 {
					break outer
				}
			}
		}
		copy(data, data[1024*1024:])
		_, _ = rnd.Read(data[31:])
	}
}

func TestCdc_verifyMask10a(t *testing.T) { // pattern for 8 bit mask
	pattern, _ := hex.DecodeString("840ec3c0fd581243cea3e828a15c70ce9a7f3b59f9a2aae3eb28cb670f0e97")
	cfg, _ := NewConfig(10)
	cdc := cfg.NewCDC()
	data := make([]byte, 1031)
	copy(data[1000:], pattern)
	require.Equal(t, []int(nil), cdc.Next(data))
}

func TestCdc_verifyMask10b(t *testing.T) { // pattern for 9 bit mask
	pattern, _ := hex.DecodeString("80e29f79c7ead1f14912f238a56d59736908f3fe1ad87a97e8c2cd89d8a479")
	cfg, _ := NewConfig(10)
	cdc := cfg.NewCDC()
	data := make([]byte, 1031)
	copy(data[1000:], pattern)
	require.Equal(t, []int{1031}, cdc.Next(data))
}
