package cdc

//
//import (
//	"math/rand/v2"
//	"testing"
//)
//
//func TestCdc_findMasks(t *testing.T) {
//	for bits := 6; bits <= 30; bits++ {
//		rnd := rand.NewChaCha8([32]byte{})
//		data := make([]byte, 1024*1024+30)
//		fingerprint := 0
//		_, _ = rnd.Read(data)
//		for i := 0; i < 30; i++ {
//			// Warm up fingerprint
//			fingerprint = fingerprint>>1 ^ table[data[i]]
//		}
//		mask := 1<<bits - 1
//		mask2 := 2<<bits - 1
//	outer:
//		for {
//			for i := 30; i < len(data); i++ {
//				fingerprint = fingerprint>>1 ^ table[data[i]]
//				if fingerprint&mask == 0 && fingerprint&mask2 != 0 {
//					t.Logf("bits=%d, i=%d, fingerprint=%d", bits, i, fingerprint)
//					break outer
//				}
//			}
//			copy(data, data[1024*1024:])
//			_, _ = rnd.Read(data[31:])
//		}
//	}
//}
