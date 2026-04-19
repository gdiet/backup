package cdc

//
//import (
//	"math/rand/v2"
//	"testing"
//)
//
//func TestCdc_listChunkSizeByEntropy(t *testing.T) {
//	for bits := 1; bits <= 8; bits++ {
//		rnd := rand.NewChaCha8([32]byte{})
//		data := make([]byte, 1024*1024*11)
//		_, _ = rnd.Read(data)
//		for i := range data {
//			data[i] &= 1<<bits - 1
//		}
//		fingerprint := 0
//		for i := 0; i < 30; i++ {
//			// Warm up fingerprint
//			fingerprint = fingerprint>>1 ^ table[data[i]]
//		}
//		mask := 1<<6 - 1
//		chunk := 0
//		for i := 30; i < len(data); i++ {
//			fingerprint = fingerprint>>1 ^ table[data[i]]
//			if fingerprint&mask == 0 {
//				chunk++
//				if chunk < 10000 {
//					continue
//				}
//				t.Logf("bits=%d, avg=%d", bits, i/10000)
//				break
//			}
//		}
//	}
//}
