package main

import (
	"fmt"
	"time"

	"dedupfs/lts"
)

func main() {
	p, o, s := lts.PathOffsetSize(230000000010, 220)
	fmt.Printf("%s %d %d\n", p, o, s)
	start := time.Now()
	for n := 1; n <= 20; n++ {
		for k := uint64(0); k <= 6; k++ {
			for b := uint64(0); b <= 3; b++ {
				_, _ = lts.Read("E:/georg/git/privat/backup/dedupfs-temp/LongTermStorePerformanceTest", b*100000000+k*1000, 1000)
				// fmt.Println(len(buf))
				// fmt.Println(err)
			}
		}
	}
	elapsed := time.Since(start)
	fmt.Printf("page took %s", elapsed)
}
