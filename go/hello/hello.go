package main

import (
	"dedupfs/lts"
	"fmt"
	"os"
	"time"
)

func main() {
	result := make(chan []byte, 1)
	fman := make(lts.Fman, 1)
	fman <- make(map[string]chan *os.File)
	for l := 1; l <= 10; l++ {
		start := time.Now()
		for n := 1; n <= 20; n++ {
			for k := uint64(0); k <= 6; k++ {
				for b := uint64(0); b <= 3; b++ {
					lts.Read(fman, "E:/georg/git/privat/backup/dedupfs-temp/LongTermStorePerformanceTest", b*100000000+k*1000, 1000, result)
					<-result
					// fmt.Println(len(buf))
					// fmt.Println(err)
				}
			}
		}
		elapsed := time.Since(start)
		fmt.Printf("page took %s\n", elapsed)
	}
}
