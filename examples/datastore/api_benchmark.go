package main

import (
	"fmt"
	"log"
	"time"

	"github.com/gdiet/backup/src/storage"
)

func main() {
	fmt.Println("=== DataStore API Performance Comparison ===")

	// Create DataStore
	store, err := storage.FileBackedDataStore("../../test-files/api_benchmark", 1000000, 10)
	if err != nil {
		log.Fatalf("Failed to create DataStore: %v", err)
	}
	defer store.Close()

	// Write test data
	testData := []byte("Performance test data for the new getLockedHandle API")
	err = store.Write(0, testData)
	if err != nil {
		log.Fatalf("Failed to write test data: %v", err)
	}

	fmt.Printf("Test data written: %q\n", testData)

	// Benchmark sequential reads (should show improvement)
	fmt.Println("\n=== Sequential Operations Performance ===")

	numOperations := 1000
	start := time.Now()

	for i := 0; i < numOperations; i++ {
		data, warnings := store.Read(0, int64(len(testData)))
		if len(warnings) > 0 {
			fmt.Printf("Warnings: %v\n", warnings)
		}
		if string(data) != string(testData) {
			fmt.Printf("Data mismatch at iteration %d\n", i)
		}
	}

	duration := time.Since(start)
	fmt.Printf("✅ %d sequential reads completed in %v\n", numOperations, duration)
	fmt.Printf("   Average: %.2f reads/ms\n", float64(numOperations)/float64(duration.Nanoseconds())*1000000)
	fmt.Printf("   Per operation: %v\n", duration/time.Duration(numOperations))

	// Test writes
	start = time.Now()
	for i := 0; i < 100; i++ {
		writeData := fmt.Sprintf("Write test %d", i)
		err = store.Write(int64(i*1000), []byte(writeData))
		if err != nil {
			fmt.Printf("Write %d failed: %v\n", i, err)
		}
	}

	writeDuration := time.Since(start)
	fmt.Printf("✅ 100 sequential writes completed in %v\n", writeDuration)
	fmt.Printf("   Average: %.2f writes/ms\n", float64(100)/float64(writeDuration.Nanoseconds())*1000000)
	fmt.Printf("   Per operation: %v\n", writeDuration/time.Duration(100))

	fmt.Println("\n=== Benefits of New getLockedHandle API ===")
	fmt.Println("1. ✅ Cleaner code: handle, unlock, err := ds.getLockedHandle(fileID, forWriting)")
	fmt.Println("2. ✅ Automatic cleanup: defer unlock() handles both lock and release")
	fmt.Println("3. ✅ Less error-prone: No manual lock/unlock management")
	fmt.Println("4. ✅ Consistent pattern: Same as FileCache getLockedFile API")
	fmt.Println("5. ✅ RWMutex benefits: Concurrent reads, exclusive writes")

	fmt.Println("\n=== Performance Test completed! ===")
}
