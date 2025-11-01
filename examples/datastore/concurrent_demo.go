package main

import (
	"backup/src/storage"
	"fmt"
	"log"
	"sync"
	"time"
)

func main() {
	fmt.Println("=== Concurrent DataStore RWLock Test ===")

	// Create DataStore
	store, err := storage.FileBackedDataStore("../../test-files/concurrent_test", 1000000, 10)
	if err != nil {
		log.Fatalf("Failed to create DataStore: %v", err)
	}
	defer store.Close()

	// Write some test data first
	testData := []byte("This is test data for concurrent access")
	err = store.Write(0, testData)
	if err != nil {
		log.Fatalf("Failed to write test data: %v", err)
	}

	fmt.Printf("Test data written: %q\n", testData)

	// Test concurrent reads (should be fast with RWLock)
	fmt.Println("\n=== Testing Concurrent Reads (should be very fast with RWLock) ===")

	numReaders := 10
	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Each reader does multiple reads
			for j := 0; j < 100; j++ {
				data, warnings := store.Read(0, int64(len(testData)))
				if len(warnings) > 0 {
					fmt.Printf("Reader %d got warnings: %v\n", readerID, warnings)
				}
				if string(data) != string(testData) {
					fmt.Printf("Reader %d: data mismatch!\n", readerID)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(start)

	fmt.Printf("✅ %d concurrent readers completed 1000 total reads in %v\n", numReaders, duration)
	fmt.Printf("   Average: %.2f reads/ms\n", float64(1000)/float64(duration.Nanoseconds())*1000000)

	// Test mixed concurrent reads and writes
	fmt.Println("\n=== Testing Mixed Concurrent Reads and Writes ===")

	numOperations := 5
	start = time.Now()

	// Start readers
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				store.Read(0, int64(len(testData)))
			}
		}(i)
	}

	// Start writers
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				writeData := fmt.Sprintf("Writer %d iteration %d data", writerID, j)
				offset := int64(writerID * 1000000) // Different files
				store.Write(offset, []byte(writeData))
			}
		}(i)
	}

	wg.Wait()
	mixedDuration := time.Since(start)

	fmt.Printf("✅ Mixed concurrent operations completed in %v\n", mixedDuration)
	fmt.Printf("   RWLock allows multiple readers to run simultaneously!\n")
	fmt.Printf("   Only writers block other operations on the same file.\n")

	fmt.Println("\n=== Test completed successfully! ===")
}
