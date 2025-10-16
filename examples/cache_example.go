package main

import (
	"fmt"
	"log"

	"github.com/gdiet/backup/src/cache_old2"
)

func main() {
	// Create a new file cache in /tmp/cache-example
	cache, err := cache_old2.NewFileCache("/tmp/cache-example")
	if err != nil {
		log.Fatalf("Failed to create cache: %v", err)
	}
	defer cache.Close()

	// Example file ID
	fileId := 42

	fmt.Println("=== FileCache Example ===")

	// 1. Write some data
	data := []byte("Hello, FileCache! This is example data.")
	fmt.Printf("Writing data: %q\n", data)

	err = cache.Write(fileId, 0, data)
	if err != nil {
		log.Fatalf("Write failed: %v", err)
	}
	fmt.Println("✓ Data written successfully")

	// 2. Read the data back
	readData, err := cache.Read(fileId, 0, len(data))
	if err != nil {
		log.Fatalf("Read failed: %v", err)
	}
	fmt.Printf("Read data: %q\n", readData)

	// 3. Write at different position
	moreData := []byte(" Additional text.")
	err = cache.Write(fileId, int64(len(data)), moreData)
	if err != nil {
		log.Fatalf("Append write failed: %v", err)
	}
	fmt.Println("✓ Additional data written")

	// 4. Read the complete file
	totalLength := int64(len(data) + len(moreData))
	completeData, err := cache.Read(fileId, 0, int(totalLength))
	if err != nil {
		log.Fatalf("Complete read failed: %v", err)
	}
	fmt.Printf("Complete data: %q\n", completeData)

	// 5. Truncate the file
	newLength := int64(len(data) - 10)
	err = cache.Truncate(fileId, newLength)
	if err != nil {
		log.Fatalf("Truncate failed: %v", err)
	}
	fmt.Printf("✓ File truncated to %d bytes\n", newLength)

	// 6. Read truncated data
	truncatedData, err := cache.Read(fileId, 0, int(newLength))
	if err != nil {
		log.Fatalf("Read after truncate failed: %v", err)
	}
	fmt.Printf("Truncated data: %q\n", truncatedData)

	// 7. Show cache statistics
	stats := cache.GetStats()
	fmt.Printf("\nCache Statistics:\n")
	fmt.Printf("  Base Directory: %s\n", stats["baseDir"])
	fmt.Printf("  Open Files: %d\n", stats["openFiles"])
	fmt.Printf("  Tracked Files: %d\n", stats["trackedFiles"])

	// 8. Create another file
	fileId2 := 123
	err = cache.Write(fileId2, 0, []byte("Second file content"))
	if err != nil {
		log.Fatalf("Second file write failed: %v", err)
	}

	stats = cache.GetStats()
	fmt.Printf("After creating second file:\n")
	fmt.Printf("  Open Files: %d\n", stats["openFiles"])
	fmt.Printf("  File List: %v\n", stats["openFilesList"])

	// 9. Dispose the first file
	err = cache.Dispose(fileId)
	if err != nil {
		log.Fatalf("Dispose failed: %v", err)
	}
	fmt.Printf("✓ File %q disposed\n", fileId)

	stats = cache.GetStats()
	fmt.Printf("After disposing first file:\n")
	fmt.Printf("  Open Files: %d\n", stats["openFiles"])
	fmt.Printf("  File List: %v\n", stats["openFilesList"])

	fmt.Println("\n=== Example completed successfully ===")
}
