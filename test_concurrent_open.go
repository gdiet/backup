package main
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

func main() {
	testDir := "/tmp/concurrent-open-test"
	os.MkdirAll(testDir, 0755)
	defer os.RemoveAll(testDir)
	
	filePath := filepath.Join(testDir, "test-file")
	
	fmt.Println("=== Testing Concurrent os.OpenFile ===")
	fmt.Printf("File path: %s\n", filePath)
	
	var wg sync.WaitGroup
	results := make(chan string, 10)
	
	// Start 5 goroutines that try to open the same file simultaneously
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// Try to open the file
			file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
			if err != nil {
				results <- fmt.Sprintf("Goroutine %d: ERROR - %v", id, err)
				return
			}
			
			// Write some data to verify we have a valid file handle
			data := fmt.Sprintf("Data from goroutine %d at %v\n", id, time.Now())
			n, writeErr := file.WriteAt([]byte(data), int64(id*50))
			
			// Close the file
			closeErr := file.Close()
			
			if writeErr != nil {
				results <- fmt.Sprintf("Goroutine %d: SUCCESS open, WRITE ERROR - %v", id, writeErr)
			} else if closeErr != nil {
				results <- fmt.Sprintf("Goroutine %d: SUCCESS open+write, CLOSE ERROR - %v", id, closeErr)
			} else {
				results <- fmt.Sprintf("Goroutine %d: SUCCESS - opened, wrote %d bytes, closed", id, n)
			}
		}(i)
	}
	
	// Wait for all goroutines to complete
	wg.Wait()
	close(results)
	
	// Print results
	fmt.Println("\n=== Results ===")
	successCount := 0
	for result := range results {
		fmt.Println(result)
		if fmt.Sprintf("%s", result)[len(fmt.Sprintf("Goroutine %d: ", 0)):len(fmt.Sprintf("Goroutine %d: SUCCESS", 0))] == "SUCCESS" {
			successCount++
		}
	}
	
	fmt.Printf("\n=== Summary ===\n")
	fmt.Printf("Total attempts: 5\n")
	fmt.Printf("Successful opens: %d\n", successCount)
	
	// Check final file content
	if _, err := os.Stat(filePath); err == nil {
		content, err := os.ReadFile(filePath)
		if err == nil {
			fmt.Printf("Final file size: %d bytes\n", len(content))
			fmt.Printf("File content:\n%s", content)
		}
	}
}