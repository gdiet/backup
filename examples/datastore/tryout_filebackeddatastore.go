package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/gdiet/backup/src/storage"
)

func main() {
	fmt.Println("=== FileBackedDataStore Test ===")

	// Create test directory
	testDir := "../../test-files/example_datastore"
	os.RemoveAll(testDir) // Clean up if exists

	// Create DataStore with 100,000,000 bytes (100MB) fileSize
	fileSize := int64(100_000_000)
	openFilesSoftLimit := 10

	store, err := storage.FileBackedDataStore(testDir, fileSize, openFilesSoftLimit)
	if err != nil {
		fmt.Printf("Error creating DataStore: %v\n", err)
		return
	}
	defer store.Close()

	fmt.Printf("DataStore created with fileSize: %d bytes (%.1f MB)\n", fileSize, float64(fileSize)/(1024*1024))
	fmt.Printf("Test directory: %s\n", testDir)

	// Define test datasets
	testData := []struct {
		offset      int64
		data        string
		description string
	}{
		{0, "DATA_AT_ZERO", "Offset 0 (first file)"},
		{100_000_000, "DATA_AT_100MB", "Offset 100MB (second file)"},
		{1_200_000_000, "DATA_AT_1200MB", "Offset 1.2GB (13th file)"},
		{33_000_000_000, "DATA_AT_33GB", "Offset 33GB (331st file)"},
		{456_100_000_000, "DATA_AT_456GB", "Offset 456.1GB (4562nd file)"},
		{21_456_100_000_000, "DATA_AT_21TB", "Offset 21.4TB (214562nd file)"},
		{987_456_100_000_000, "DATA_AT_987TB", "Offset 987.4TB (9874562nd file)"},
	}

	fmt.Println("\n=== Writing Test Datasets ===")

	// Write datasets
	for i, test := range testData {
		fmt.Printf("%d. Writing '%s' at offset %d (%s)\n",
			i+1, test.data, test.offset, test.description)

		err := store.Write(test.offset, []byte(test.data))
		if err != nil {
			fmt.Printf("   ❌ Error: %v\n", err)
			continue
		}

		// Calculate expected FileID
		fileID := test.offset / fileSize
		fmt.Printf("   ✅ Written to FileID: %d\n", fileID)
	}

	fmt.Println("\n=== Creating Directory Structure Analysis ===")

	// Analyze all created files
	createdFiles := []string{}

	err = filepath.Walk(testDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() {
			// Relative path to testDir
			relPath, _ := filepath.Rel(testDir, path)
			createdFiles = append(createdFiles, relPath)
		}

		return nil
	})

	if err != nil {
		fmt.Printf("Error during analysis: %v\n", err)
		return
	}

	fmt.Printf("Found files: %d\n", len(createdFiles))

	// Output files sorted
	for _, file := range createdFiles {
		fmt.Printf("  📁 %s\n", file)

		// Show file size
		fullPath := filepath.Join(testDir, file)
		if info, err := os.Stat(fullPath); err == nil {
			fmt.Printf("     Size: %d bytes\n", info.Size())
		}
	}

	fmt.Println("\n=== Directory Structure Analysis ===")

	// Show directory structure
	showDirectoryStructure(testDir, "")

	fmt.Println("\n=== Data Verification ===")

	// Read back and verify written data
	for i, test := range testData {
		fmt.Printf("%d. Reading from offset %d:\n", i+1, test.offset)

		data, warnings := store.Read(test.offset, int64(len(test.data)))

		if len(warnings) > 0 {
			fmt.Printf("   ⚠️  Warnings: %v\n", warnings)
		}

		readString := string(data)
		if readString == test.data {
			fmt.Printf("   ✅ Correctly read: '%s'\n", readString)
		} else {
			fmt.Printf("   ❌ Error! Expected: '%s', Read: '%s'\n", test.data, readString)
		}
	}

	fmt.Println("\n=== FileID Calculation Details ===")
	for _, test := range testData {
		fileID := test.offset / fileSize
		offsetInFile := test.offset % fileSize

		// Calculate actual filename (as in store.go code)
		fileName := fmt.Sprintf("%010d", int(fileID)*int(fileSize)) // Filename is the offset
		dirNames := fmt.Sprintf("%06d", fileID)
		expectedDir := filepath.Join(dirNames[:len(dirNames)-4], dirNames[len(dirNames)-4:len(dirNames)-2])
		expectedPath := filepath.Join(expectedDir, fileName)

		fmt.Printf("Offset %15d → FileID %8d → Path: %s (Position %d in file)\n",
			test.offset, fileID, expectedPath, offsetInFile)
	}

	fmt.Printf("\n✅ Test completed! Check the directory '%s' manually.\n", testDir)
}

func showDirectoryStructure(dir string, indent string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		fmt.Printf("%s❌ Error reading %s: %v\n", indent, dir, err)
		return
	}

	for _, entry := range entries {
		fullPath := filepath.Join(dir, entry.Name())
		relPath, _ := filepath.Rel("../../test-files/example_datastore", fullPath)

		if entry.IsDir() {
			fmt.Printf("%s📁 %s/\n", indent, relPath)
			showDirectoryStructure(fullPath, indent+"  ")
		} else {
			info, _ := entry.Info()
			fmt.Printf("%s📄 %s (%d bytes)\n", indent, relPath, info.Size())
		}
	}
}
