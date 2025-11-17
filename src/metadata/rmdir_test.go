package metadata

import (
	"testing"
)

func TestRepositoryRmdirSuccessful(t *testing.T) {
	r := testRepo(t)
	defer r.Close()

	// Create directory to remove
	_, err := r.Mkdir([]string{"testDir"})
	if err != nil {
		t.Fatalf("Failed to create directory for removal test: %v", err)
	}

	// Remove the directory
	err = r.Rmdir([]string{"testDir"})
	if err != nil {
		t.Fatalf("Failed to remove directory: %v", err)
	}

	// Verify removal
	entries, err := r.Readdir(nil)
	if err != nil {
		t.Fatalf("Failed to read root directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("Expected 0 entries after removal, found %d", len(entries))
	}
}

func TestRepositoryRmdirFailsForRoot(t *testing.T) {
	r := testRepo(t)
	defer r.Close()

	err := r.Rmdir(nil)
	if err == nil {
		t.Fatal("Expected error when removing directory with empty path, but got nil")
	}
}

func TestRepositoryRmdirFailsNotFound(t *testing.T) {
	r := testRepo(t)
	defer r.Close()

	err := r.Rmdir([]string{"nonexistent", "subdir"})
	if err == nil {
		t.Fatal("Expected error when removing directory with non-existent parent, but got nil")
	}

	err = r.Rmdir([]string{"nonexistent"})
	if err == nil {
		t.Fatal("Expected error when removing non-existent directory, but got nil")
	}
}

// TODO wait for implementation of files in the filesystem
// func TestRepositoryRmdirFailsNotDir(t *testing.T) {
