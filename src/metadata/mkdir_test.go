package metadata

import (
	u "backup/src/util_test"
	"testing"
)

func TestRepositoryMkdirFailsForRoot(t *testing.T) {
	r := testRepo(t, u.TempFile(t))
	defer r.Close()

	_, err := r.Mkdir(nil)
	if err == nil {
		t.Fatal("Expected error when creating directory with empty path, but got nil")
	}
}

func TestRepositoryMkdirFailsNotFound(t *testing.T) {
	r := testRepo(t, u.TempFile(t))
	defer r.Close()

	_, err := r.Mkdir([]string{"nonexistent", "subdir"})
	if err == nil {
		t.Fatal("Expected error when creating directory with non-existent parent, but got nil")
	}
}

// TODO wait for implementation of files in the filesystem
// func TestRepositoryMkdirFailsNotDir(t *testing.T) {
