package metadata

import (
	u "backup/src/util_test"
	"testing"
)

func TestRepositoryReaddirNotFound(t *testing.T) {
	r := testRepo(t, u.TempFile(t))
	defer r.Close()

	_, err := r.Readdir([]string{"nonexistent"})
	if err == nil {
		t.Fatal("Expected error when reading non-existent directory, but got nil")
	}
}

// TODO wait for implementation of files in the filesystem
// func TestRepositoryReaddirNotDir(t *testing.T) {
