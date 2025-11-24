package metadata

import (
	u "backup/src/util_test"
	"testing"
)

// testRepoForPath sets up a repository for testing.
func testRepoForPath(t *testing.T, path string) *Metadata {
	t.Helper()
	repo, err := NewMetadata(path)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	return repo
}

// testRepo sets up a temporary repository for testing and returns it.
// The containing directory is automatically removed when the test and all its subtests complete.
func testRepo(t *testing.T) *Metadata {
	t.Helper()
	repo, err := NewMetadata(u.TempFile(t))
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	return repo
}
