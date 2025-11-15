package metadata

import (
	"testing"
)

// testRepo sets up a temporary repository for testing.
// The containing directory is automatically removed when the test and all its subtests complete.
func testRepo(t *testing.T, path string) *Repository {
	t.Helper()
	repo, err := NewRepository(path)
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	return repo
}
