package metadata

import (
	u "backup/src/util_test"
	"testing"
)

// TODO check whether we need both testRepo and setupTestRepo

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

// setupTestRepo sets up a temporary repository for testing and returns it.
func setupTestRepo(t *testing.T) *Repository {
	t.Helper()
	dbFile := u.TempFile(t)
	repo := testRepo(t, dbFile)
	return repo
}
