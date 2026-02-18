package meta_test

import (
	"testing"

	"github.com/gdiet/backup/meta"
)

func TestBasics(t *testing.T) {
	testWithMetadata(t, func(m *meta.Metadata) {
		// Mkdir
		dir, err := m.Mkdir(path("dir"))
		noerr(t, err, "Mkdir failed")
		assert(t, dir == 1, "id1 != 1")

		// Readdir
		read, err := m.Readdir(nil)
		noerr(t, err, "Readdir failed")
		assert(t, len(read) == 1, "expected 1 entry")
		assert(t, read[0].Name() == "dir", "expected entry name 'dir'")
		if _, ok := read[0].(*meta.DirEntry); !ok {
			t.Fatalf("expected entry to be a directory")
		}

		// Rename
		err = m.Rename(path("dir"), path("newdir"))
		noerr(t, err, "Rename failed")
		read, err = m.Readdir(nil)
		noerr(t, err, "Readdir failed after rename")
		assert(t, len(read) == 1, "expected 1 entry after rename")
		assert(t, read[0].Name() == "newdir", "expected entry name 'newdir' after rename")
		if _, ok := read[0].(*meta.DirEntry); !ok {
			t.Fatalf("expected entry to be a directory")
		}

		// Rmdir
		err = m.Rmdir(path("newdir"))
		noerr(t, err, "Rmdir failed")
		read, err = m.Readdir(nil)
		noerr(t, err, "Readdir failed after rmdir")
		assert(t, len(read) == 0, "expected 0 entries after rmdir")
	})
}

func noerr(t *testing.T, err error, msg string) {
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

func assert(t *testing.T, condition bool, msg string) {
	if !condition {
		t.Fatal(msg)
	}
}

func path(elems ...string) []string {
	return elems
}

func testWithMetadata(t *testing.T, testFunc func(m *meta.Metadata)) {
	dir := t.TempDir()
	m, err := meta.NewMetadata(dir)
	if err != nil {
		t.Fatalf("Failed to create Metadata: %v", err)
	}
	defer func() {
		if err := m.Close(); err != nil {
			t.Fatalf("Failed to close Metadata: %v", err)
		}
	}()
	testFunc(m)
}
