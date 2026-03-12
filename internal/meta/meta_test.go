package meta_test

import (
	"testing"

	"github.com/gdiet/backup/internal/meta"
	"github.com/stretchr/testify/require"
)

func TestBasics(t *testing.T) {
	testWithMetadata(t, func(m *meta.Metadata) {
		// Mkdir
		dir, err := m.Mkdir(path("dir"))
		require.NoError(t, err, "Mkdir failed")
		require.Equal(t, uint64(1), meta.B64u(dir))

		// Readdir
		read, err := m.Readdir(nil)
		require.NoError(t, err, "Readdir failed")
		require.Len(t, read, 1)
		require.Equal(t, "dir", read[0].Name())
		require.IsType(t, &meta.DirEntry{}, read[0])

		// Rename
		err = m.Rename(path("dir"), path("newdir"))
		require.NoError(t, err, "Rename failed")
		read, err = m.Readdir(nil)
		require.NoError(t, err, "Readdir failed after rename")
		require.Len(t, read, 1)
		require.Equal(t, "newdir", read[0].Name())
		require.IsType(t, &meta.DirEntry{}, read[0])

		// Lookup
		entry, err := m.Lookup(path("newdir"))
		require.NoError(t, err, "Lookup failed after rename")
		require.Equal(t, uint64(1), meta.B64u(entry.ID()))
		require.IsType(t, &meta.DirEntry{}, entry)

		// Rmdir
		err = m.Rmdir(path("newdir"))
		require.NoError(t, err, "Rmdir failed")
		read, err = m.Readdir(nil)
		require.NoError(t, err, "Readdir failed after rmdir")
		require.Empty(t, read)
	})
}

func path(elems ...string) []string {
	return elems
}

func testWithMetadata(t *testing.T, testFunc func(m *meta.Metadata)) {
	dir := t.TempDir()
	m, err := meta.NewMetadata(dir)
	require.NoError(t, err, "Failed to create Metadata")
	defer func() {
		require.NoError(t, m.Close(), "Failed to close Metadata")
	}()
	testFunc(m)
}
