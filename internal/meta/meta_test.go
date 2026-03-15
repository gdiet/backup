package meta_test

import (
	"encoding/binary"
	"testing"

	"github.com/gdiet/backup/internal/meta"
	"github.com/stretchr/testify/require"
)

func TestBasics(t *testing.T) {
	testWithMetadata(t, func(c *meta.Context) {
		// Mkdir
		dirId, err := c.Mkdir(path("dir"))
		require.NoError(t, err, "Mkdir failed")
		require.Equal(t, 1, btoi(dirId))

		// Readdir
		read, err := c.Readdir(nil)
		require.NoError(t, err, "Readdir failed")
		require.Len(t, read, 1)
		require.Equal(t, "dir", read[0].Name())
		require.IsType(t, &meta.DirEntry{}, read[0])

		// Rename
		err = c.Rename(path("dir"), path("newdir"))
		require.NoError(t, err, "Rename failed")
		read, err = c.Readdir(nil)
		require.NoError(t, err, "Readdir failed after rename")
		require.Len(t, read, 1)
		require.Equal(t, "newdir", read[0].Name())
		require.IsType(t, &meta.DirEntry{}, read[0])

		// Lookup
		entry, err := c.Lookup(path("newdir"))
		require.NoError(t, err, "Lookup failed after rename")
		require.Equal(t, 1, btoi(entry.ID()))
		require.IsType(t, &meta.DirEntry{}, entry)

		// Rmdir
		err = c.Rmdir(path("newdir"))
		require.NoError(t, err, "Rmdir failed")
		read, err = c.Readdir(nil)
		require.NoError(t, err, "Readdir failed after rmdir")
		require.Empty(t, read)
	})
}

func btoi(b []byte) int {
	return int(binary.BigEndian.Uint64(b))
}

func path(elems ...string) []string {
	return elems
}

func noError(testFunc func(c *meta.Context)) func(c *meta.Context) error {
	return func(c *meta.Context) error {
		testFunc(c)
		return nil
	}
}

func testWithMetadata(t *testing.T, testFunc func(c *meta.Context)) {
	dir := t.TempDir()
	m, err := meta.NewMetadata(dir)
	require.NoError(t, err, "Failed to create Metadata")
	defer func() {
		require.NoError(t, m.Close(), "Failed to close Metadata")
	}()
	_ = m.Write(noError(testFunc))
}
