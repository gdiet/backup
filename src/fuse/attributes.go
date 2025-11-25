package fuse

import (
	"backup/src/fserr"
	"backup/src/meta"
	"backup/src/util"
	"errors"
	"log"
	"os"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

// Getattr gets the attributes of a file or directory:
// https://man7.org/linux/man-pages/man2/stat.2.html
//
//   Returns -fuse.ENOENT if the path does not exist.
//   Returns -fuse.EIO on other errors.
//
// 'fh' is ignored until we find a use case where we need it.
func (f *FS) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	log.Printf("Getattr fh %d - %s", fh, path)

	// set basic stat, will be overwritten if it's a file
	stat.Mode = fuse.S_IFDIR | 0755
	stat.Mtim = fuse.NewTimespec(time.Now())
	stat.Nlink = 2
	stat.Uid = uint32(os.Getuid())
	stat.Gid = uint32(os.Getgid())

	parts := partsFrom(path)
	if len(parts) == 1 && parts[0] == "" { // Root directory
		return 0
	}

	_, entry, err := f.repo.Lookup(parts)
	switch {
	case err == nil:
		// continue
	case errors.Is(err, fserr.NotFound):
		return -fuse.ENOENT
	default:
		util.AssertionFailedf("unexpected error %v in Getattr", err)
		return -fuse.EIO
	}
	switch entry := entry.(type) {
	case *meta.DirEntry:
		// continue
	case *meta.FileEntry:
		stat.Mode = fuse.S_IFREG | 0644
		stat.Size = entry.Size()
	default:
		util.AssertionFailedf("unexpected entry type %T in Getattr", entry)
		return -fuse.ENOENT
	}

	return 0
}

// Utimens changes the access and modification times of a file.
// FIXME not implemented yet.
func (f *FS) Utimens(path string, tmsp []fuse.Timespec) int {
	return -fuse.ENOSYS
}
