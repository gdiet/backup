package fs

import (
	"backup/src/fserr"
	"backup/src/meta"
	"backup/src/repo"
	"backup/src/util"
	"errors"
	"log"
	"os"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

type FS struct {
	fuse.FileSystemBase
	repo *repo.Repository
}

// Getattr gets the attributes of a file or directory:
// https://man7.org/linux/man-pages/man2/stat.2.html
//
//	Returns -fuse.ENOENT if the path does not exist.
//	Returns -fuse.EIO on other errors.
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

// Readdir reads the contents of a directory:
// https://man7.org/linux/man-pages/man2/readdir.2.html
//
//	Returns -fuse.ENOENT if the path does not exist.
//	Returns -fuse.ENOTDIR if the path is not a directory.
//	Returns -fuse.EIO on other errors.
//
// 'ofst' and 'fh' are ignored until we find a use case where we need them.
func (f *FS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	log.Printf("Readdir ofst %d fh %d - %s", ofst, fh, path)

	fill(".", dirStat(), 0)
	fill("..", dirStat(), 0)

	entries, err := f.repo.Readdir(partsFrom(path))
	switch {
	case err == nil:
		// continue
	case errors.Is(err, fserr.NotFound):
		return -fuse.ENOENT
	case errors.Is(err, fserr.NotDir):
		return -fuse.ENOTDIR
	default:
		util.AssertionFailedf("unexpected error %v in Readdir", err)
		return -fuse.EIO
	}

	for _, entry := range entries {
		var entryStat *fuse.Stat_t
		switch entry := entry.(type) {
		case *meta.DirEntry:
			entryStat = dirStat()
		case *meta.FileEntry:
			entryStat = fileStat(entry.Size())
		default:
			util.AssertionFailedf("unexpected entry type %T in Readdir", entry)
			continue
		}
		fill(entry.Name(), entryStat, 0)
	}

	return 0
}

// Mkdir creates a new directory:
// https://man7.org/linux/man-pages/man2/mkdir.2.html
//
//	Returns -fuse.ENOENT if the parent path does not exist.
//	Returns -fuse.ENOTDIR if the parent path is not a directory.
//	Returns -fuse.EEXIST if a child with the same name already exists.
//	Returns -fuse.EIO on errors.
//
// 'mode' is ignored until we find a use case where we need it.
func (f *FS) Mkdir(path string, mode uint32) int {
	log.Printf("Mkdir mode %d - %s", mode, path)

	_, err := f.repo.Mkdir(partsFrom(path))
	switch {
	case err == nil:
		// continue
	case errors.Is(err, fserr.NotFound):
		return -fuse.ENOENT
	case errors.Is(err, fserr.NotDir):
		return -fuse.ENOTDIR
	case errors.Is(err, fserr.Exists):
		return -fuse.EEXIST
	case errors.Is(err, fserr.IO):
		return -fuse.EIO
	default:
		util.AssertionFailedf("unexpected error %v in Mkdir", err)
		return -fuse.EIO
	}

	return 0
}
