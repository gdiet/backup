package fuse

import (
	"backup/src/fserr"
	"backup/src/meta"
	"backup/src/util"
	"errors"
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

// Mkdir creates a new directory:
// https://man7.org/linux/man-pages/man2/mkdir.2.html
//
//   Returns -fuse.ENOENT if the parent path does not exist.
//   Returns -fuse.ENOTDIR if the parent path is not a directory.
//   Returns -fuse.EEXIST if a child with the same name already exists.
//   Returns -fuse.EIO on errors.
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
	default:
		util.AssertionFailedf("unexpected error %v in Mkdir", err)
		return -fuse.EIO
	}

	return 0
}

// Rmdir removes a directory:
// https://man7.org/linux/man-pages/man2/rmdir.2.html
//
//   Returns -fuse.ENOENT if the path does not exist.
//   Returns -fuse.ENOTDIR if the path is not a directory.
//   Returns -fuse.ENOTEMPTY if the directory is not empty.
//   Returns -fuse.EBUSY if the directory is the root.
func (f *FS) Rmdir(path string) int {
	log.Printf("Rmdir - %s", path)

	err := f.repo.Rmdir(partsFrom(path))
	switch {
	case err == nil:
		// continue
	case errors.Is(err, fserr.NotFound):
		return -fuse.ENOENT
	case errors.Is(err, fserr.NotDir):
		return -fuse.ENOTDIR
	case errors.Is(err, fserr.NotEmpty):
		return -fuse.ENOTEMPTY
	case errors.Is(err, fserr.IsRoot):
		return -fuse.EBUSY
	default:
		util.AssertionFailedf("unexpected error %v in Rmdir", err)
		return -fuse.EIO
	}

	return 0
}

// Readdir reads the contents of a directory:
// https://man7.org/linux/man-pages/man2/readdir.2.html
//
//   Returns -fuse.ENOENT if the path does not exist.
//   Returns -fuse.ENOTDIR if the path is not a directory.
//   Returns -fuse.EIO on other errors.
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

// Rename renames a file or directory, moving it to a new location if required:
// https://man7.org/linux/man-pages/man2/rename.2.html
//
// If oldPath is a directory and newPath is an empty directory, newPath is replaced.
// If oldPath is a file and newPath is an existing file, newPath is replaced.
// If oldPath and newPath exist and are the same, no operation is performed (success).
//
//   Returns -fuse.ENOENT if oldPath or a parent of newPath does not exist.
//   Returns -fuse.ENOTDIR if a parent of newPath is not a directory.
//   Returns -fuse.ENOTDIR if renaming a directory to a file.
//   Returns -fuse.ENOTEMPTY if renaming a directory to an existing non-empty directory.
//   Returns -fuse.EISDIR if renaming a file to a directory.
//   Returns -fuse.EINVAL if renaming a directory to a subdirectory of itself.
//   Returns -fuse.EBUSY if renaming the root directory itself.
//   Returns -fuse.EIO on other errors.
func (f *FS) Rename(oldPath string, newPath string) int {
	log.Printf("Rename - %s --> %s", oldPath, newPath)

	err := f.repo.Rename(partsFrom(oldPath), partsFrom(newPath))
	switch {
	case err == nil:
		// continue
	case errors.Is(err, fserr.NotFound):
		return -fuse.ENOENT
	case errors.Is(err, fserr.NotDir):
		return -fuse.ENOTDIR
	case errors.Is(err, fserr.Exists):
		return -fuse.EEXIST
	// FIXME what about other error cases?
	default:
		util.AssertionFailedf("unexpected error %v in Rename", err)
		return -fuse.EIO
	}

	return 0
}
