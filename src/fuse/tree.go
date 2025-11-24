package fuse

import (
	"backup/src/fserr"
	"backup/src/meta"
	"backup/src/util"
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

// Mkdir creates a new directory.
// https://man7.org/linux/man-pages/man2/mkdir.2.html
// Returns -fuse.ENOENT if the parent path does not exist.
// Returns -fuse.ENOTDIR if the parent path is not a directory.
// Returns -fuse.EEXIST if a child with the same name already exists.
// Returns -fuse.EIO on errors.
func (f *FuseFS) Mkdir(path string, mode uint32) int {
	log.Printf("Mkdir %s", path)
	_, err := f.repo.Mkdir(partsFrom(path))
	switch err {
	case nil:
		return 0
	case fserr.ErrNotFound:
		return -fuse.ENOENT
	case fserr.ErrNotDir:
		return -fuse.ENOTDIR
	case fserr.ErrExists:
		return -fuse.EEXIST
	default:
		util.AssertionFailedf("unexpected error %v in Mkdir", err)
		return -fuse.EIO
	}
}

// Rmdir removes a directory.
// https://man7.org/linux/man-pages/man2/rmdir.2.html
// Returns -fuse.ENOENT if the path does not exist.
// Returns -fuse.ENOTDIR if the path is not a directory.
// Returns -fuse.ENOTEMPTY if the directory is not empty.
// Returns -fuse.EBUSY if the directory is the root.
func (f *FuseFS) Rmdir(path string) int {
	log.Printf("Rmdir %s", path)
	err := f.repo.Rmdir(partsFrom(path))
	switch err {
	case nil:
		return 0
	case fserr.ErrNotFound:
		return -fuse.ENOENT
	case fserr.ErrNotDir:
		return -fuse.ENOTDIR
	case fserr.ErrNotEmpty:
		return -fuse.ENOTEMPTY
	case fserr.ErrIsRoot:
		return -fuse.EBUSY
	default:
		util.AssertionFailedf("unexpected error %v in Rmdir", err)
		return -fuse.EIO
	}
}

// Readdir reads the contents of a directory.
// https://man7.org/linux/man-pages/man2/readdir.2.html
// Returns -fuse.ENOENT if the path does not exist.
// Returns -fuse.ENOTDIR if the path is not a directory.
// Returns -fuse.EIO on other errors.
func (f *FuseFS) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	log.Printf("Readdir %s", path)

	fill(".", dirStat(), 0)
	fill("..", dirStat(), 0)

	entries, err := f.repo.Readdir(partsFrom(path))
	switch err {
	case nil:
		// continue
	case fserr.ErrNotFound:
		return -fuse.ENOENT
	case fserr.ErrNotDir:
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

// Rename renames a file or directory, moving it to a new location if required.
// https://man7.org/linux/man-pages/man2/rename.2.html
// If oldPath is a directory and newPath is an empty directory, newPath is replaced.
// If oldPath is a file and newPath is an existing file, newPath is replaced.
// If oldPath and newPath exist and are the same, no operation is performed (success).
// Returns -fuse.ENOENT if the source path or a parent of the destination path does not exist.
// Returns -fuse.ENOTDIR if a parent of the destination is not a directory or if trying to rename a directory to a file.
// Returns -fuse.ENOTEMPTY if trying to rename a directory to an existing non-empty directory.
// Returns -fuse.EISDIR if trying to rename a file to a directory.
// Returns -fuse.EINVAL if trying to rename a directory to a subdirectory of itself.
// Returns -fuse.EBUSY if trying to rename the root directory itself.
// Returns -fuse.EIO on other errors.
func (f *FuseFS) Rename(oldPath string, newPath string) int {
	log.Printf("Rename %s to %s", oldPath, newPath)
	err := f.repo.Rename(partsFrom(oldPath), partsFrom(newPath))
	switch err {
	case nil:
		return 0
	case fserr.ErrNotFound:
		return -fuse.ENOENT
	case fserr.ErrNotDir:
		return -fuse.ENOTDIR
	case fserr.ErrExists:
		return -fuse.EEXIST
	default:
		util.AssertionFailedf("unexpected error %v in Rename", err)
		return -fuse.EIO
	}
}
