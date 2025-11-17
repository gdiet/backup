package main

import (
	"backup/src/metadata"
	"backup/src/util"
	"log"
	"os"
	"strings"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

type fs struct {
	fuse.FileSystemBase
	repo *metadata.Repository
}

func NewFs(repo *metadata.Repository) *fs {
	log.Printf("Repository created")
	return &fs{repo: repo}
}

// Mkdir creates a new directory.
// https://man7.org/linux/man-pages/man2/mkdir.2.html
// Returns -fuse.ENOENT if the parent path does not exist.
// Returns -fuse.ENOTDIR if the parent path is not a directory.
// Returns -fuse.EEXIST if a child with the same name already exists.
// Returns -fuse.EIO on errors.
func (f *fs) Mkdir(path string, mode uint32) int {
	log.Printf("Mkdir %s", path)
	_, err := f.repo.Mkdir(partsFrom(path))
	switch err {
	case nil:
		return 0
	case metadata.ErrNotFound:
		return -fuse.ENOENT
	case metadata.ErrNotDir:
		return -fuse.ENOTDIR
	case metadata.ErrExists:
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
func (f *fs) Rmdir(path string) int {
	log.Printf("Rmdir %s", path)
	err := f.repo.Rmdir(partsFrom(path))
	switch err {
	case nil:
		return 0
	case metadata.ErrNotFound:
		return -fuse.ENOENT
	case metadata.ErrNotDir:
		return -fuse.ENOTDIR
	case metadata.ErrNotEmpty:
		return -fuse.ENOTEMPTY
	case metadata.ErrIsRoot:
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
func (f *fs) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	log.Printf("Readdir %s", path)

	fill(".", dirStat(), 0)
	fill("..", dirStat(), 0)

	entries, err := f.repo.Readdir(partsFrom(path))
	switch err {
	case nil:
		// continue
	case metadata.ErrNotFound:
		return -fuse.ENOENT
	case metadata.ErrNotDir:
		return -fuse.ENOTDIR
	default:
		util.AssertionFailedf("unexpected error %v in Readdir", err)
		return -fuse.EIO
	}

	for _, entry := range entries {
		var entryStat *fuse.Stat_t
		switch entry := entry.(type) {
		case *metadata.DirEntry:
			entryStat = dirStat()
		case *metadata.FileEntry:
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
func (f *fs) Rename(oldPath string, newPath string) int {
	log.Printf("Rename %s to %s", oldPath, newPath)
	err := f.repo.Rename(partsFrom(oldPath), partsFrom(newPath))
	switch err {
	case nil:
		return 0
	case metadata.ErrNotFound:
		return -fuse.ENOENT
	case metadata.ErrNotDir:
		return -fuse.ENOTDIR
	case metadata.ErrExists:
		return -fuse.EEXIST
	default:
		util.AssertionFailedf("unexpected error %v in Rename", err)
		return -fuse.EIO
	}
}

// Getattr gets the attributes of a file or directory.
// https://man7.org/linux/man-pages/man2/stat.2.html
// Returns -fuse.ENOENT if the path does not exist.
// Returns -fuse.EIO on other errors.
func (f *fs) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	log.Printf("Getattr %s", path)

	// set basic stat, will be overwritten if it's a file
	stat.Mode = fuse.S_IFDIR | 0755
	stat.Mtim = fuse.NewTimespec(time.Now())
	stat.Nlink = 2
	stat.Uid = uint32(os.Getuid())
	stat.Gid = uint32(os.Getgid())

	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 1 && parts[0] == "" { // Root directory
		return 0
	}

	_, entry, err := f.repo.Lookup(parts)
	switch err {
	case nil:
		// continue
	case metadata.ErrNotFound:
		return -fuse.ENOENT
	default:
		util.AssertionFailedf("unexpected error %v in Getattr", err)
		return -fuse.EIO
	}

	switch entry := entry.(type) {
	case *metadata.DirEntry:
		return 0
	case *metadata.FileEntry:
		stat.Mode = fuse.S_IFREG | 0644
		stat.Size = entry.Size()
		return 0
	default:
		util.AssertionFailedf("unexpected entry type %T in Getattr", entry)
		return -fuse.ENOENT
	}
}

// Destroy unmounts the file system.
func (f *fs) Destroy() {
	log.Printf("Unmounting file system...")
	err := f.repo.Close()
	if err != nil {
		log.Printf("Error closing repository: %v", err)
	}
}

func main() {
	var err error
	var tempFile *os.File
	var r *metadata.Repository

	if tempFile, err = os.CreateTemp("", "fs-*.db"); err != nil {
		log.Printf("Failed to create temp file: %v", err)
		os.Exit(1)
	}
	defer func() {
		if e := os.Remove(tempFile.Name()); e == nil {
			log.Printf("Removed temp database file: %s", tempFile.Name())
		} else {
			log.Printf("Failed to remove temp database file: %v", e)
		}
		if err != nil {
			log.Printf("Process finished with error: %v", err)
			os.Exit(1)
		}
	}()
	if err = tempFile.Close(); err != nil {
		log.Printf("Failed to close temp database file: %v", err)
		return
	}
	log.Printf("Using temp database file: %s", tempFile.Name())

	if r, err = metadata.NewRepository(tempFile.Name()); err != nil {
		log.Printf("Failed to create repository: %v", err)
		return
	}
	if _, err := r.Mkdir([]string{"testing"}); err != nil {
		log.Printf("Failed to create directory 'testing': %v", err)
		return
	} else {
		if _, err := r.Mkdir([]string{"testing", "hello"}); err != nil {
			log.Printf("Failed to create directory 'hello': %v", err)
			return
		}
		if _, err := r.Mkdir([]string{"testing", "world"}); err != nil {
			log.Printf("Failed to create directory 'world': %v", err)
			return
		}
	}

	fs := NewFs(r)
	host := fuse.NewFileSystemHost(fs)
	// Using host.SetCapReaddirPlus(true) could save some Getattr calls, but it's not easy to get it right.
	// On FUSE3, we could set host.SetUseIno(true), but I don't see a real benefit yet.
	log.Printf("Mounting file system...")
	host.Mount("", os.Args[1:])
	log.Printf("Finished main execution")
}
