package fs

import (
	"backup/src/fserr"
	"backup/src/meta"
	"backup/src/repo"
	"backup/src/util"
	"log"
	"os"
	"time"

	"github.com/winfsp/cgofuse/fuse"
)

type fileSystem struct {
	fuse.FileSystemBase
	repo *repo.Repository
}

func newFileSystem(repository string) (*fileSystem, error) {
	r, err := repo.NewRepository(repository)
	if err != nil {
		log.Printf("failed to open repository %s: %v", repository, err)
		return nil, fserr.IO_RAW
	}
	return &fileSystem{repo: r}, nil
}

// Destroy is called when the file system is unmounted, releasing any held resources.
func (f *fileSystem) Destroy() {
	log.Printf("Unmounting file system...")
	err := f.repo.Close()
	if err != nil {
		log.Printf("Error closing repository: %v", err)
	}
}

// Getattr gets the attributes of a file or directory:
// https://man7.org/linux/man-pages/man2/stat.2.html
//
//	Returns -fuse.ENOENT if the path does not exist.
//	Returns -fuse.EIO on other errors.
//
// 'fh' is ignored until we find a use case where we need it.
func (f *fileSystem) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	log.Printf("Getattr fh %d - %s", fh, path)

	// set basic stat, will be overwritten if it's a file
	stat.Mode = fuse.S_IFDIR | 0755
	stat.Mtim = fuse.NewTimespec(time.Now())
	stat.Nlink = 2
	stat.Uid = uint32(os.Getuid())
	stat.Gid = uint32(os.Getgid())

	_, entry, err := f.repo.Lookup(partsFrom(path))
	if fuseErr := mapError(err, fserr.NotFound); fuseErr != 0 {
		return fuseErr
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
func (f *fileSystem) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	log.Printf("Readdir ofst %d fh %d - %s", ofst, fh, path)

	fill(".", dirStat(), 0)
	fill("..", dirStat(), 0)

	entries, err := f.repo.Readdir(partsFrom(path))
	if fuseErr := mapError(err, fserr.NotFound, fserr.NotDir); fuseErr != 0 {
		return fuseErr
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
func (f *fileSystem) Mkdir(path string, mode uint32) int {
	log.Printf("Mkdir mode %d - %s", mode, path)

	_, err := f.repo.Mkdir(partsFrom(path))
	return mapError(err, fserr.NotFound, fserr.NotDir, fserr.Exists, fserr.IO_RAW)
}

// Rmdir removes a directory:
// https://man7.org/linux/man-pages/man2/rmdir.2.html
//
//	Returns -fuse.ENOENT if the path does not exist.
//	Returns -fuse.ENOTDIR if the path is not a directory.
//	Returns -fuse.ENOTEMPTY if the directory is not empty.
//	Returns -fuse.EBUSY if the directory is the root.
func (f *fileSystem) Rmdir(path string) int {
	log.Printf("Rmdir - %s", path)

	err := f.repo.Rmdir(partsFrom(path))
	return mapError(err, fserr.NotFound, fserr.NotDir, fserr.NotEmpty, fserr.IsRoot)
}

// Rename renames a file or directory, moving it to a new location if required:
// https://man7.org/linux/man-pages/man2/rename.2.html
//
// If oldPath is a directory and newPath is an empty directory, newPath is replaced.
// If oldPath is a file and newPath is an existing file, newPath is replaced.
// If oldPath and newPath exist and are the same, no operation is performed (success).
//
//	Returns -fuse.ENOENT if oldPath or a parent of newPath does not exist.
//	Returns -fuse.ENOTDIR if a parent of newPath is not a directory.
//	Returns -fuse.ENOTDIR if renaming a directory to a file.
//	Returns -fuse.ENOTEMPTY if renaming a directory to an existing non-empty directory.
//	Returns -fuse.EISDIR if renaming a file to a directory.
//	Returns -fuse.EINVAL if renaming a directory to a subdirectory of itself.
//	Returns -fuse.EBUSY if renaming the root directory itself.
//	Returns -fuse.EIO on other errors.
func (f *fileSystem) Rename(oldPath string, newPath string) int {
	log.Printf("Rename - %s --> %s", oldPath, newPath)

	err := f.repo.Rename(partsFrom(oldPath), partsFrom(newPath))
	return mapError(err, fserr.NotFound, fserr.NotDir, fserr.NotEmpty, fserr.IsDir, fserr.Invalid, fserr.IsRoot)
}

// Create creates and opens a file:
// https://man7.org/linux/man-pages/man2/open.2.html
//
//	Returns -fuse.ENOENT if the parent path does not exist.
//	Returns -fuse.ENOTDIR if the parent path is not a directory.
//	Returns -fuse.EEXIST if the file already exists.
//
// 'flags' and 'mode' are ignored until we find a use case where we need them.
func (f *fileSystem) Create(path string, flags int, mode uint32) (int, uint64) {
	log.Printf("Create flags %d mode %d - %s", flags, mode, path)

	id, err := f.repo.Mkfile(partsFrom(path))
	return mapError(err, fserr.NotFound, fserr.NotDir, fserr.Exists), id
}
