package main

import (
	"backup/src/metadata"
	"backup/src/util"
	"log"
	"os"
	"strings"

	"github.com/winfsp/cgofuse/fuse"
)

type fs struct {
	fuse.FileSystemBase
	repo *metadata.Repository
}

func NewFs(repo *metadata.Repository) *fs {
	return &fs{repo: repo}
}

// Readdir reads the contents of a directory.
// Returns -fuse.ENOENT if the path does not exist.
// Returns -fuse.ENOTDIR if the path is not a directory.
// Returns -fuse.EIO on other errors.
func (f *fs) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	log.Printf("Readdir called for path: %s", path)

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

// Getattr gets the attributes of a file or directory.
// Returns -fuse.ENOENT if the path does not exist.
// Returns -fuse.EIO on other errors.
func (f *fs) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	log.Printf("Getattr called for path: %s", path)

	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 1 && parts[0] == "" {
		// Root directory
		stat.Mode = fuse.S_IFDIR | 0755
		stat.Nlink = 2
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
		stat.Mode = fuse.S_IFDIR | 0755
		stat.Nlink = 2
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
	if id, err := r.Mkdir(0, "testing"); err != nil {
		log.Printf("Failed to create directory 'testing': %v", err)
		return
	} else {
		if _, err := r.Mkdir(id, "hello"); err != nil {
			log.Printf("Failed to create directory 'hello': %v", err)
			return
		}
		if _, err := r.Mkdir(id, "world"); err != nil {
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
