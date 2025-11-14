package main

import (
	"backup/src/metadata"
	internal "backup/src/metadata/notinternal"
	"log"
	"os"
	"strings"

	"github.com/winfsp/cgofuse/fuse"
)

type Fs struct {
	fuse.FileSystemBase
	r *metadata.Repository
}

func NewFs(r *metadata.Repository) *Fs {
	return &Fs{r: r}
}

func (f *Fs) Readdir(path string, fill func(name string, stat *fuse.Stat_t, ofst int64) bool, ofst int64, fh uint64) int {
	log.Printf("Readdir called for path: %s", path)

	fill(".", &fuse.Stat_t{Mode: fuse.S_IFDIR | 0755, Nlink: 2}, 0)
	fill("..", &fuse.Stat_t{Mode: fuse.S_IFDIR | 0755, Nlink: 2}, 0)

	parts := strings.Split(strings.Trim(path, "/"), "/")
	log.Printf("Readdir parts: %v - length %d", parts, len(parts))
	var entries []internal.TreeEntry
	var err error
	if len(parts) == 1 && parts[0] == "" {
		entries, err = f.r.Readdir([]string{})
	} else {
		entries, err = f.r.Readdir(parts)
	}
	log.Printf("Readdir entries: %v, err: %v", entries, err)
	if err != nil {
		// TODO distinguish errors, e.g., os.ErrNotExist
		return -fuse.ENOENT
	}
	for _, entry := range entries {
		entryStat := &fuse.Stat_t{}
		switch entry.(type) {
		case *internal.DirEntry:
			entryStat.Mode = fuse.S_IFDIR | 0755
			entryStat.Nlink = 2
		case *internal.FileEntry:
			entryStat.Mode = fuse.S_IFREG | 0644
			// TODO set size from metadata
			entryStat.Size = 0
		default:
			// TODO better error handling or logging
			continue
		}
		fill(entry.GetName(), entryStat, 0)
	}

	return 0
}

func (f *Fs) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	log.Printf("Getattr called for path: %s", path)

	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 1 && parts[0] == "" {
		// Root directory
		stat.Mode = fuse.S_IFDIR | 0755
		stat.Nlink = 2
		return 0
	}

	_, entry, err := f.r.Lookup(parts)
	if err != nil {
		// FIXME distinguish errors, e.g., os.ErrNotExist
		return -fuse.ENOENT
	}

	switch entry.(type) {
	case *internal.DirEntry:
		stat.Mode = fuse.S_IFDIR | 0755
		stat.Nlink = 2
		return 0
	case *internal.FileEntry:
		stat.Mode = fuse.S_IFREG | 0644
		// TODO set size from metadata
		stat.Size = 0
		return 0
	default:
		// TODO better error handling or logging
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
