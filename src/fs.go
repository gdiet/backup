package main

import (
	"backup/src/metadata"
	"log"
	"os"

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

	stat := &fuse.Stat_t{}
	stat.Mode = fuse.S_IFDIR | 0755
	stat.Nlink = 2

	fill(".", stat, 0)
	fill("..", stat, 0)

	switch path {
	case "/":
		// Root directory contains "testing"
		fill("testing", stat, 0)

	case "/testing":
		// Testing directory contains "hello" and "world"
		fill("hello", stat, 0)
		fill("world", stat, 0)
	}

	return 0
}

func (f *Fs) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
	log.Printf("Getattr called for path: %s", path)

	switch path {
	case "/", "/testing", "/testing/hello", "/testing/world":
		stat.Mode = fuse.S_IFDIR | 0755
		stat.Nlink = 2
		return 0
	default:
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
