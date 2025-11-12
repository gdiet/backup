package main

import (
	"log"
	"os"

	"github.com/winfsp/cgofuse/fuse"
)

type Fs struct {
	fuse.FileSystemBase
}

func NewFs() *Fs {
	self := Fs{}
	return &self
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
	fs := NewFs()
	host := fuse.NewFileSystemHost(fs)
	// Using host.SetCapReaddirPlus(true) could save some Getattr calls, but it's not easy to get it right.
	// On FUSE3, we could set host.SetUseIno(true), but I don't see a real benefit yet.
	host.Mount("", os.Args[1:])
}
