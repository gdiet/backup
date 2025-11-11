package main

import (
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
	fill(".", nil, 0)
	fill("..", nil, 0)

	switch path {
	case "/":
		// Root directory contains "testing"
		stat := &fuse.Stat_t{}
		stat.Mode = fuse.S_IFDIR | 0755
		fill("testing", stat, 0)

	case "/testing":
		// Testing directory contains "hello" and "world"
		stat := &fuse.Stat_t{}
		stat.Mode = fuse.S_IFDIR | 0755
		fill("hello", stat, 0)
		fill("world", stat, 0)
	}

	return 0
}

func (f *Fs) Getattr(path string, stat *fuse.Stat_t, fh uint64) int {
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
	host.SetCapReaddirPlus(true)
	host.SetUseIno(true) // FUSE3 only
	host.Mount("", os.Args[1:])
}
