package fs

import (
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

type FS struct {
	fuse.FileSystemBase
}

const contents = "Hello, World!\n"

func (fs *FS) Open(path string, flags int) (errc int, fh uint64) {
	switch path {
	case "/hello.txt":
		return 0, 0
	default:
		return -fuse.ENOENT, ^uint64(0)
	}
}

func (fs *FS) Getattr(path string, stat *fuse.Stat_t, fh uint64) (errc int) {
	switch path {
	case "/":
		stat.Mode = fuse.S_IFDIR | 0555
		return 0
	case "/hello.txt":
		stat.Mode = fuse.S_IFREG | 0444
		stat.Size = int64(len(contents))
		return 0
	default:
		return -fuse.ENOENT
	}
}

func (fs *FS) Read(path string, buff []byte, ofst int64, fh uint64) (n int) {
	endofst := ofst + int64(len(buff))
	if endofst > int64(len(contents)) {
		endofst = int64(len(contents))
	}
	if endofst < ofst {
		return 0
	}
	n = copy(buff, contents[ofst:endofst])
	return
}

func (fs *FS) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64,
	fh uint64) (errc int) {
	fill(".", nil, 0)
	fill("..", nil, 0)
	fill("hello.txt", nil, 0)
	return 0
}

func Mount(mountPoint string) {
	host := Setup()
	log.Printf("Mounting file system...")
	DoMount(host, mountPoint)
	log.Printf("Finished main execution")
}

func Setup() *fuse.FileSystemHost {
	fs := &FS{}
	host := fuse.NewFileSystemHost(fs)
	// Using host.SetCapReaddirPlus(true) could save some Getattr calls, but it's not worth the effort.
	// On FUSE3, we could set host.SetUseIno(true), but what would be the benefit?
	return host
}

func DoMount(host *fuse.FileSystemHost, mountPoint string) {
	host.Mount(mountPoint, nil)
}
