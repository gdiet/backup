package fs

import (
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

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
