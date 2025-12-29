package fs

import (
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

func Mount(repository, mountPoint string) {
	host := setup(repository)
	log.Printf("Mounting file system...")
	mount(host, mountPoint)
	log.Printf("Finished main execution")
}

func setup(repository string) *fuse.FileSystemHost {
	// TODO initialize FS with repository
	fs := &dfs{}
	host := fuse.NewFileSystemHost(fs)
	// Using host.SetCapReaddirPlus(true) could save some Getattr calls, but it's not worth the effort.
	// On FUSE3, we could set host.SetUseIno(true), but what would be the benefit?
	return host
}

func mount(host *fuse.FileSystemHost, mountPoint string) {
	host.Mount(mountPoint, nil)
}
