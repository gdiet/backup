package fs

import (
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

func Mount(repository, mountPoint string) error {
	log.Printf("Preparing file system...")
	host, err := setup(repository)
	if err != nil {
		return err
	}
	log.Printf("Mounting file system...")
	mount(host, mountPoint)
	log.Printf("Finished main execution.")
	return nil
}

func setup(repository string) (*fuse.FileSystemHost, error) {
	fs, err := newFileSystem(repository)
	if err != nil {
		return nil, err
	}
	host := fuse.NewFileSystemHost(fs)
	return host, nil
	// Using host.SetCapReaddirPlus(true) could save some Getattr calls, but it's not worth the effort.
	// On FUSE3, we could set host.SetUseIno(true), but what would be the benefit?
}

func mount(host *fuse.FileSystemHost, mountPoint string) {
	host.Mount(mountPoint, nil)
}
