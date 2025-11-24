package main

import (
	"backup/src/fuse"
	"backup/src/meta"
	"log"
	"os"

	cgofuse "github.com/winfsp/cgofuse/fuse"
)

func main() {
	var err error
	var tempFile *os.File
	var r *meta.Metadata

	if tempFile, err = os.CreateTemp("", "FuseFS-*.db"); err != nil {
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

	if r, err = meta.NewMetadata(tempFile.Name()); err != nil {
		log.Printf("Failed to create repository: %v", err)
		return
	}
	if _, err := r.Mkdir([]string{"testing"}); err != nil {
		log.Printf("Failed to create directory 'testing': %v", err)
		return
	} else {
		if _, err := r.Mkdir([]string{"testing", "hello"}); err != nil {
			log.Printf("Failed to create directory 'hello': %v", err)
			return
		}
		if _, err := r.Mkdir([]string{"testing", "world"}); err != nil {
			log.Printf("Failed to create directory 'world': %v", err)
			return
		}
	}

	fs := fuse.NewFS(r)
	host := cgofuse.NewFileSystemHost(fs)
	// Using host.SetCapReaddirPlus(true) could save some Getattr calls, but it's not easy to get it right.
	// On FUSE3, we could set host.SetUseIno(true), but I don't see a real benefit yet.
	log.Printf("Mounting file system...")
	host.Mount("", os.Args[1:])
	log.Printf("Finished main execution")
}
