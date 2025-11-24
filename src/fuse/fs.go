package fuse

import (
	"backup/src/meta"
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

type FuseFS struct {
	fuse.FileSystemBase
	repo *meta.Metadata
}

func NewFS(repo *meta.Metadata) *FuseFS {
	log.Printf("Repository created")
	return &FuseFS{repo: repo}
}

// Destroy unmounts the file system.
func (f *FuseFS) Destroy() {
	log.Printf("Unmounting file system...")
	err := f.repo.Close()
	if err != nil {
		log.Printf("Error closing repository: %v", err)
	}
}
