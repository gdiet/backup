package fuse

import (
	"backup/src/repo"
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

type FuseFS struct {
	fuse.FileSystemBase
	repo *repo.Repository
}

func NewFS(repository *repo.Repository) *FuseFS {
	log.Printf("Repository created")
	return &FuseFS{repo: repository}
}

// Destroy unmounts the file system.
func (f *FuseFS) Destroy() {
	log.Printf("Unmounting file system...")
	err := f.repo.Close()
	if err != nil {
		log.Printf("Error closing repository: %v", err)
	}
}
