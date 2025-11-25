package fuse

import (
	"backup/src/repo"
	"log"

	"github.com/winfsp/cgofuse/fuse"
)

type FS struct {
	fuse.FileSystemBase
	repo *repo.Repository
}

func NewFS(repository *repo.Repository) *FS {
	log.Printf("Repository created")
	return &FS{repo: repository}
}

// Destroy unmounts the file system, releasing any held resources.
func (f *FS) Destroy() {
	log.Printf("Unmounting file system...")
	err := f.repo.Close()
	if err != nil {
		log.Printf("Error closing repository: %v", err)
	}
}
