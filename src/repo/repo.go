package repo

import (
	"backup/src/fserr"
	"backup/src/meta"
)

type Repository struct {
	metadata *meta.Metadata
}

// NewRepository creates a new Repository instance wrapping the given metadata.
func NewRepository(repository string) (*Repository, error) {
	metadata, err := meta.NewMetadata(repository)
	if err != nil {
		return nil, err
	}
	return &Repository{metadata: metadata}, nil
}

func (r *Repository) Close() error {
	return r.metadata.Close()
}

func (r *Repository) Mkfile(path []string) (uint64, error) {
	return 0, fserr.IO
	// return r.metadata.Mkfile(path)
}

func (r *Repository) Lookup(path []string) (id uint64, entry meta.TreeEntry, err error) {
	if len(path) == 0 {
		return 0, meta.NewDirEntry(""), nil
	}
	return r.metadata.Lookup(path)
}

func (r *Repository) Mkdir(path []string) (uint64, error) {
	return r.metadata.Mkdir(path)
}

func (r *Repository) Rmdir(path []string) error {
	return r.metadata.Rmdir(path)
}

func (r *Repository) Readdir(path []string) (entries []meta.TreeEntry, err error) {
	return r.metadata.Readdir(path)
}

func (r *Repository) Rename(oldPath []string, newPath []string) error {
	return fserr.IO
	// return r.metadata.Rename(oldPath, newPath)
}
