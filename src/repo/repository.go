package repo

import (
	"backup/src/data"
	"backup/src/meta"
)

type Repository struct {
	metadata *meta.Metadata
	handles  *data.Handles
}

// NewRepository creates a new Repository instance wrapping the given metadata.
func NewRepository(metadata *meta.Metadata) *Repository {
	// TODO initialize handles
	return &Repository{metadata: metadata}
}

func (r *Repository) Close() error {
	// TODO flush to the store and close handles
	return r.metadata.Close()
}

func (r *Repository) Create(path []string) (uint64, error) {
	id, err := r.metadata.Mkfile(path)
	if err != nil {
		return 0, err
	}
	_, err = r.handles.Create(id)
	return id, err
}

func (r *Repository) Open(id uint64) (data.Handle, error) {
	return r.handles.Get(id)
}

func (r *Repository) Lookup(path []string) (id uint64, entry meta.TreeEntry, err error) {
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
	return r.metadata.Rename(oldPath, newPath)
}
