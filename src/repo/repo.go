package repo

import "backup/src/meta"

type Repository struct {
	metadata *meta.Metadata
}

// NewRepository creates a new Repository instance wrapping the given metadata.
func NewRepository(metadata *meta.Metadata) *Repository {
	return &Repository{metadata: metadata}
}

func (r *Repository) Close() error {
	return EIO
	// return r.metadata.Close()
}

func (r *Repository) Mkfile(path []string) (uint64, error) {
	return 0, EIO
	// return r.metadata.Mkfile(path)
}

func (r *Repository) Lookup(path []string) (id uint64, entry meta.TreeEntry, err error) {
	return 0, nil, EIO
	// return r.metadata.Lookup(path)
}

func (r *Repository) Mkdir(path []string) (uint64, error) {
	return 0, EIO
	// return r.metadata.Mkdir(path)
}

func (r *Repository) Rmdir(path []string) error {
	return EIO
	// return r.metadata.Rmdir(path)
}

func (r *Repository) Readdir(path []string) (entries []meta.TreeEntry, err error) {
	return nil, EIO
	// return r.metadata.Readdir(path)
}

func (r *Repository) Rename(oldPath []string, newPath []string) error {
	return EIO
	// return r.metadata.Rename(oldPath, newPath)
}
