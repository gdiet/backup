package repo

import "backup/src/meta"

type Repository struct {
	metadata *meta.Metadata
}

// NewRepository creates a new Repository instance wrapping the given metadata.
func NewRepository(metadata *meta.Metadata) *Repository {
	return &Repository{metadata: metadata}
}

// Close closes the repository and underlying metadata store.
func (r *Repository) Close() error {
	return r.metadata.Close()
}

// Mkfile creates a new file at the given path.
func (r *Repository) Mkfile(path []string) (uint64, error) {
	return r.metadata.Mkfile(path)
}

// Lookup looks up a path and returns the ID and tree entry.
func (r *Repository) Lookup(path []string) (id uint64, entry meta.TreeEntry, err error) {
	return r.metadata.Lookup(path)
}

// Mkdir creates a new directory at the given path.
func (r *Repository) Mkdir(path []string) (uint64, error) {
	return r.metadata.Mkdir(path)
}

// Rmdir removes a directory at the given path.
func (r *Repository) Rmdir(path []string) error {
	return r.metadata.Rmdir(path)
}

// Readdir reads the contents of a directory at the given path.
func (r *Repository) Readdir(path []string) (entries []meta.TreeEntry, err error) {
	return r.metadata.Readdir(path)
}

// Rename renames/moves a file or directory from oldPath to newPath.
func (r *Repository) Rename(oldPath []string, newPath []string) error {
	return r.metadata.Rename(oldPath, newPath)
}
