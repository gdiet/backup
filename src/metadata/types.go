package metadata

import (
	"backup/src/metadata/internal"
)

type TreeEntry = internal.TreeEntry
type DirEntry = internal.DirEntry
type FileEntry = internal.FileEntry

var (
	ErrNotFound = internal.ErrNotFound
	ErrNotDir   = internal.ErrNotDir
)
