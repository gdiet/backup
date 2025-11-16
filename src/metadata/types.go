package metadata

import (
	"backup/src/metadata/internal"
)

type TreeEntry = internal.TreeEntry
type DirEntry = internal.DirEntry
type FileEntry = internal.FileEntry

var (
	ErrExists   = internal.ErrExists
	ErrIsRoot   = internal.ErrIsRoot
	ErrNotDir   = internal.ErrNotDir
	ErrNotEmpty = internal.ErrNotEmpty
	ErrNotFound = internal.ErrNotFound
)
