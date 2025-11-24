package meta

import (
	"backup/src/meta/internal"
)

type TreeEntry = internal.TreeEntry
type DirEntry = internal.DirEntry
type FileEntry = internal.FileEntry

var (
	ErrExists   = internal.ErrExists
	ErrInvalid  = internal.ErrInvalid
	ErrIsRoot   = internal.ErrIsRoot
	ErrNotDir   = internal.ErrNotDir
	ErrNotEmpty = internal.ErrNotEmpty
	ErrNotFound = internal.ErrNotFound
)
