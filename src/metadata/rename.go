package metadata

import (
	"backup/src/metadata/internal"
	"backup/src/util"
	"errors"

	"go.etcd.io/bbolt"
)

// TODO aliases for internal.Bucket and internal.WrapBucket?

// Rename renames a file or directory, moving it to a new location if required.
// If oldPath is a directory and newPath is an empty directory, newPath is replaced.
// If oldPath is a file and newPath is an existing file, newPath is replaced.
// If oldPath and newPath exist and are the same, no operation is performed (success).
// Returns ErrNotFound if the source path or a parent of the destination path does not exist.
// Returns ErrNotEmpty if trying to rename a directory to an existing non-empty directory.
// Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
// Returns ErrIsDir if trying to rename a file to a directory.
// Returns ErrInvalid if trying to rename a directory to a subdirectory of itself.
// Returns ErrIsRoot if trying to rename the root directory itself.
func (r *Repository) Rename(oldPath []string, newPath []string) error {
	// handle root directory rename
	if len(oldPath) == 0 {
		if len(newPath) == 0 {
			return nil // If oldPath and newPath exist and are the same, no operation is performed (success).
		}
		return ErrIsRoot // Returns ErrIsRoot if trying to rename the root directory itself.
	}

	return r.db.Update(func(tx *bbolt.Tx) error {
		tree := internal.WrapBucket(tx.Bucket(r.treeKey))
		children := internal.WrapBucket(tx.Bucket(r.childrenKey))

		// Lookup source: parent and entry
		oldParentID, _, err := internal.Lookup(tree, children, oldPath[:len(oldPath)-1])
		if err != nil {
			return err // Returns ErrNotFound if the source path or a parent of the destination path does not exist.
		}
		oldEntryID, oldEntry, err := internal.GetChild(tree, children, oldParentID, oldPath[len(oldPath)-1])
		if err != nil {
			return err // Returns ErrNotFound if the source path or a parent of the destination path does not exist.
		}

		// Check for no-op rename (source = target) for existing paths
		if Equals(oldPath, newPath) {
			return nil // If oldPath and newPath exist and are the same, no operation is performed (success).
		}

		// Lookup destination: parent and, if any, entry
		newParentID, newParent, err := internal.Lookup(tree, children, newPath[:len(newPath)-1])
		if err != nil {
			return err // Returns ErrNotFound if the source path or a parent of the destination path does not exist.
		}
		// Ensure the new parent is a directory
		if _, isDir := newParent.(*DirEntry); !isDir {
			return ErrNotDir // Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		}

		// Detect loop renames (directory to its own subdirectory)
		if len(oldPath) < len(newPath) && Equals(oldPath, newPath[:len(oldPath)]) {
			return ErrInvalid // Returns ErrInvalid if trying to rename a directory to a subdirectory of itself.
		}

		// Lookup destination entry, if any
		newEntryID, newEntry, getNewEntryErr := internal.GetChild(tree, children, newParentID, newPath[len(newPath)-1])

		switch oldEntry.(type) {
		case *FileEntry:
			// Renaming a file
			// Returns ErrIsDir if trying to rename a file to a directory.
			return errors.New("not implemented: renaming files") // TODO implement

		case *DirEntry:
			// Renaming a directory
			return renameDirectory(tree, children, oldParentID, oldEntryID, oldEntry, newParentID, newEntryID, newEntry, getNewEntryErr)

		default:
			util.AssertionFailedf("unexpected source entry type %T in Rename", oldEntry)
			return errors.New("invalid entry type") // Should not happen
		}
	})
}

// renameDirectory handles renaming of directories, including moving to a new parent.
// Returns ErrNotEmpty if trying to rename a directory to an existing non-empty directory.
// Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
func renameDirectory(
	tree internal.Bucket, children internal.Bucket,
	oldParentID, oldEntryID []byte, oldEntry TreeEntry,
	newParentID, newEntryID []byte, newEntry TreeEntry, getNewEntryError error) error {

	if getNewEntryError == nil {
		// Destination exists
		switch newEntry.(type) {
		case *FileEntry:
			return internal.ErrNotDir // Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		case *DirEntry:
			// Remove destination entry unless it's not empty
			err := internal.Rmdir(tree, children, newParentID, newEntryID)
			if err != nil {
				return err // Returns ErrNotEmpty if trying to rename a directory to an existing non-empty directory.
			}
		default:
			util.AssertionFailedf("unexpected destination entry type %T in Rename", newEntry)
			return errors.New("invalid entry type") // Should not happen
		}
	}

	// Move entry to new location
	// FIXME implement
	// Rename to the new name
	// FIXME implement
	return nil
}

// // Check for invalid rename (directory to its own subdirectory)
// isSubdir, err := internal.IsSubdirectory(tree, children, oldEntryID, newParentID)
// if err != nil {
// 	return err
// }
// if isSubdir {
// 	return ErrInvalid // Returns ErrInvalid if trying to rename a directory to a subdirectory of itself.
// }

// if _, isFile := oldEntry.(*FileEntry); isFile {
// }

// if dirEntry, isDir := oldEntry.(*DirEntry); isDir {
// 	if getNewEntryErr == nil {
// 		// Destination exists
// 		switch newEntry.(type) {
// 		case *internal.FileEntry:
// 			return ErrNotDir // Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
// 		case *internal.DirEntry:
// 			// Renaming a directory
// 			// Ensure destination directory is empty
// 			isEmpty, err := internal.IsDirectoryEmpty(tree, children, newEntryID)
// 			if err != nil {
// 				return err
// 			}
// 			if !isEmpty {
// 				return ErrNotEmpty // Returns ErrNotEmpty if trying to rename a directory to an existing non-empty directory.
// 			}
// 		default:
// 			util.AssertionFailedf("unexpected destination entry type %T in Rename", newEntry)
// 			return errors.New("invalid entry type") // Should not happen
// 		}
// 	}
// }
// panic("not fully implemented: renaming directories")

// // Returns ErrNotEmpty if trying to rename a directory to an existing non-empty directory.
// // Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
// // Returns ErrInvalid if trying to rename a directory to a subdirectory of itself.

// // // Check for invalid rename (directory to its own subdirectory)
// // if dirEntry, isDir := oldEntry.(*internal.DirEntry); isDir {
// // 	isSubdir, err := internal.IsSubdirectory(tree, children, dirEntry.IDBytes(), newParentID)
// // 	if err != nil {
// // 		return err
// // 	}
// // 	if isSubdir {
// // 		return ErrInvalid
// // 	}
// // }

// // // Check if destination entry exists
// // newEntryID, newEntry, err := internal.LookupChild(tree, children, newParentID, newPath[len(newPath)-1])
// // if err != nil && err != ErrNotFound {
// // 	return err
// // }

// // if newEntry != nil {
// // 	// Destination exists
// // 	switch oldEntry.(type) {
// // 	case *internal.DirEntry:
// // 		// Renaming a directory
// // 		if _, isDir := newEntry.(*internal.DirEntry); !isDir {
// // 			return ErrNotDir // Can't rename directory to file
// // 		}
// // 		// Ensure destination directory is empty
// // 		isEmpty, err := internal.IsDirectoryEmpty(tree, children, newEntryID)
// // 		if err != nil {
// // 			return err
// // 		}
// // 		if !isEmpty {
// // 			return ErrNotEmpty
// // 		}
// // 	case *internal.FileEntry:
// // 		// Renaming a file
// // 		if _, isDir := newEntry.(*internal.DirEntry); isDir {
// // 			return ErrIsDir // Can't rename file to directory
// // 		}
// // 	}
// // 	// Remove existing destination entry
// // 	err = internal.RemoveChild(tree, children, newParentID, newPath[len(newPath)-1])
// // 	if err != nil {
// // 		return err
// // 	}
// // }

// // // Add entry to new location
// // err = internal.AddChild(tree, children, newParentID, newPath[len(newPath)-1], oldEntryID)
// // if err != nil {
// // 	return err
// // }

// // // Remove entry from old location
// // err = internal.RemoveChild(tree, children, oldParentID, oldPath[len(oldPath)-1])
// // return err
