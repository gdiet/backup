package meta

import (
	"backup/src/meta/internal"
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
func (r *Metadata) Rename(oldPath []string, newPath []string) error {
	// handle root directory rename
	if stopHere, err := checkForRootDirectoryRename(oldPath, newPath); stopHere {
		return err
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
		switch newParent.(type) {
		case *FileEntry:
			return ErrNotDir // Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		case *DirEntry:
			// continue
		default:
			util.AssertionFailedf("unexpected destination parent entry type %T in Rename", newParent)
			return ErrNotDir // Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		}

		if _, isDir := newParent.(*DirEntry); !isDir {
			return ErrNotDir // Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		}

		// Detect loop renames (directory to its own subdirectory)
		if len(oldPath) < len(newPath) && Equals(oldPath, newPath[:len(oldPath)]) {
			return ErrInvalid // Returns ErrInvalid if trying to rename a directory to a subdirectory of itself.
		}

		switch oldEntry.(type) {
		case *FileEntry:
			// Returns ErrIsDir if trying to rename a file to a directory.
			return errors.New("not implemented: renaming files") // TODO wait for implementation of files in the filesystem
		case *DirEntry:
			// continue
		default:
			util.AssertionFailedf("unexpected source entry type %T in Rename", oldEntry)
			return errors.New("invalid entry type") // Should not happen
		}

		return renameDirectory(tree, children, oldParentID, oldEntryID, oldEntry, newParentID, newPath[len(newPath)-1])
	})
}

// checkForRootDirectoryRename checks if the rename operation involves the root directory.
// If oldPath is the root directory, it returns ErrIsRoot unless newPath is also the root directory (no-op in that case).
func checkForRootDirectoryRename(oldPath []string, newPath []string) (bool, error) {
	if len(oldPath) == 0 {
		if len(newPath) == 0 {
			return true, nil // If oldPath and newPath exist and are the same, no operation is performed (success).
		}
		return true, ErrIsRoot // Returns ErrIsRoot if trying to rename the root directory itself.
	}
	return false, nil
}

// renameDirectory handles renaming of directories, including moving to a new parent.
// Returns ErrNotEmpty if trying to rename a directory to an existing non-empty directory.
// Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
func renameDirectory(
	tree internal.Bucket, children internal.Bucket,
	oldParentID, oldEntryID []byte, oldEntry TreeEntry,
	newParentID []byte, newEntryName string) error {

	// Lookup destination entry to replace, if any
	replaceEntryID, replaceEntry, getReplaceEntryError := internal.GetChild(tree, children, newParentID, newEntryName)

	if getReplaceEntryError == nil {
		// Destination exists
		switch replaceEntry.(type) {
		case *FileEntry:
			return internal.ErrNotDir // Returns ErrNotDir if a parent of the destination is not a directory or if trying to rename a directory to a file.
		case *DirEntry:
			// Remove destination entry unless it's not empty
			err := internal.Rmdir(tree, children, newParentID, replaceEntryID)
			if err != nil {
				return err // Returns ErrNotEmpty if trying to rename a directory to an existing non-empty directory.
			}
		default:
			util.AssertionFailedf("unexpected destination entry type %T in Rename", replaceEntry)
			return errors.New("invalid entry type") // Should not happen
		}
	}

	// Move entry to new location
	internal.RemoveChild(children, oldParentID, oldEntryID)
	internal.AddChild(children, newParentID, oldEntryID)

	// Rename to the new name if necessary
	if oldEntry.Name() != newEntryName {
		oldEntry.SetName(newEntryName)
		if err := tree.Put(oldEntryID, oldEntry.ToBytes()); err != nil {
			return err
		}
	}
	return nil
}
