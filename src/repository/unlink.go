package repository

// // Unlink deletes a file or directory from the filesystem.
// // Returns os.ErrNotExist if the entry does not exist.
// // Returns syscall.ENOTEMPTY if trying to delete a non-empty directory.
// func (r *Repository) Unlink(id uint64) error {
// 	err := r.db.Update(func(tx *bbolt.Tx) error {
// 		tree := tx.Bucket(treeKey)
// 		children := tx.Bucket(childrenKey)
// 		data := tx.Bucket(dataKey)
// 		freeAreas := tx.Bucket(freeAreasKey)
// 		return internal.Unlink(tree, children, data, freeAreas, internal.U64b(id))
// 	})
// 	return err
// }
