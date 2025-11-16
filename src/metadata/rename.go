package metadata

// // Rename renames a directory or file, moving it between directories if required.
// // Returns ENOENT if id does not exist.
// // Returns ENOENT if parent of newpath does not exist.
// // Returns ENOTDIR if parent of newpath is a file.
// // Returns EISDIR if id is a file and newpath is a dir.
// // Returns ENOTEMPTY if newpath is a nonempty directory.
// /*
//    If newpath already exists, it will be atomically replaced, so that
//    there is no point at which another process attempting to access
//    newpath will find it missing.  However, there will probably be a
//    window in which both oldpath and newpath refer to the file being
//    renamed.

//    If oldpath and newpath are existing hard links referring to the
//    same file, then rename() does nothing, and returns a success
//    status.

//    If newpath exists but the operation fails for some reason,
//    rename() guarantees to leave an instance of newpath in place.

//    oldpath can specify a directory.  In this case, newpath must
//    either not exist, or it must specify an empty directory.

//    If oldpath refers to a symbolic link, the link is renamed; if
//    newpath refers to a symbolic link, the link will be overwritten.
// */
// func (r *Repository) Rename(id uint64, newPath []string) error {
// 	panic("not implemented")
// }
