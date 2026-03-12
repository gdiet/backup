package meta

import "github.com/gdiet/backup/internal/fserr"

// FIXME do we need this at all?
// TreeEntry represents a directory or file entry in the tree
type TreeEntry struct {
	id   []byte
	name string
}

type TreeEntryIface interface {
	ID() []byte
	Name() string
}

// DirEntry represents a directory entry in the tree
type DirEntry struct {
	TreeEntry
}

func (d *DirEntry) ID() []byte   { return d.id }
func (d *DirEntry) Name() string { return d.name }

// ToBytes returns the byte array representation of the entry metadata, i.e., everything except the ID
func (d *DirEntry) ToBytes() []byte {
	// 1 byte type + name
	buf := make([]byte, 1+len(d.name))
	// buf[0] is 0, dirEntry type = 0
	copy(buf[1:], d.name)
	return buf
}

// FileEntry represents a file entry in the tree
type FileEntry struct {
	TreeEntry
	time int64    // UnixMilli
	dref [40]byte // len|hash of dataEntry
}

func (f *FileEntry) ID() []byte   { return f.id }
func (f *FileEntry) Name() string { return f.name }

// ToBytes returns the byte array representation of the entry metadata, i.e., everything except the ID
func (f *FileEntry) ToBytes() []byte {
	// 1 byte type + 8 bytes time + 40 bytes data reference + name
	buf := make([]byte, 49+len(f.name))
	buf[0] = 1 // fileEntry type = 1
	i64w(buf[1:], f.time)
	copy(buf[9:49], f.dref[:])
	copy(buf[49:], f.name)
	return buf
}

// dref copies a byte slice to a [40]byte array for the data reference
func dref(src []byte) [40]byte {
	var dref [40]byte
	copy(dref[:], src)
	return dref
}

// FIXME rename to treeEntryFrom
// treeEntryFromBytes parses a tree entry from bytes
func treeEntryFromBytes(id, data []byte) (TreeEntryIface, error) {
	if len(data) < 2 {
		return nil, fserr.IO()
	}
	// Determine entry type by first byte
	switch data[0] {
	case 0:
		// dirEntry: 1 byte type + name
		return &DirEntry{TreeEntry{id, string(data[1:])}}, nil
	case 1:
		// fileEntry: 1 byte type + 8 bytes time + 40 bytes data reference + name
		if len(data) < 50 {
			return nil, fserr.IO()
		}
		return &FileEntry{TreeEntry{id, string(data[49:])}, b64i(data[1:9]), dref(data[9:49])}, nil
	}
	return nil, fserr.IO()
}
