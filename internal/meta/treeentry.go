package meta

import "github.com/gdiet/backup/internal/fserr"

// TreeEntry represents a directory or file entry in the tree
type TreeEntry interface {
	ID() []byte
	TreeProp
}

// TreeProp represents properties of directory and file entries
type TreeProp interface {
	Name() string
	ToBytes() []byte
}

// DirProp represents properties of a directory entry
type DirProp struct {
	name string // root name is empty, otherwise at least 1 character
}

// DirEntry implements DirProp and TreeEntry
type DirEntry struct {
	DirProp
	id []byte
}

func (d *DirEntry) ID() []byte {
	return d.id
}

func (d *DirProp) Name() string {
	return d.name
}

func (d *DirProp) ToBytes() []byte {
	// 1 byte type + name
	buf := make([]byte, 1+len(d.name))
	// buf[0] is 0, dirEntry type = 0
	copy(buf[1:], d.name)
	return buf
}

// newDirProp creates a new directory entry
func newDirProp(name string) *DirProp {
	return &DirProp{name: name}
}

// FileProp represents properties of a file entry
type FileProp struct {
	time int64    // UnixMilli
	dref [40]byte // len|hash of dataEntry
	name string   // at least 1 character
}

func (f *FileProp) Name() string {
	return f.name
}

func (f *FileProp) Time() int64 {
	return f.time
}

func (f *FileProp) Size() int64 {
	return b64i(f.dref[:8])
}

// newFileProp creates a new file entry
func newFileProp(name string, time int64, dref [40]byte) *FileProp {
	return &FileProp{
		name: name,
		time: time,
		dref: dref,
	}
}

func (f *FileProp) ToBytes() []byte {
	// 1 byte type + 8 bytes time + 40 bytes data reference + name
	buf := make([]byte, 49+len(f.name))
	buf[0] = 1 // fileEntry type = 1
	i64w(buf[1:], f.time)
	copy(buf[9:49], f.dref[:])
	copy(buf[49:], f.name)
	return buf
}

// treeEntryFromBytes parses a tree entry from bytes
func treeEntryFromBytes(data []byte) (TreeProp, error) {
	if len(data) < 2 {
		return nil, fserr.IO()
	}
	// Determine entry type by first byte
	switch data[0] {
	case 0:
		// dirEntry: 1 byte type + name
		return newDirProp(string(data[1:])), nil
	case 1:
		// fileEntry: 1 byte type + 8 bytes time + 40 bytes data reference + name
		if len(data) < 50 {
			return nil, fserr.IO()
		}
		dref := [40]byte{}
		copy(dref[:], data[9:49])
		return newFileProp(string(data[49:]), b64i(data[1:9]), dref), nil
	default:
		return nil, fserr.IO()
	}
}
