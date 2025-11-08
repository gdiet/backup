package metadata

import (
	"encoding/binary"
	"errors"
)

// area represents a located contiguous range of bytes.
type area struct {
	off int64
	len int64
}

type dirEntry struct {
	name string // at least 1 byte
}

func (d *dirEntry) toBytes() []byte {
	// 1 byte type + name
	buf := make([]byte, 1+len(d.name))
	// buf[0] is 0, dirEntry type = 0
	copy(buf[1:], []byte(d.name))
	return buf
}

type fileEntry struct {
	time int64    // UnixMilli
	dref [40]byte // len|hash of dataEntry
	name string   // at least 1 byte
}

func (f *fileEntry) toBytes() []byte {
	// 1 byte type + 8 bytes time + 40 bytes data reference + name
	buf := make([]byte, 49+len(f.name))
	buf[0] = 1 // fileEntry type = 1
	binary.LittleEndian.PutUint64(buf[1:], uint64(f.time))
	copy(buf[9:], f.dref[:])
	copy(buf[49:], []byte(f.name))
	return buf
}

func treeEntryFromBytes(data []byte) (interface{}, error) {
	if len(data) < 2 {
		return nil, errors.New("treeEntry too short")
	}
	// Determine entry type by first byte
	switch data[0] {
	case 0:
		// dirEntry: 1 byte type + name
		return dirEntry{name: string(data[1:])}, nil
	case 1:
		// fileEntry: 1 byte type + 8 bytes time + 40 bytes data reference + name
		if len(data) < 50 {
			return nil, errors.New("fileEntry too short")
		}
		dref := [40]byte{}
		copy(dref[:], data[9:49])
		f := fileEntry{
			time: int64(binary.LittleEndian.Uint64(data[1:])),
			dref: dref,
			name: string(data[17:]),
		}
		return f, nil
	default:
		return nil, errors.New("invalid treeEntry type")
	}
}

type dataEntry struct {
	refs  uint64 // Reference count
	areas []area // Storage areas
}

func (d *dataEntry) toBytes() []byte {
	// 8 bytes refs + 16 bytes per area
	buf := make([]byte, 8+16*len(d.areas))
	binary.LittleEndian.PutUint64(buf, d.refs)
	pos := 8
	for _, area := range d.areas {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(area.off))
		binary.LittleEndian.PutUint64(buf[pos+8:], uint64(area.len))
		pos += 16
	}
	return buf
}
