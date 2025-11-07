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
	time   int64 // UnixMilli
	dataID int64
	name   string // at least 1 byte
}

func (f *fileEntry) toBytes() []byte {
	// 1 byte type + 8 bytes time + 8 bytes dataID + name
	buf := make([]byte, 1+16+len(f.name)) // 1 byte type + 8 time + 8 dataID + name
	buf[0] = 1                            // fileEntry type = 1
	binary.LittleEndian.PutUint64(buf[1:], uint64(f.time))
	binary.LittleEndian.PutUint64(buf[9:], uint64(f.dataID))
	copy(buf[17:], []byte(f.name))
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
		// fileEntry: 1 byte type + 8 bytes time + 8 bytes dataID + name
		if len(data) < 18 {
			return nil, errors.New("fileEntry too short")
		}
		f := fileEntry{
			time:   int64(binary.LittleEndian.Uint64(data[1:])),
			dataID: int64(binary.LittleEndian.Uint64(data[9:])),
			name:   string(data[17:]),
		}
		return f, nil
	default:
		return nil, errors.New("invalid treeEntry type")
	}
}

type dataEntry struct {
	refs  uint64   // Reference count
	hash  [32]byte // Blake3 256-bit hash of the data
	areas []area   // Storage areas
	// possibly add sync.Once or *int64 for cached length
}

func dataEntryFromBytes(data []byte) (d dataEntry, err error) {
	// 8 bytes refs + 32 bytes hash + 16 bytes per area
	dataLen := len(data)
	if dataLen < 40 || dataLen%16 != 8 {
		return dataEntry{}, errors.New("dataEntry length invalid")
	}
	d.refs = binary.LittleEndian.Uint64(data)
	copy(d.hash[:], data[8:40])
	d.areas = make([]area, (dataLen-40)/16)
	pos := 40
	for i := range d.areas {
		off := int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		len := int64(binary.LittleEndian.Uint64(data[pos+8 : pos+16]))
		d.areas[i] = area{off: off, len: len}
		pos += 16
	}
	return d, nil
}

func (d *dataEntry) len() (total int64) {
	for _, area := range d.areas {
		total += area.len
	}
	return total
}

func (d *dataEntry) toBytes() []byte {
	// 8 bytes refs + 32 bytes hash + 16 bytes per area
	buf := make([]byte, 40+16*len(d.areas))
	binary.LittleEndian.PutUint64(buf, d.refs)
	copy(buf[8:40], d.hash[:])
	pos := 40
	for _, area := range d.areas {
		binary.LittleEndian.PutUint64(buf[pos:], uint64(area.off))
		binary.LittleEndian.PutUint64(buf[pos+8:], uint64(area.len))
		pos += 16
	}
	return buf
}
