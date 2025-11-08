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

type treeEntry interface {
	getName() string
}

type dirEntry struct {
	name string // at least 1 byte
}

func (d dirEntry) getName() string {
	return d.name
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

func (f fileEntry) getName() string {
	return f.name
}

func (f *fileEntry) toBytes() []byte {
	// 1 byte type + 8 bytes time + 40 bytes data reference + name
	buf := make([]byte, 49+len(f.name))
	buf[0] = 1 // fileEntry type = 1
	binary.BigEndian.PutUint64(buf[1:], uint64(f.time))
	copy(buf[9:], f.dref[:])
	copy(buf[49:], []byte(f.name))
	return buf
}

func treeEntryFromBytes(data []byte) (treeEntry, error) {
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
			time: int64(binary.BigEndian.Uint64(data[1:])),
			dref: dref,
			name: string(data[49:]),
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
	// 8 bytes reference count + 16 bytes per area
	buf := make([]byte, 8+16*len(d.areas))
	binary.BigEndian.PutUint64(buf, d.refs)
	pos := 8
	for _, area := range d.areas {
		binary.BigEndian.PutUint64(buf[pos:], uint64(area.off))
		binary.BigEndian.PutUint64(buf[pos+8:], uint64(area.len))
		pos += 16
	}
	return buf
}

func dataEntryFromBytes(data []byte) (d dataEntry, err error) {
	// 8 bytes reference count + 16 bytes per area
	dataLen := len(data)
	if dataLen < 8 || dataLen%16 != 8 {
		return dataEntry{}, errors.New("dataEntry length invalid")
	}
	d.refs = binary.BigEndian.Uint64(data)
	d.areas = make([]area, (dataLen-8)/16)
	pos := 8
	for i := range d.areas {
		off := int64(binary.BigEndian.Uint64(data[pos : pos+8]))
		len := int64(binary.BigEndian.Uint64(data[pos+8 : pos+16]))
		d.areas[i] = area{off: off, len: len}
		pos += 16
	}
	return d, nil
}
