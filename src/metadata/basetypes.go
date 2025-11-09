package metadata

import (
	"encoding/binary"
	"errors"
)

// area represents a located contiguous range of bytes.
// Other than in the system file API, uint64 is used here because it is easier to serialize.
type area struct {
	off uint64
	len uint64
}

type treeEntry interface {
	getName() string
}

type dirEntry struct {
	name string // at least 1 byte
}

func (d *dirEntry) getName() string {
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

func (f *fileEntry) getName() string {
	return f.name
}

func (f *fileEntry) toBytes() []byte {
	// 1 byte type + 8 bytes time + 40 bytes data reference + name
	buf := make([]byte, 49+len(f.name))
	buf[0] = 1 // fileEntry type = 1
	i64w(buf[1:], f.time)
	copy(buf[9:49], f.dref[:])
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
		return &dirEntry{name: string(data[1:])}, nil
	case 1:
		// fileEntry: 1 byte type + 8 bytes time + 40 bytes data reference + name
		if len(data) < 50 {
			return nil, errors.New("fileEntry too short")
		}
		dref := [40]byte{}
		copy(dref[:], data[9:49])
		f := &fileEntry{
			time: b64i(data[1:9]),
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
	u64w(buf, d.refs)
	pos := 8
	for _, area := range d.areas {
		u64w(buf[pos:], area.off)
		u64w(buf[pos+8:], area.len)
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
	d.refs = b64u(data)
	d.areas = make([]area, (dataLen-8)/16)
	pos := 8
	for i := range d.areas {
		off := b64u(data[pos : pos+8])
		len := b64u(data[pos+8 : pos+16])
		d.areas[i] = area{off: off, len: len}
		pos += 16
	}
	return d, nil
}

// u64b converts an uint64 to a byte slice key for bbolt.
func u64b(u uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, u)
	return key
}

// u64w writes an uint64 to an existing byte slice.
func u64w(buf []byte, u uint64) {
	binary.BigEndian.PutUint64(buf, u)
}

// i64w writes an int64 to an existing byte slice.
func i64w(buf []byte, i int64) {
	binary.BigEndian.PutUint64(buf, uint64(i))
}

// b64u converts a byte slice to an uint64.
// This function is added for readability and symmetry to u64b.
func b64u(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// b64i converts a byte slice to an int64.
// This function is added for readability and symmetry to i64w.
func b64i(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b))
}
