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
	d.refs = binary.LittleEndian.Uint64(data[:8])
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
