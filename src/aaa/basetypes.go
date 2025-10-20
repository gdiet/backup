package aaa

type baseFile interface {
	read(position int, data bytes) (err error)
	length() int
}

type area struct {
	off int // TODO or rename to position for consistency, and length?
	len int
}

func (area *area) end() int {
	return area.off + area.len
}

type areas []area

type bytes []byte

// copy creates a compact deep copy of the bytes slice.
func (data *bytes) copy() bytes {
	result := make(bytes, len(*data))
	copy(result, *data)
	return result
}

// dataArea is a located contiguous area of bytes, e.g. in a file.
type dataArea struct {
	position int // TODO rename to off for consistency?
	data     bytes
}

// copy creates a compact deep copy of the data area.
func (area *dataArea) copy() dataArea {
	return dataArea{position: area.position, data: area.data.copy()}
}

func (area *dataArea) len() int {
	return len(area.data)
}

func (area *dataArea) end() int {
	return area.position + len(area.data)
}

type dataAreas []dataArea
