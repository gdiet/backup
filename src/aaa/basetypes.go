package aaa

type bytes []byte

// dataArea is a located contiguous area of bytes, e.g. in a file.
type dataArea struct {
	position int
	data     bytes
}

func (area *dataArea) end() int {
	return area.position + len(area.data)
}

// dataAreas is a collection of data areas.
type dataAreas []dataArea
