package aaa

type bytes []byte

// dataArea is a located contiguous area of bytes, e.g. in a file.
type dataArea struct {
	position int
	data     bytes
}

func (area *dataArea) copy() dataArea {
	return dataArea{position: area.position, data: append(bytes{}, area.data...)}
}

func (area *dataArea) len() int {
	return len(area.data)
}

func (area *dataArea) end() int {
	return area.position + len(area.data)
}

// dataAreas is a collection of data areas.
type dataAreas []dataArea
