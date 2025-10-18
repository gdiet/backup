package aaa

type bytes []byte

func (data *bytes) copy() bytes {
	return append(bytes{}, (*data)...)
}

// dataArea is a located contiguous area of bytes, e.g. in a file.
type dataArea struct {
	position int
	data     bytes
}

func (area *dataArea) copy() dataArea {
	return dataArea{position: area.position, data: area.data.copy()}
}

func (area *dataArea) len() int {
	return len(area.data)
}

func (area *dataArea) end() int {
	return area.position + len(area.data)
}

// dataAreas is a collection of data areas.
type dataAreas []dataArea
