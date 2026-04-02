package cdc

type singleChunk struct {
	chunkLength int
}

var _ Chunker = (*singleChunk)(nil)

func NewSingleChunk() Chunker {
	return &singleChunk{}
}

func (s *singleChunk) Next(data []byte) []int {
	s.chunkLength += len(data)
	return nil
}

func (s *singleChunk) Flush() []int {
	defer func() { s.chunkLength = 0 }()
	return []int{s.chunkLength}
}
