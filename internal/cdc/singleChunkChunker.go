package cdc

type singleChunkChunker struct {
	chunkLength int
}

var _ Chunker = (*singleChunkChunker)(nil)

func NewSingleChunkChunker() Chunker {
	return &singleChunkChunker{}
}

func (s *singleChunkChunker) Next(data []byte) []int {
	s.chunkLength += len(data)
	return nil
}

func (s *singleChunkChunker) Flush() []int {
	defer func() { s.chunkLength = 0 }()
	return []int{s.chunkLength}
}
