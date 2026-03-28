package cdc

import (
	"lukechampine.com/blake3"
)

type HashingChunker struct {
	chunker     Chunker
	hasher      *blake3.Hasher
	chunkLength int
}

type LengthHash struct {
	Length int
	Hash   []byte
}

func NewHashingChunker(chunker Chunker) *HashingChunker {
	return &HashingChunker{
		chunker: chunker,
		hasher:  blake3.New(20, nil),
	}
}

func (c *HashingChunker) Flush() []LengthHash {
	lengths := c.chunker.Flush()
	var result []LengthHash
	for _, length := range lengths {
		hash := c.hasher.Sum(nil)
		result = append(result, LengthHash{length, hash})
		c.hasher.Reset()
		c.chunkLength = 0
	}
	return result
}

func (c *HashingChunker) Next(data []byte) []LengthHash {
	newChunkLength := c.chunkLength + len(data)
	var result []LengthHash
	for _, length := range c.chunker.Next(data) {
		_, _ = c.hasher.Write(data[:length-c.chunkLength])
		data = data[length-c.chunkLength:]
		hash := c.hasher.Sum(nil)
		c.hasher.Reset()
		c.chunkLength = 0
		result = append(result, LengthHash{length, hash})
		newChunkLength -= length
	}
	_, _ = c.hasher.Write(data)
	c.chunkLength = newChunkLength
	return result
}
