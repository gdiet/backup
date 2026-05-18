package cdc

import (
	"encoding/hex"
	"fmt"
	"hash"
)

type HashingChunker struct {
	chunker     Chunker
	hasher      hash.Hash
	chunkLength int
}

type LengthHash struct {
	Length int
	Hash   []byte
}

func (lh *LengthHash) String() string {
	return fmt.Sprintf("%d,%s", lh.Length, hex.EncodeToString(lh.Hash[:8]))
}

func NewHashingChunker(hasher hash.Hash, chunker Chunker) *HashingChunker {
	return &HashingChunker{
		chunker: chunker,
		hasher:  hasher,
	}
}

func (c *HashingChunker) Flush() []LengthHash {
	lengths := c.chunker.Flush()
	var result []LengthHash
	for _, length := range lengths {
		result = append(result, LengthHash{length, c.hasher.Sum(nil)})
		c.hasher.Reset()
		c.chunkLength = 0
	}
	return result
}

func (c *HashingChunker) Next(data []byte) []LengthHash {
	newChunkLength := c.chunkLength + len(data)
	var result []LengthHash
	for _, length := range c.chunker.Next(data) {
		// As documented in hash.Hash, Write never returns an error.
		_, _ = c.hasher.Write(data[:length-c.chunkLength])
		data = data[length-c.chunkLength:]
		result = append(result, LengthHash{length, c.hasher.Sum(nil)})
		c.hasher.Reset()
		c.chunkLength = 0
		newChunkLength -= length
	}
	_, _ = c.hasher.Write(data)
	c.chunkLength = newChunkLength
	return result
}
