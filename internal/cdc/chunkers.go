package cdc

import "errors"

type ChunkerFactory struct {
	DefaultChunker func() Chunker
	SingleChunker  func() Chunker
}

func NewChunkerFactory(defaultChunker string, targetSizeBits int) (ChunkerFactory, error) {
	config, err := NewConfig(targetSizeBits)
	if err != nil {
		return ChunkerFactory{}, err
	}
	var f func() Chunker
	switch defaultChunker {
	case "none":
		f = func() Chunker { return NewSingleChunk() }
	case "cdc":
		f = func() Chunker { return config.NewCDC() }
	case "jpeg+cdc":
		f = func() Chunker { return config.NewFileSpecificChunker() }
	default:
		return ChunkerFactory{}, errors.New("unknown default chunker: " + defaultChunker)
	}
	return ChunkerFactory{f, func() Chunker { return NewSingleChunk() }}, nil
}
