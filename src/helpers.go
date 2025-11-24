package main

import (
	"io"

	"github.com/zeebo/blake3"
)

func hash(data []byte) [32]byte {
	return blake3.Sum256(data)
}

func hashAll(data *io.Reader) ([]byte, error) {
	h := blake3.New() // 32 bytes output is default
	_, err := io.Copy(h, *data)
	return h.Sum(nil), err
}
