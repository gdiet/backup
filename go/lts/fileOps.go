package lts

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

// Read todo todo
func Read(basePath string, position, size uint64) ([]byte, error) {
	relativePath, offsetInFile, bytesToRead := PathOffsetSize(position, size)
	path := filepath.Join(basePath, relativePath)
	file, err := os.Open(path) // os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	buffer := make([]byte, bytesToRead)
	if _, err = file.Seek(int64(offsetInFile), io.SeekStart); err != nil {
		return nil, err
	}
	if _, err = io.ReadFull(file, buffer); err != nil {
		log.Println(err)
	}
	return buffer, err
}
