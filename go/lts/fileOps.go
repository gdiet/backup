package lts

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

// Fmap todo todo
type Fmap map[string]chan *os.File

// Fman todo todo
type Fman chan Fmap

// Read todo todo
func Read(fman Fman, basePath, relativePath string, offsetInFile, bytesToRead uint64, result chan []byte) {
	read(fman, <-fman, basePath, relativePath, offsetInFile, bytesToRead, result)
}

func read(fman Fman, fmap Fmap, basePath, relativePath string, offsetInFile, bytesToRead uint64, result chan []byte) {
	if fchan, ok := fmap[relativePath]; ok {
		fman <- fmap // pushing back means we have to call Read if needed, not read
		if file := <-fchan; file != nil {
			if _, err := file.Seek(int64(offsetInFile), io.SeekStart); err != nil {
				result <- nil
				return
			}
			buffer := make([]byte, bytesToRead)
			if _, err := io.ReadFull(file, buffer); err != nil {
				log.Println(err)
			}
			result <- buffer
			return
		}
		Read(fman, basePath, relativePath, offsetInFile, bytesToRead, result) // just try again
		return
	}
	// here: no entry in map for basepath
	if len(fmap) >= maxOpenFiles {
		if closeOne(&fmap) {
			read(fman, fmap, basePath, relativePath, offsetInFile, bytesToRead, result)
			return
		}
		// here: could not close an entry in a non-blocking way
		for path, fileChannel := range fmap {
			(<-fileChannel).Close() // yes, blocks - but makes sure that maxOpenFiles is not exceeded
			fileChannel <- nil
			delete(fmap, path)
			read(fman, fmap, basePath, relativePath, offsetInFile, bytesToRead, result)
			return
		}
	}
	path := filepath.Join(basePath, relativePath)
	file, err := os.Open(path) // or os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		result <- nil
	}
	fileChannel := make(chan *os.File, 1)
	fileChannel <- file
	fmap[relativePath] = fileChannel
	read(fman, fmap, basePath, relativePath, offsetInFile, bytesToRead, result)
	return
}

// closeOne closes one available file, removing it from the map
func closeOne(fmap *Fmap) bool {
	for relativePath, fileChannel := range *fmap {
		select {
		case file := <-fileChannel:
			delete(*fmap, relativePath)
			if file != nil {
				file.Close()
			}
			return true
		default:
		}
	}
	return false
}
