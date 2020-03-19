package lts

import (
	"io"
	"log"
	"os"
	"path/filepath"
)

// Fman todo todo
type Fman chan map[string]chan *os.File

// Read todo todo
func Read(manChannel Fman, basePath string, position, size uint64) ([]byte, error) {
	man := <-manChannel
	var err error
	var file *os.File
	relativePath, offsetInFile, bytesToRead := PathOffsetSize(position, size)
	fileChannel, ok := man[relativePath]
	if ok {
		manChannel <- man
		file = <-fileChannel
		if file == nil {
			fileChannel <- nil
			return Read(manChannel, basePath, position, size)
		}
	} else {
		if len(man) > 4 {
			if !closeOne(&man) {
				for _, fileChannel = range man {
					manChannel <- man
					file := <-fileChannel
					fileChannel <- nil
					if file != nil {
						file.Close()
					}
					return Read(manChannel, basePath, position, size)
				}
			}
			manChannel <- man
			return Read(manChannel, basePath, position, size)
		}
		path := filepath.Join(basePath, relativePath)
		// os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0777)
		if file, err = os.Open(path); err != nil {
			return nil, err
		}
		fileChannel := make(chan *os.File)
		man[relativePath] = fileChannel
		manChannel <- man
	}
	buffer := make([]byte, bytesToRead)
	if _, err = file.Seek(int64(offsetInFile), io.SeekStart); err != nil {
		fileChannel <- file
		return nil, err
	}
	if _, err = io.ReadFull(file, buffer); err != nil {
		log.Println(err)
	}
	fileChannel <- file
	return buffer, err
}

// closeOne closes one available file, removing it from the map
func closeOne(man *map[string]chan *os.File) bool {
	for relativePath, fileChannel := range *man {
		select {
		case file := <-fileChannel:
			delete(*man, relativePath)
			if file != nil {
				file.Close()
			}
			return true
		default:
		}
	}
	return false
}
