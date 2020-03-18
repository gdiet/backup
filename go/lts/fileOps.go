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

// Read2 todo todo
func Read2(files map[string]*os.File, basePath string, position, size uint64) ([]byte, error) {
	var err error
	relativePath, offsetInFile, bytesToRead := PathOffsetSize(position, size)
	file := files[relativePath]
	if file == nil {
		path := filepath.Join(basePath, relativePath)
		// os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0777)
		if file, err = os.Open(path); err != nil {
			return nil, err
		}
		files[relativePath] = file
	}
	buffer := make([]byte, bytesToRead)
	if _, err = file.Seek(int64(offsetInFile), io.SeekStart); err != nil {
		return nil, err
	}
	if _, err = io.ReadFull(file, buffer); err != nil {
		log.Println(err)
	}
	return buffer, err
}

// Fman todo todo
type Fman chan map[string]chan *os.File

func fileOffsetSize(manChannel Fman, basePath string, position, size uint64) {
	man := <-manChannel
	relativePath, _, _ := PathOffsetSize(position, size)
	fileChannel := man[relativePath]
	var file *os.File
	if fileChannel == nil {
	} else {
		file = <-fileChannel
	}
	_ = file
}

// closeOne closes one available file, returning its relative path
func closeOne(man map[string]chan *os.File) string {
	for relativePath, fileChannel := range man {
		for file := range fileChannel {
			fileChannel <- nil
			if file != nil {
				file.Close()
				return relativePath
			}
		}
	}
	return ""
}

// Read3 todo todo
func Read3(manChannel Fman, basePath string, position, size uint64) ([]byte, error) {
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
			return Read3(manChannel, basePath, position, size)
		}
	} else {
		if len(man) > 4 {
			closedPath := closeOne(man)
			if closedPath == "" {
				for _, fileChannel = range man {
					manChannel <- man
					file := <-fileChannel
					fileChannel <- nil
					if file != nil {
						file.Close()
					}
					return Read3(manChannel, basePath, position, size)
				}
			}
			delete(man, closedPath)
			manChannel <- man
			return Read3(manChannel, basePath, position, size)
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

// Read4 todo todo
func Read4(fileChannel chan *os.File, offsetInFile, bytesToRead uint64) ([]byte, error) {
	file := <-fileChannel
	if file == nil {
		return nil, nil
	}
	buffer := make([]byte, bytesToRead)
	var err error
	if _, err = file.Seek(int64(offsetInFile), io.SeekStart); err != nil {
		return nil, err
	}
	if _, err = io.ReadFull(file, buffer); err != nil {
		log.Println(err)
	}
	return buffer, err
}
