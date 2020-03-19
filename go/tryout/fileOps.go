package tryout

import "fmt"

// Fman todo todo
type Fman chan map[string]chan []byte

// CloseOne closes one available file, returning its relative path
func CloseOne(man *map[string]chan []byte) string {
	fmt.Printf("closeOne: map size %d\n", len(*man))
	for relativePath, fileChannel := range *man {
		fmt.Printf("closeOne: path %s\n", relativePath)
		select {
		case file := <-fileChannel:
			delete(*man, relativePath)
			fmt.Printf("closeOne: file %d\n", len(file))
			fileChannel <- nil
			if file != nil {
				fmt.Printf("closeOne: close %s\n", relativePath)
				return relativePath
			}
		default:
			fmt.Printf("closeOne: skipped %s\n", relativePath)
		}
	}
	return ""
}
