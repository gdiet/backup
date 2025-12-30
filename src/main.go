package main

import (
	"backup/src/fs"
	"fmt"
	"os"
)

func main() {

	// consider "flag" package if more arguments are needed
	if len(os.Args) != 3 {
		fmt.Fprintf(os.Stderr, "Usage: %s <repository> <mountpoint>\n", os.Args[0])
		os.Exit(1)
	}

	err := fs.Mount(os.Args[1], os.Args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to mount filesystem: %v\n", err)
		os.Exit(1)
	}
}
