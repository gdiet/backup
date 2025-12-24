package main

import (
	"backup/src/fs"
	"fmt"
	"os"
)

func main() {

	// consider "flag" package if more arguments are needed
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <mountpoint>\n", os.Args[0])
		os.Exit(1)
	}

	fs.Mount(os.Args[1])
}
