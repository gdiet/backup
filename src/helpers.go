package main

import (
	"backup/src/util"
	"strings"

	"github.com/winfsp/cgofuse/fuse"
)

func dirStat() *fuse.Stat_t {
	return &fuse.Stat_t{Mode: fuse.S_IFDIR | 0755, Nlink: 2}
}

// partsFrom splits the given path into its components.
// For the root path "/", it returns an empty slice.
func partsFrom(path string) []string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if parts[0] == "" {
		util.Assert(len(parts) == 1, "invalid path parts")
		return []string{}
	}
	return parts
}
