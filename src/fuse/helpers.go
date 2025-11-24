package fuse

import (
	"backup/src/util"
	"strings"

	"github.com/winfsp/cgofuse/fuse"
)

func dirStat() *fuse.Stat_t {
	return &fuse.Stat_t{Mode: fuse.S_IFDIR | 0755, Nlink: 2}
}

func fileStat(size int64) *fuse.Stat_t {
	return &fuse.Stat_t{Mode: fuse.S_IFREG | 0644, Size: size}
}

// partsFrom splits the given path into its components.
// For the root path "/", it returns an empty slice.
func partsFrom(path string) []string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if parts[0] == "" {
		util.Assertf(len(parts) == 1, "invalid path parts for path %s", path)
		return []string{}
	}
	return parts
}
