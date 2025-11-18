package main

import (
	"backup/src/util"
	"io"
	"strings"

	"github.com/winfsp/cgofuse/fuse"
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
		util.Assert(len(parts) == 1, "invalid path parts")
		return []string{}
	}
	return parts
}
