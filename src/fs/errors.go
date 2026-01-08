package fs

import (
	"backup/src/fserr"
	"backup/src/util"
	"errors"

	"github.com/winfsp/cgofuse/fuse"
)

var errorMap = map[error]int{
	fserr.NotFound: -fuse.ENOENT,
	fserr.NotDir:   -fuse.ENOTDIR,
	fserr.Exists:   -fuse.EEXIST,
	fserr.NotEmpty: -fuse.ENOTEMPTY,
	fserr.IsRoot:   -fuse.EBUSY,
	fserr.IsDir:    -fuse.EISDIR,
	fserr.Invalid:  -fuse.EINVAL,
	fserr.IO_RAW:   -fuse.EIO,
}

// mapError converts fserr errors to fuse errors, only allowing specified error types
func mapError(err error, allowed ...error) int {
	if err == nil {
		return 0
	}

	// Check if error is in allowed list
	for _, allowedErr := range allowed {
		if errors.Is(err, allowedErr) {
			if fuseCode, exists := errorMap[allowedErr]; exists {
				return fuseCode
			}
		}
	}

	// Unexpected error
	util.AssertionFailedf("unexpected error %v", err)
	return -fuse.EIO
}
