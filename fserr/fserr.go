package fserr

import (
	"errors"

	"github.com/gdiet/backup/util"
)

var (
	Exists   = errors.New("already exists")
	Invalid  = errors.New("invalid operation")
	IO_RAW   = errors.New("input/output error")
	IsDir    = errors.New("is a directory")
	IsRoot   = errors.New("is root directory")
	NotDir   = errors.New("not a directory")
	NotEmpty = errors.New("directory not empty")
	NotFound = errors.New("not found")
)

func IO() error {
	util.AssertionFailed(IO_RAW.Error())
	return IO_RAW
}
