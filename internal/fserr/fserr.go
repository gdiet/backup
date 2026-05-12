package fserr

import (
	"errors"

	"github.com/gdiet/backup/internal/util"
)

var (
	Exists   = errors.New("already exists")
	Invalid  = errors.New("invalid operation")
	IoRaw    = errors.New("input/output error")
	IsDir    = errors.New("is a directory")
	IsRoot   = errors.New("is root directory")
	NotDir   = errors.New("not a directory")
	NotEmpty = errors.New("directory not empty")
	NotFound = errors.New("not found")
)

type ioError struct{ cause error }

func (e *ioError) Error() string   { return IoRaw.Error() + ": " + e.cause.Error() }
func (e *ioError) Unwrap() []error { return []error{IoRaw, e.cause} }

func IO(cause error) error {
	util.AssertionFailed(IoRaw.Error())
	return &ioError{cause: cause}
}
