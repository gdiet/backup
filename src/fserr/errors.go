package fserr

import (
	"errors"
	"fmt"
)

var (
	ErrExists   = errors.New("already exists")
	ErrInvalid  = errors.New("invalid operation")
	ErrIsDir    = errors.New("is a directory")
	ErrIsRoot   = errors.New("is root directory")
	ErrNotDir   = errors.New("not a directory")
	ErrNotEmpty = errors.New("directory not empty")
	ErrNotFound = errors.New("not found")
)

type DeserializationError struct {
	Msg string
}

func (e *DeserializationError) Error() string {
	return fmt.Sprintf("DeserializationError: %s", e.Msg)
}
