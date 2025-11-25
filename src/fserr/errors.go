package fserr

import (
	"errors"
	"fmt"
)

var (
	Exists   = errors.New("already exists")
	Invalid  = errors.New("invalid operation")
	IsDir    = errors.New("is a directory")
	IsRoot   = errors.New("is root directory")
	NotDir   = errors.New("not a directory")
	NotEmpty = errors.New("directory not empty")
	NotFound = errors.New("not found")
)

type DeserializationError struct {
	Msg string
}

func (e *DeserializationError) Error() string {
	return fmt.Sprintf("DeserializationError: %s", e.Msg)
}
