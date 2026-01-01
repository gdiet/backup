package fserr

import (
	"errors"
)

var (
	Exists   = errors.New("already exists")
	Invalid  = errors.New("invalid operation")
	IO       = errors.New("input/output error") // TODO Add assertion failed to most or all places that return this
	IsDir    = errors.New("is a directory")
	IsRoot   = errors.New("is root directory")
	NotDir   = errors.New("not a directory")
	NotEmpty = errors.New("directory not empty")
	NotFound = errors.New("not found")
)
