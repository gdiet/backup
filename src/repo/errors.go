package repo

import (
	"errors"
)

var (
	EExists   = errors.New("already exists")
	EInvalid  = errors.New("invalid operation")
	EIO       = errors.New("input/output error")
	EIsDir    = errors.New("is a directory")
	EIsRoot   = errors.New("is root directory")
	ENotDir   = errors.New("not a directory")
	ENotEmpty = errors.New("directory not empty")
	ENotFound = errors.New("not found")
)
