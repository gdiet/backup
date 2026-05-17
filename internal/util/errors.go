package util

import (
	"errors"
	"fmt"
)

// InvalidError indicates incorrect usage of the application (e.g. missing arguments, invalid flags, etc.)
// as opposed to runtime problems (e.g. corrupt database).
type InvalidError struct {
	message string
	err     error
}

func NewInvalidError(message string) error {
	return &InvalidError{message: message}
}

func (e *InvalidError) Error() string {
	switch {
	case e.err != nil && e.message != "":
		return fmt.Sprintf("%s: %v", e.message, e.err)
	case e.err != nil:
		return e.err.Error()
	default:
		return e.message
	}
}

func (e *InvalidError) Unwrap() error {
	return e.err
}

func IsInvalid(err error) bool {
	_, isInvalid := errors.AsType[*InvalidError](err)
	return isInvalid
}
