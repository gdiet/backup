package util

import (
	"errors"
	"fmt"
)

// InvalidError is to indicate incorrect usage of the application (e.g. missing arguments, invalid flags, etc.).
var InvalidError = errors.New("incorrect usage")

func Invalid(message string) error {
	return fmt.Errorf("%w: "+message, InvalidError)
}

func Invalidf(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{InvalidError}, args...)...)
}
