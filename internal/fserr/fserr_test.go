package fserr_test

import (
	"errors"
	"testing"

	"github.com/gdiet/backup/internal/fserr"
	"github.com/stretchr/testify/assert"
)

func TestIO(t *testing.T) {
	cause := errors.New("disk read failed")
	assert.PanicsWithValue(t, "assertion failed: input/output error", func() { _ = fserr.IO(cause) })
}
