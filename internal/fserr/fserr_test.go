package fserr_test

import (
	"testing"

	"github.com/gdiet/backup/internal/fserr"
	"github.com/stretchr/testify/assert"
)

func TestIO(t *testing.T) {
	assert.PanicsWithValue(t, "assertion failed: input/output error", func() { _ = fserr.IO() })
}
