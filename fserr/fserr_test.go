package fserr_test

import (
	"testing"

	"github.com/gdiet/backup/fserr"
)

func TestIO(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Errorf("IO() did not panic, want panic with assertion failed message")
			return
		}
		msg, ok := recovered.(string)
		if !ok || msg != "assertion failed: input/output error" {
			t.Errorf("IO() panic message = %v, want %q", recovered, "assertion failed: input/output error")
		}
	}()
	fserr.IO()
	t.Errorf("IO() should have panicked, but did not")
}
