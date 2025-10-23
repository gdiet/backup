package aaa

import (
	"os"
	"testing"
)

func TestDiskClose(t *testing.T) {
	t.Run("close open file", func(t *testing.T) {
		path := "test_disk_close.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		d.write(0, bytes{1, 2, 3})
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected file to be created, but it does not exist")
		}
		d.close()
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("expected file to be removed, but it still exists")
		}
		if d.file != nil {
			t.Errorf("expected file to be nil after close, got %v", d.file)
		}
		if d.areas != nil {
			t.Errorf("expected areas to be nil after close, got %v", d.areas)
		}
	})
}
