package aaa

import (
	"os"
	"testing"
)

func TestDiskClose(t *testing.T) {
	t.Run("close open file", func(t *testing.T) {
		path := "test_disk_close.tmp"
		_ = os.Remove(path)
		file, err := os.Create(path)
		if err != nil {
			t.Fatalf("failed to create test file: %v", err)
		}
		_, _ = file.Write([]byte("test"))
		_ = file.Close()

		d := &disk{filePath: path}
		file, err = os.OpenFile(path, os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("failed to open test file: %v", err)
		}
		d.file = file

		d.close()

		if d.file != nil {
			t.Errorf("expected file to be nil after close, got %v", d.file)
		}
		if d.areas != nil {
			t.Errorf("expected areas to be nil after close, got %v", d.areas)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("expected file to be removed, but it still exists")
		}
	})

	t.Run("close nil file", func(t *testing.T) {
		d := &disk{}
		d.close()
		if d.file != nil {
			t.Errorf("expected file to be nil, got %v", d.file)
		}
		if d.areas != nil {
			t.Errorf("expected areas to be nil, got %v", d.areas)
		}
	})
}
