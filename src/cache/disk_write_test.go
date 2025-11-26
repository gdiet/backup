package cache

import (
	"os"
	"reflect"
	"testing"
)

func TestDiskWrite(t *testing.T) {
	t.Run("write to new file", func(t *testing.T) {
		path := "test_disk_write_new.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		data := bytes([]byte{1, 2, 3, 4, 5})
		err := d.write(0, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("failed to open file: %v", err)
		}
		buf := make([]byte, 5)
		_, err = f.ReadAt(buf, 0)
		_ = f.Close()
		_ = os.Remove(path)
		if !reflect.DeepEqual(buf, []byte{1, 2, 3, 4, 5}) {
			t.Errorf("expected file contents [1 2 3 4 5], got %v", buf)
		}
		if !reflect.DeepEqual(d.areas, areas{{off: 0, len: 5}}) {
			t.Errorf("expected disk areas [{0 5}], got %v", d.areas)
		}
	})

	t.Run("write at offset", func(t *testing.T) {
		path := "test_disk_write_offset.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		data := bytes([]byte{9, 8, 7})
		err := d.write(5, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("failed to open file: %v", err)
		}
		buf := make([]byte, 8)
		_, err = f.ReadAt(buf, 0)
		_ = f.Close()
		_ = os.Remove(path)
		want := []byte{0, 0, 0, 0, 0, 9, 8, 7}
		if !reflect.DeepEqual(buf, want) {
			t.Errorf("expected file contents %v, got %v", want, buf)
		}
		if !reflect.DeepEqual(d.areas, areas{{off: 5, len: 3}}) {
			t.Errorf("expected disk areas [{5 3}], got %v", d.areas)
		}
	})

	t.Run("write empty data", func(t *testing.T) {
		path := "test_disk_write_empty.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		data := bytes([]byte{})
		err := d.write(0, data)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.file != nil {
			t.Errorf("file should not be opened for empty write")
		}
		if len(d.areas) != 0 {
			t.Errorf("expected no disk areas, got %v", d.areas)
		}
	})

	t.Run("write twice, overwrite", func(t *testing.T) {
		path := "test_disk_write_overwrite.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		err := d.write(0, []byte{1, 2, 3, 4})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		err = d.write(2, []byte{9, 8})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("failed to open file: %v", err)
		}
		buf := make([]byte, 4)
		_, err = f.ReadAt(buf, 0)
		_ = f.Close()
		_ = os.Remove(path)
		want := []byte{1, 2, 9, 8}
		if !reflect.DeepEqual(buf, want) {
			t.Errorf("expected file contents %v, got %v", want, buf)
		}
		if !reflect.DeepEqual(d.areas, areas{{off: 0, len: 4}}) {
			t.Errorf("expected disk areas [{0 4}], got %v", d.areas)
		}
	})

	t.Run("write to file fails", func(t *testing.T) {
		// mainly here for code coverage
		path := "test_disk_write_fails.tmp"
		_ = os.Remove(path)
		file, _ := os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0644)
		d := &disk{filePath: path, file: file}
		data := bytes([]byte{9, 8, 7})
		err := d.write(5, data)
		_ = os.Remove(path)
		if err == nil {
			t.Fatalf("expected error, but got none")
		}
	})

	t.Run("write - open file fails", func(t *testing.T) {
		// mainly here for code coverage
		d := &disk{filePath: ""}
		data := bytes([]byte{9, 8, 7})
		err := d.write(5, data)
		if err == nil {
			t.Fatalf("expected error, but got none")
		}
	})
}
