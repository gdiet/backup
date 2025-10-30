package cache

import (
	"os"
	"reflect"
	"testing"
)

func TestDiskTruncate(t *testing.T) {
	t.Run("truncate to smaller size", func(t *testing.T) {
		path := "test_disk_truncate_smaller.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2, 3, 4, 5}))
		_ = d.write(10, bytes([]byte{6, 7, 8, 9, 10}))
		err := d.truncate(8)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		buf := make([]byte, 10)
		unreadAreas, _ := d.read(0, buf)
		if !reflect.DeepEqual(buf, []byte{1, 2, 3, 4, 5, 0, 0, 0, 0, 0}) {
			t.Errorf("expected file contents %v, got %v", []byte{1, 2, 3, 4, 5, 0, 0, 0, 0, 0}, buf)
		}
		if !reflect.DeepEqual(unreadAreas, areas{{off: 5, len: 5}}) {
			t.Errorf("expected unreadAreas %v, got %v", areas{{off: 5, len: 5}}, unreadAreas)
		}
		if !reflect.DeepEqual(d.areas, areas{{off: 0, len: 5}}) {
			t.Errorf("expected buffer contents %v, got %v", areas{{off: 0, len: 5}}, d.areas)
		}
		d.close()
	})

	t.Run("truncate in the middle of an area", func(t *testing.T) {
		path := "test_disk_truncate_middle.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2, 3, 4, 5}))
		_ = d.write(6, bytes([]byte{6, 7, 8, 9, 10}))
		err := d.truncate(8)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		buf := make([]byte, 8)
		unreadAreas, _ := d.read(0, buf)
		if !reflect.DeepEqual(buf, []byte{1, 2, 3, 4, 5, 0, 6, 7}) {
			t.Errorf("expected file contents %v, got %v", []byte{1, 2, 3, 4, 5, 0, 6, 7}, buf)
		}
		if !reflect.DeepEqual(unreadAreas, areas{{off: 5, len: 1}}) {
			t.Errorf("expected unreadAreas %v, got %v", areas{{off: 5, len: 1}}, unreadAreas)
		}
		if !reflect.DeepEqual(d.areas, areas{{off: 0, len: 5}, {off: 6, len: 2}}) {
			t.Errorf("expected buffer contents %v, got %v", areas{{off: 0, len: 5}, {off: 6, len: 2}}, d.areas)
		}
		d.close()
	})

	t.Run("truncate to larger size", func(t *testing.T) {
		path := "test_disk_truncate_larger.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2, 3, 4, 5}))
		_ = d.write(10, bytes([]byte{6, 7, 8, 9, 10}))
		err := d.truncate(16)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		buf := make([]byte, 2)
		unreadAreas, _ := d.read(14, buf)
		if !reflect.DeepEqual(buf, []byte{10, 0}) {
			t.Errorf("expected file contents %v, got %v", []byte{10, 0}, buf)
		}
		if !reflect.DeepEqual(unreadAreas, areas{{off: 15, len: 1}}) {
			t.Errorf("expected unreadAreas %v, got %v", areas{{off: 15, len: 1}}, unreadAreas)
		}
		if !reflect.DeepEqual(d.areas, areas{{off: 0, len: 5}, {off: 10, len: 5}}) {
			t.Errorf("expected disk areas %v, got %v", areas{{off: 0, len: 5}, {off: 10, len: 5}}, d.areas)
		}
		d.close()
	})

	t.Run("truncate to zero", func(t *testing.T) {
		path := "test_disk_truncate_zero.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2, 3, 4, 5}))
		_ = d.write(10, bytes([]byte{6, 7, 8, 9, 10}))
		err := d.truncate(0)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(d.areas, areas{}) {
			t.Errorf("expected buffer contents %v, got %v", areas{}, d.areas)
		}
		d.close()
	})

	t.Run("truncate file fails", func(t *testing.T) {
		// mainly here for code coverage
		path := "test_disk_truncate_fails.tmp"
		_ = os.Remove(path)
		file, _ := os.OpenFile(path, os.O_CREATE|os.O_RDONLY, 0644)
		d := &disk{filePath: path, file: file}
		err := d.truncate(5)
		_ = os.Remove(path)
		if err == nil {
			t.Fatalf("expected error, but got none")
		}
	})
}
