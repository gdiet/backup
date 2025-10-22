package aaa

import (
	"os"
	"reflect"
	"testing"
)

func TestDiskRead(t *testing.T) {
	t.Run("read from empty file", func(t *testing.T) {
		path := "test_disk_read_empty.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{})) // ensure file exists
		buf := make([]byte, 5)
		unread, err := d.read(0, bytes(buf))
		_ = os.Remove(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(buf, make([]byte, 5)) {
			t.Errorf("expected buffer to be zeroed, got %v", buf)
		}
		want := areas{{off: 0, len: 5}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})

	t.Run("read fully covered", func(t *testing.T) {
		path := "test_disk_read_full.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2, 3, 4, 5}))
		buf := make([]byte, 5)
		unread, err := d.read(0, bytes(buf))
		_ = os.Remove(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(buf, []byte{1, 2, 3, 4, 5}) {
			t.Errorf("expected buffer to be [1 2 3 4 5], got %v", buf)
		}
		if len(unread) != 0 {
			t.Errorf("expected no unreadAreas, got %v", unread)
		}
	})

	t.Run("read partially covered at start", func(t *testing.T) {
		path := "test_disk_read_partial_start.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(2, bytes([]byte{3, 4, 5}))
		buf := make([]byte, 5)
		unread, err := d.read(0, bytes(buf))
		_ = os.Remove(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(buf, []byte{0, 0, 3, 4, 5}) {
			t.Errorf("expected buffer to be [0 0 3 4 5], got %v", buf)
		}
		want := areas{{off: 0, len: 2}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})

	t.Run("read partially covered at end", func(t *testing.T) {
		path := "test_disk_read_partial_end.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2, 3}))
		buf := make([]byte, 5)
		unread, err := d.read(0, bytes(buf))
		_ = os.Remove(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(buf, []byte{1, 2, 3, 0, 0}) {
			t.Errorf("expected buffer to be [1 2 3 0 0], got %v", buf)
		}
		want := areas{{off: 3, len: 2}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})

	t.Run("read with gap in the middle", func(t *testing.T) {
		path := "test_disk_read_gap.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2}))
		_ = d.write(4, bytes([]byte{5, 6}))
		buf := make([]byte, 6)
		unread, err := d.read(0, bytes(buf))
		_ = os.Remove(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(buf, []byte{1, 2, 0, 0, 5, 6}) {
			t.Errorf("expected buffer to be [1 2 0 0 5 6], got %v", buf)
		}
		want := areas{{off: 2, len: 2}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})
}
