package cache

import (
	"os"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
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

	t.Run("read zero length", func(t *testing.T) {
		path := "test_disk_read_zero_length.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2, 3, 4, 5}))
		buf := make([]byte, 0)
		unread, err := d.read(0, bytes(buf))
		_ = os.Remove(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(buf, []byte{}) {
			t.Errorf("expected buffer to be empty, got %v", buf)
		}
		if len(unread) != 0 {
			t.Errorf("expected no unreadAreas, got %v", unread)
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

	t.Run("read with gap in the middle and more data at the end", func(t *testing.T) {
		path := "test_disk_read_gap_end.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2}))
		_ = d.write(4, bytes([]byte{3, 4}))
		_ = d.write(8, bytes([]byte{5, 6}))
		_ = d.write(12, bytes([]byte{8, 9}))
		_ = d.write(16, bytes([]byte{1, 2}))
		buf := make([]byte, 8)
		unread, err := d.read(5, bytes(buf))
		_ = os.Remove(path)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !reflect.DeepEqual(buf, []byte{4, 0, 0, 5, 6, 0, 0, 8}) {
			t.Errorf("expected buffer to be [4 0 0 5 6 0 0 8], got %v", buf)
		}
		want := areas{{off: 6, len: 2}, {off: 10, len: 2}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})

	t.Run("read from file get EOF", func(t *testing.T) {
		// mainly here for code coverage
		path := "test_disk_read_EOF.tmp"
		_ = os.Remove(path)
		d := &disk{filePath: path}
		_ = d.write(0, bytes([]byte{1, 2}))
		// manipulate areas to cause read beyond EOF
		d.areas = append(d.areas, area{off: 5, len: 5})
		buf := []byte{9, 9, 9, 9, 9}
		require.Panics(t, func() {
			d.read(1, bytes(buf))
		})
		_ = os.Remove(path)
		if !reflect.DeepEqual(buf, []byte{2, 9, 9, 9, 0}) {
			t.Errorf("expected buffer to be [2 9 9 9 0], got %v", buf)
		}
	})

	t.Run("read from file fails", func(t *testing.T) {
		// mainly here for code coverage
		path := "test_disk_read_fails.tmp"
		_ = os.Remove(path)
		file, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
		// use write-only file to cause read failure
		d := &disk{filePath: path, file: file, areas: areas{{off: 2, len: 4}}}
		buf := make([]byte, 5)
		_, err := d.read(0, bytes(buf))
		_ = os.Remove(path)
		if err == nil {
			t.Fatalf("expected error, but got none")
		}
	})
}
