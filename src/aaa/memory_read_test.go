package aaa

import (
	"reflect"
	"testing"
)

func TestMemoryRead(t *testing.T) {
	const unreadMsg = "expected unreadAreas %+v, got %+v"

	t.Run("read from empty memory", func(t *testing.T) {
		mem := &memory{}
		buf := make([]byte, 5)
		unread := mem.read(0, bytes(buf))
		if !reflect.DeepEqual(buf, make([]byte, 5)) {
			t.Errorf("expected buffer to be zeroed, got %v", buf)
		}
		want := areas{{off: 0, len: 5}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf(unreadMsg, want, unread)
		}
	})

	t.Run("read zero length", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes([]byte{1, 2, 3, 4, 5}), 1024)
		buf := make([]byte, 0)
		unread := mem.read(3, bytes(buf))
		if !reflect.DeepEqual(buf, []byte{}) {
			t.Errorf("expected buffer to be empty, got %v", buf)
		}
		if len(unread) != 0 {
			t.Errorf("expected no unreadAreas, got %+v", unread)
		}
	})

	t.Run("read fully covered", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes([]byte{1, 2, 3, 4, 5}), 1024)
		buf := make([]byte, 5)
		unread := mem.read(0, bytes(buf))
		if !reflect.DeepEqual(buf, []byte{1, 2, 3, 4, 5}) {
			t.Errorf("expected buffer to be [1 2 3 4 5], got %v", buf)
		}
		if len(unread) != 0 {
			t.Errorf("expected no unreadAreas, got %+v", unread)
		}
	})

	t.Run("read partially covered at start", func(t *testing.T) {
		mem := &memory{}
		mem.write(2, bytes([]byte{3, 4, 5}), 1024)
		buf := make([]byte, 5)
		unread := mem.read(0, bytes(buf))
		if !reflect.DeepEqual(buf, []byte{0, 0, 3, 4, 5}) {
			t.Errorf("expected buffer to be [0 0 3 4 5], got %v", buf)
		}
		want := areas{{off: 0, len: 2}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf(unreadMsg, want, unread)
		}
	})

	t.Run("read partially covered at end", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes([]byte{1, 2, 3}), 1024)
		buf := make([]byte, 5)
		unread := mem.read(0, bytes(buf))
		if !reflect.DeepEqual(buf, []byte{1, 2, 3, 0, 0}) {
			t.Errorf("expected buffer to be [1 2 3 0 0], got %v", buf)
		}
		want := areas{{off: 3, len: 2}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf(unreadMsg, want, unread)
		}
	})

	t.Run("read with gap in the middle", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes([]byte{1, 2}), 1024)
		mem.write(4, bytes([]byte{5, 6}), 1024)
		buf := make([]byte, 6)
		unread := mem.read(0, bytes(buf))
		if !reflect.DeepEqual(buf, []byte{1, 2, 0, 0, 5, 6}) {
			t.Errorf("expected buffer to be [1 2 0 0 5 6], got %v", buf)
		}
		want := areas{{off: 2, len: 2}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf(unreadMsg, want, unread)
		}
	})

	t.Run("read only from first part", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes([]byte{1, 2}), 1024)
		mem.write(4, bytes([]byte{5, 6}), 1024)
		buf := make([]byte, 2)
		unread := mem.read(1, bytes(buf))
		if !reflect.DeepEqual(buf, []byte{2, 0}) {
			t.Errorf("expected buffer to be [2 0], got %v", buf)
		}
		want := areas{{off: 2, len: 1}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf(unreadMsg, want, unread)
		}
	})

	t.Run("read only from second part", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes([]byte{1, 2}), 1024)
		mem.write(4, bytes([]byte{5, 6}), 1024)
		buf := make([]byte, 4)
		unread := mem.read(3, bytes(buf))
		if !reflect.DeepEqual(buf, []byte{0, 5, 6, 0}) {
			t.Errorf("expected buffer to be [0, 5, 6, 0], got %v", buf)
		}
		want := areas{{off: 3, len: 1}, {off: 6, len: 1}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf(unreadMsg, want, unread)
		}
	})
}
