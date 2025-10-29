package aaa

import (
	"reflect"
	"testing"
)

func TestSparseRead(t *testing.T) {
	t.Run("read from empty sparse", func(t *testing.T) {
		s := &sparse{}
		buf := bytes([]byte{1, 2, 3, 4, 5})
		unread := s.read(0, buf)
		if !reflect.DeepEqual([]byte(buf), []byte{1, 2, 3, 4, 5}) {
			t.Errorf("expected buffer unchanged, got %v", buf)
		}
		want := areas{{off: 0, len: 5}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})

	t.Run("read no data", func(t *testing.T) {
		s := &sparse{sparseAreas: areas{{off: 0, len: 5}}}
		buf := bytes([]byte{})
		unread := s.read(0, buf)
		if len(unread) != 0 {
			t.Errorf("expected empty unreadAreas, got %v", unread)
		}
	})

	t.Run("read fully sparse", func(t *testing.T) {
		s := &sparse{sparseAreas: areas{{off: 0, len: 5}}}
		buf := bytes([]byte{9, 9, 9, 9, 9})
		unread := s.read(0, buf)
		if !reflect.DeepEqual([]byte(buf), []byte{0, 0, 0, 0, 0}) {
			t.Errorf("expected buffer to be all zeros, got %v", buf)
		}
		if len(unread) != 0 {
			t.Errorf("expected empty unreadAreas, got %v", unread)
		}
	})

	t.Run("read partially sparse at start", func(t *testing.T) {
		s := &sparse{sparseAreas: areas{{off: 0, len: 2}}}
		buf := bytes([]byte{1, 2, 3, 4, 5})
		unread := s.read(0, buf)
		if !reflect.DeepEqual([]byte(buf), []byte{0, 0, 3, 4, 5}) {
			t.Errorf("expected buffer to be [0 0 3 4 5], got %v", buf)
		}
		want := areas{{off: 2, len: 3}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})

	t.Run("read partially sparse at end", func(t *testing.T) {
		s := &sparse{sparseAreas: areas{{off: 3, len: 2}}}
		buf := bytes([]byte{1, 2, 3, 4, 5})
		unread := s.read(0, buf)
		if !reflect.DeepEqual([]byte(buf), []byte{1, 2, 3, 0, 0}) {
			t.Errorf("expected buffer to be [1 2 3 0 0], got %v", buf)
		}
		want := areas{{off: 0, len: 3}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})

	t.Run("read with gap in sparse", func(t *testing.T) {
		s := &sparse{sparseAreas: areas{{off: 1, len: 2}, {off: 4, len: 1}}}
		buf := bytes([]byte{9, 9, 9, 9, 9, 9})
		unread := s.read(0, buf)
		if !reflect.DeepEqual([]byte(buf), []byte{9, 0, 0, 9, 0, 9}) {
			t.Errorf("expected buffer to be [9 0 0 9 0 9], got %v", buf)
		}
		want := areas{{off: 0, len: 1}, {off: 3, len: 1}, {off: 5, len: 1}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})

	t.Run("read in the middle of sparse areas", func(t *testing.T) {
		s := &sparse{sparseAreas: areas{{off: 0, len: 2}, {off: 4, len: 1}, {off: 7, len: 2}}}
		buf := bytes([]byte{9, 9, 9})
		unread := s.read(3, buf)
		if !reflect.DeepEqual([]byte(buf), []byte{9, 0, 9}) {
			t.Errorf("expected buffer to be [9 0 9], got %v", buf)
		}
		want := areas{{off: 3, len: 1}, {off: 5, len: 1}}
		if !reflect.DeepEqual(unread, want) {
			t.Errorf("expected unreadAreas %v, got %v", want, unread)
		}
	})
}
