package aaa

import (
	"reflect"
	"testing"
)

func TestMemoryWrite(t *testing.T) {

	t.Run("write to empty memory", func(t *testing.T) {
		mem := &memory{}
		pos := 0
		data := bytesOf(1, 2, 3)
		mem.write(pos, data, 1024)
		if len(mem.areas) != 1 {
			t.Fatalf("expected 1 area, got %d", len(mem.areas))
		}
		if mem.areas[0].position != pos || !reflect.DeepEqual(mem.areas[0].data, data) {
			t.Errorf("unexpected area: got %+v", mem.areas[0])
		}
		// FIXME ensure we have a copy of data, not the same slice
	})

	t.Run("overwrite existing area", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{position: 0, data: bytesOf(1, 2, 3, 4, 5)}}}
		pos := 1
		data := bytesOf(9, 9, 9)
		mem.write(pos, data, 1024)
		// Expect: [1 9 9 9 5] - merged into one area
		want := dataAreas{
			{position: 0, data: bytesOf(1, 9, 9, 9, 5)},
		}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after overwrite: got %+v, want %+v", mem.areas, want)
		}
		// FIXME ensure we have a copy of data, not the same slice
		// FIXME add test for no merge due to size limit
	})

	t.Run("merge adjacent areas", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 0, data: bytesOf(1, 2)},
			{position: 2, data: bytesOf(3, 4)},
		}}
		mem.write(4, bytesOf(5, 6), 1024)
		// Should merge all into one area
		want := dataAreas{{position: 0, data: bytesOf(1, 2, 3, 4, 5, 6)}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after merge: got %+v, want %+v", mem.areas, want)
		}
	})
}

func bytesOf(b ...byte) bytes {
	return bytes(b)
}
