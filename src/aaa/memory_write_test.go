package aaa

import (
	"reflect"
	"testing"
)

func TestMemoryWrite(t *testing.T) {
	t.Run("no merge possible", func(t *testing.T) {
		mem := &memory{}
		// Prepare left and right areas
		left := dataArea{position: 0, data: bytes{1, 2, 3}}
		right := dataArea{position: 10, data: bytes{7, 8, 9}}
		mem.areas = dataAreas{left, right}

		// Write new data in the gap, mergeSizeHint too small for any merge
		pos := 5
		data := bytes{4, 5, 6}
		mergeSizeHint := 2 // deliberately too small
		mem.write(pos, data, mergeSizeHint)

		want := dataAreas{left, {position: pos, data: data}, right}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after no-merge write: got %+v, want %+v", mem.areas, want)
		}
	})

	t.Run("write to empty memory", func(t *testing.T) {
		mem := &memory{}
		area := dataArea{0, bytesOf(1, 2, 3)}
		memoryDelta := mem.write(area.position, area.data, 1024)

		// create a deep copy of area for comparison
		areaCopy := area.copy()
		// overwrite data in place to prove write has used a deep copy
		copy(area.data, make(bytes, area.len()))

		if len(mem.areas) != 1 {
			t.Fatalf("expected 1 area, got %d", len(mem.areas))
		}
		if mem.areas[0].position != areaCopy.position || !reflect.DeepEqual(mem.areas[0].data, areaCopy.data) {
			t.Errorf("unexpected area: got %+v", mem.areas[0])
		}
		if memoryDelta != areaCopy.len() {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, areaCopy.len())
		}
	})

	t.Run("write zero length data", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{position: 0, data: bytesOf(1, 2, 3, 4, 5)}}}
		pos := 1
		data := bytesOf()
		memoryDelta := mem.write(pos, data, 1024)

		// Expect: [1 2 3 4 5] - merged into one area
		want := dataAreas{
			{position: 0, data: bytesOf(1, 2, 3, 4, 5)},
		}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after overwrite: got %+v, want %+v", mem.areas, want)
		}
		if memoryDelta != 0 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 0)
		}
	})

	t.Run("overwrite existing area", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{position: 0, data: bytesOf(1, 2, 3, 4, 5)}}}
		pos := 1
		data := bytesOf(9, 9, 9)
		memoryDelta := mem.write(pos, data, 1024)

		// overwrite data in place to prove write has used a deep copy
		copy(data, make(bytes, len(data)))

		// Expect: [1 9 9 9 5] - merged into one area
		want := dataAreas{
			{position: 0, data: bytesOf(1, 9, 9, 9, 5)},
		}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after overwrite: got %+v, want %+v", mem.areas, want)
		}
		if memoryDelta != 0 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 0)
		}
	})

	t.Run("merge adjacent areas", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 0, data: bytesOf(1, 2)},
			{position: 2, data: bytesOf(3, 4)},
		}}
		data := bytesOf(5, 6)
		memoryDelta := mem.write(4, data, 1024)

		// overwrite data in place to prove write has used a deep copy
		copy(data, make(bytes, len(data)))

		// Expect: [1 2] [3 4 5 6]
		want := dataAreas{{position: 0, data: bytesOf(1, 2)}, {position: 2, data: bytesOf(3, 4, 5, 6)}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after merge: got %+v, want %+v", mem.areas, want)
		}
		if memoryDelta != 2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 2)
		}
	})

	t.Run("no merge left because there is nothing, no merge right due to size limit", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 2, data: bytesOf(3, 4)},
		}}
		data := bytesOf(5, 6)
		memoryDelta := mem.write(0, data, 3)

		// overwrite data in place to prove write has used a deep copy
		copy(data, make(bytes, len(data)))

		// Expect: [5 6] [3 4]
		want := dataAreas{{position: 0, data: bytesOf(5, 6)}, {position: 2, data: bytesOf(3, 4)}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after merge: got %+v, want %+v", mem.areas, want)
		}
		if memoryDelta != 2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 2)
		}
	})

	t.Run("merge from the middle", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 0, data: bytesOf(1, 2)},
			{position: 4, data: bytesOf(3, 4)},
		}}
		data := bytesOf(5, 6)
		memoryDelta := mem.write(2, data, 1024)

		// overwrite data in place to prove write has used a deep copy
		copy(data, make(bytes, len(data)))

		// Expect: [1 2 5 6 3 4]
		want := dataAreas{{position: 0, data: bytesOf(1, 2, 5, 6, 3, 4)}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after merge: got %+v, want %+v", mem.areas, want)
		}
		if memoryDelta != 2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 2)
		}
	})

	t.Run("merge from the middle, no merge right due to size limit", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 0, data: bytesOf(1, 2)},
			{position: 4, data: bytesOf(3, 4)},
		}}
		data := bytesOf(5, 6)
		memoryDelta := mem.write(2, data, 5)

		// overwrite data in place to prove write has used a deep copy
		copy(data, make(bytes, len(data)))

		// Expect: [1 2 5 6] [3 4]
		want := dataAreas{{position: 0, data: bytesOf(1, 2, 5, 6)}, {position: 4, data: bytesOf(3, 4)}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after merge: got %+v, want %+v", mem.areas, want)
		}
		if memoryDelta != 2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 2)
		}
	})

	t.Run("merge from the middle, no merge left due to size limit", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 0, data: bytesOf(1, 2, 3, 4, 5, 6, 7, 8, 9)},
		}}
		data := bytesOf(5, 6)
		memoryDelta := mem.write(5, data, 4)

		// overwrite data in place to prove write has used a deep copy
		copy(data, make(bytes, len(data)))

		// Expect: [1 2 3 4 5] [5 6 8 9]
		want := dataAreas{{position: 0, data: bytesOf(1, 2, 3, 4, 5)}, {position: 5, data: bytesOf(5, 6, 8, 9)}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after merge: got %+v, want %+v", mem.areas, want)
		}
		if memoryDelta != 0 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 0)
		}
	})

	t.Run("put into the middle, no merge due to size limit", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 0, data: bytesOf(1, 2)},
			{position: 4, data: bytesOf(3, 4)},
		}}
		data := bytesOf(5, 6, 7)
		memoryDelta := mem.write(1, data, 3)

		// overwrite data in place to prove write has used a deep copy
		copy(data, make(bytes, len(data)))

		// Expect: [1] [5 6 7] [3 4]
		want := dataAreas{{position: 0, data: bytesOf(1)}, {position: 1, data: bytesOf(5, 6, 7)}, {position: 4, data: bytesOf(3, 4)}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("areas after merge: got %+v, want %+v", mem.areas, want)
		}
		if memoryDelta != 2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 2)
		}
	})

	// FIXME add test for memory released when splitting or truncating
}

func bytesOf(b ...byte) bytes {
	return bytes(b)
}
