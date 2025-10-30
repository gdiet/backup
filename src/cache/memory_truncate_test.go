package cache

import (
	"reflect"
	"testing"
)

func TestMemoryTruncate(t *testing.T) {
	t.Run("truncate empty memory", func(t *testing.T) {
		mem := &memory{}
		memoryDelta := mem.truncate(10)
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if memoryDelta != 0 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 0)
		}
	})

	t.Run("truncate to zero", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{position: 0, data: bytes{1, 2, 3}}}}
		memoryDelta := mem.truncate(0)
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if memoryDelta != -3 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, -3)
		}
	})

	t.Run("truncate in the middle of an area", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{position: 0, data: bytes{1, 2, 3, 4, 5}}}}
		memoryDelta := mem.truncate(3)
		want := dataAreas{{position: 0, data: bytes{1, 2, 3}}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("expected areas %+v, got %+v", want, mem.areas)
		}
		if memoryDelta != -2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, -2)
		}
	})

	t.Run("truncate between areas", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 0, data: bytes{1, 2}},
			{position: 2, data: bytes{3, 4}},
			{position: 4, data: bytes{5, 6}},
		}}
		memoryDelta := mem.truncate(3)
		want := dataAreas{{position: 0, data: bytes{1, 2}}, {position: 2, data: bytes{3}}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("expected areas %+v, got %+v", want, mem.areas)
		}
		if memoryDelta != -3 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, -3)
		}
	})

	t.Run("truncate at area boundary", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{position: 0, data: bytes{1, 2}},
			{position: 2, data: bytes{3, 4}},
		}}
		memoryDelta := mem.truncate(2)
		want := dataAreas{{position: 0, data: bytes{1, 2}}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("expected areas %+v, got %+v", want, mem.areas)
		}
		if memoryDelta != -2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, -2)
		}
	})

	t.Run("truncate beyond all areas", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{position: 0, data: bytes{1, 2, 3}}}}
		memoryDelta := mem.truncate(10)
		want := dataAreas{{position: 0, data: bytes{1, 2, 3}}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("expected areas %+v, got %+v", want, mem.areas)
		}
		if memoryDelta != 0 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 0)
		}
	})
}
