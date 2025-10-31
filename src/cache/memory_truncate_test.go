package cache

import (
	"reflect"
	"testing"
)

func TestMemoryTruncate(t *testing.T) {
	t.Run("truncate empty memory", func(t *testing.T) {
		mem := &memory{}
		memoryDelta := mem.shrink(10)
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if memoryDelta != 0 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 0)
		}
	})

	t.Run("truncate to zero", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{0, bytes{1, 2, 3}}}}
		memoryDelta := mem.shrink(0)
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if memoryDelta != -3 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, -3)
		}
	})

	t.Run("truncate in the middle of an area", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{0, bytes{1, 2, 3, 4, 5}}}}
		memoryDelta := mem.shrink(3)
		want := dataAreas{{0, bytes{1, 2, 3}}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("expected areas %+v, got %+v", want, mem.areas)
		}
		if memoryDelta != -2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, -2)
		}
	})

	t.Run("truncate between areas", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{0, bytes{1, 2}},
			{2, bytes{3, 4}},
			{4, bytes{5, 6}},
		}}
		memoryDelta := mem.shrink(3)
		want := dataAreas{{0, bytes{1, 2}}, {2, bytes{3}}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("expected areas %+v, got %+v", want, mem.areas)
		}
		if memoryDelta != -3 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, -3)
		}
	})

	t.Run("truncate at area boundary", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{0, bytes{1, 2}},
			{2, bytes{3, 4}},
		}}
		memoryDelta := mem.shrink(2)
		want := dataAreas{{0, bytes{1, 2}}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("expected areas %+v, got %+v", want, mem.areas)
		}
		if memoryDelta != -2 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, -2)
		}
	})

	t.Run("truncate beyond all areas", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{0, bytes{1, 2, 3}}}}
		memoryDelta := mem.shrink(10)
		want := dataAreas{{0, bytes{1, 2, 3}}}
		if !reflect.DeepEqual(mem.areas, want) {
			t.Errorf("expected areas %+v, got %+v", want, mem.areas)
		}
		if memoryDelta != 0 {
			t.Errorf("unexpected memoryDelta: got %d, want %d", memoryDelta, 0)
		}
	})
}
