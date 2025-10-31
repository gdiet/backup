package cache

import (
	"reflect"
	"testing"
)

func TestMemoryRemove(t *testing.T) {
	t.Run("remove called on empty memory", func(t *testing.T) {
		mem := &memory{}
		delta := mem.remove(area{off: 0, len: 10})
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if delta != 0 {
			t.Errorf("expected memoryDelta 0, got %d", delta)
		}
	})

	t.Run("remove single area", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{5, bytes{1, 2, 3}}}}
		delta := mem.remove(area{off: 4, len: 5})
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if delta != -3 {
			t.Errorf("expected memoryDelta -3, got %d", delta)
		}
	})

	t.Run("remove multiple areas, two of them partially", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{0, bytes{1, 2}},
			{2, bytes{3, 4, 5}},
			{5, bytes{9, 9}},
			{7, bytes{4, 5}},
			{9, bytes{6, 7}},
		}}
		delta := mem.remove(area{off: 4, len: 4})
		if len(mem.areas) != 4 {
			t.Errorf("expected 4 areas, got %d", len(mem.areas))
		}
		if mem.areas[0].off != 0 || !reflect.DeepEqual(mem.areas[0].data, bytes{1, 2}) {
			t.Errorf("unexpected first area: got %+v", mem.areas[0])
		}
		if mem.areas[1].off != 2 || !reflect.DeepEqual(mem.areas[1].data, bytes{3, 4}) {
			t.Errorf("unexpected second area: got %+v", mem.areas[1])
		}
		if mem.areas[2].off != 8 || !reflect.DeepEqual(mem.areas[2].data, bytes{5}) {
			t.Errorf("unexpected third area: got %+v", mem.areas[2])
		}
		if mem.areas[3].off != 9 || !reflect.DeepEqual(mem.areas[3].data, bytes{6, 7}) {
			t.Errorf("unexpected fourth area: got %+v", mem.areas[3])
		}
		if delta != -4 {
			t.Errorf("expected memoryDelta -4, got %d", delta)
		}
	})

	t.Run("remove after truncate", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{0, bytes{1, 2, 3, 4, 5}}}}
		mem.shrink(3)
		da := mem.areas[0]
		delta := mem.remove(area{off: da.off, len: len(da.data)})
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if delta != -3 {
			t.Errorf("expected memoryDelta -3, got %d", delta)
		}
	})

	t.Run("remove after write", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes{1, 2, 3, 4}, 1024)
		da := mem.areas[0]
		delta := mem.remove(area{off: da.off, len: len(da.data)})
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if delta != -4 {
			t.Errorf("expected memoryDelta -4, got %d", delta)
		}
	})

	t.Run("remove after multiple writes", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes{1, 2}, 1024)
		mem.write(2, bytes{3, 4, 5}, 1024)
		mem.write(5, bytes{6}, 1024)
		delta := 0
		for _, da := range mem.areas {
			delta += mem.remove(area{off: da.off, len: len(da.data)})
		}
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas, got %d", len(mem.areas))
		}
		if delta != -6 {
			t.Errorf("expected memoryDelta -6, got %d", delta)
		}
	})
}
