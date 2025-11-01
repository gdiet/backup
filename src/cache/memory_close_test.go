package cache

import (
	"testing"
)

func TestMemoryClose(t *testing.T) {
	t.Run("close empty memory", func(t *testing.T) {
		mem := &memory{}
		memoryDelta := mem.close()
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas after close, got %d", len(mem.areas))
		}
		if memoryDelta != 0 {
			t.Errorf("expected memoryDelta to be 0, got %d", memoryDelta)
		}
	})

	t.Run("close with single area", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{off: 0, data: bytes{1, 2, 3}}}}
		memoryDelta := mem.close()
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas after close, got %d", len(mem.areas))
		}
		if memoryDelta != -3 {
			t.Errorf("expected memoryDelta to be -3, got %d", memoryDelta)
		}
	})

	t.Run("close with multiple areas", func(t *testing.T) {
		mem := &memory{areas: dataAreas{
			{off: 0, data: bytes{1, 2}},
			{off: 2, data: bytes{3, 4, 5}},
			{off: 5, data: bytes{6}},
		}}
		memoryDelta := mem.close()
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas after close, got %d", len(mem.areas))
		}
		if memoryDelta != -6 {
			t.Errorf("expected memoryDelta to be -6, got %d", memoryDelta)
		}
	})

	t.Run("close after write", func(t *testing.T) {
		mem := &memory{}
		mem.write(0, bytes{1, 2, 3, 4}, 1024)
		memoryDelta := mem.close()
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas after close, got %d", len(mem.areas))
		}
		if memoryDelta != -4 {
			t.Errorf("expected memoryDelta to be -4, got %d", memoryDelta)
		}
	})

	t.Run("close after truncate", func(t *testing.T) {
		mem := &memory{areas: dataAreas{{off: 0, data: bytes{1, 2, 3, 4, 5}}}}
		mem.shrink(3)
		memoryDelta := mem.close()
		if len(mem.areas) != 0 {
			t.Errorf("expected no areas after close, got %d", len(mem.areas))
		}
		if memoryDelta != -3 {
			t.Errorf("expected memoryDelta to be -3, got %d", memoryDelta)
		}
	})
}
