package cache

import (
	"reflect"
	"testing"
)

// TestSparseWrite tests the sparse.write method which adds sparse (zero-filled) areas.
//
// Note: sparse.write assumes that areas are written in ascending offset order
// (see function comment: "Assumes the area's offset is beyond the current sparse areas").
// This is the expected usage pattern in the cache system.

func TestSparseWrite(t *testing.T) {
	t.Run("write to empty sparse", func(t *testing.T) {
		s := &sparse{}
		s.write(10, 5)

		want := areas{{off: 10, len: 5}}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write zero length does nothing", func(t *testing.T) {
		s := &sparse{}
		s.write(10, 0)

		if len(s.areas) != 0 {
			t.Errorf("expected no areas, got %v", s.areas)
		}
	})

	t.Run("write adjacent to existing area (merge at end)", func(t *testing.T) {
		s := &sparse{areas: areas{{off: 10, len: 5}}}
		s.write(15, 3) // Write at end of existing area

		want := areas{{off: 10, len: 8}} // Should merge: 10-14 + 15-17 = 10-17
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write non-adjacent to existing area", func(t *testing.T) {
		s := &sparse{areas: areas{{off: 10, len: 5}}}
		s.write(20, 3) // Write with gap after existing area

		want := areas{
			{off: 10, len: 5},
			{off: 20, len: 3},
		}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write multiple non-adjacent areas", func(t *testing.T) {
		s := &sparse{}
		s.write(10, 5) // 10-14
		s.write(20, 3) // 20-22
		s.write(30, 2) // 30-31

		want := areas{
			{off: 10, len: 5},
			{off: 20, len: 3},
			{off: 30, len: 2},
		}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write adjacent areas that should merge", func(t *testing.T) {
		s := &sparse{}
		s.write(10, 5) // 10-14
		s.write(15, 3) // 15-17 (adjacent to previous)
		s.write(18, 2) // 18-19 (adjacent to previous)

		want := areas{{off: 10, len: 10}} // Should all merge into 10-19
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write with gap then adjacent", func(t *testing.T) {
		s := &sparse{}
		s.write(10, 5) // 10-14
		s.write(20, 3) // 20-22 (gap)
		s.write(23, 2) // 23-24 (adjacent to 20-22)

		want := areas{
			{off: 10, len: 5},
			{off: 20, len: 5}, // 20-22 + 23-24 merged to 20-24
		}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write single byte areas", func(t *testing.T) {
		s := &sparse{}
		s.write(5, 1) // 5
		s.write(6, 1) // 6 (adjacent)
		s.write(8, 1) // 8 (gap)
		s.write(9, 1) // 9 (adjacent to 8)

		want := areas{
			{off: 5, len: 2}, // 5-6 merged
			{off: 8, len: 2}, // 8-9 merged
		}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write large area", func(t *testing.T) {
		s := &sparse{}
		s.write(1000000, 5000000) // Large sparse area

		want := areas{{off: 1000000, len: 5000000}}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write maintains sorted order and invariants", func(t *testing.T) {
		s := &sparse{}

		// Write several areas that should remain sorted
		s.write(100, 10) // 100-109
		s.write(120, 5)  // 120-124
		s.write(130, 3)  // 130-132

		// Verify areas are sorted
		for i := 1; i < len(s.areas); i++ {
			if s.areas[i-1].off >= s.areas[i].off {
				t.Errorf("areas not sorted: area[%d].off=%d >= area[%d].off=%d",
					i-1, s.areas[i-1].off, i, s.areas[i].off)
			}
		}

		// Verify no overlaps
		for i := 1; i < len(s.areas); i++ {
			if s.areas[i-1].end() > s.areas[i].off {
				t.Errorf("areas overlap: area[%d] ends at %d but area[%d] starts at %d",
					i-1, s.areas[i-1].end(), i, s.areas[i].off)
			}
		}

		// Verify all areas have positive length
		for i, area := range s.areas {
			if area.len <= 0 {
				t.Errorf("area[%d] has non-positive length: %d", i, area.len)
			}
		}
	})

	t.Run("write at offset 0", func(t *testing.T) {
		s := &sparse{}
		s.write(0, 10)

		want := areas{{off: 0, len: 10}}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write extending from offset 0", func(t *testing.T) {
		s := &sparse{areas: areas{{off: 0, len: 5}}}
		s.write(5, 3) // Extend from 0-4 to 0-7

		want := areas{{off: 0, len: 8}}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})
}

func TestSparseWriteEdgeCases(t *testing.T) {
	t.Run("write maximum int64 values", func(t *testing.T) {
		s := &sparse{}

		// Test with very large offset but reasonable length
		const maxOffset = int64(1) << 62 // Large but not overflow-prone
		s.write(maxOffset, 1000)

		want := areas{{off: maxOffset, len: 1000}}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write with negative length should be prevented", func(t *testing.T) {
		s := &sparse{}

		s.write(10, -5) // Should be prevented by length <= 0 check

		if len(s.areas) != 0 {
			t.Errorf("expected no areas with negative length, got %v", s.areas)
		}

		// Also test zero length
		s.write(20, 0) // Should also be prevented

		if len(s.areas) != 0 {
			t.Errorf("expected no areas with zero length, got %v", s.areas)
		}
	})

	t.Run("multiple writes in ascending order", func(t *testing.T) {
		s := &sparse{}

		// Write in ascending order as the implementation expects
		// ("Assumes the area's offset is beyond the current sparse areas")
		s.write(10, 5) // 10-14
		s.write(20, 3) // 20-22 (gap, but offset > previous)
		s.write(30, 2) // 30-31 (gap, but offset > previous)

		want := areas{
			{off: 10, len: 5},
			{off: 20, len: 3},
			{off: 30, len: 2},
		}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v, got %v", want, s.areas)
		}
	})

	t.Run("write preserves invariants with ascending offsets", func(t *testing.T) {
		s := &sparse{}

		// Write in ascending order only, as implementation assumes
		testWrites := []struct {
			off, length int64
		}{
			{100, 10}, // 100-109
			{120, 5},  // 120-124 (gap)
			{125, 3},  // 125-127 (should merge with previous -> 120-127)
			{140, 2},  // 140-141 (gap)
			{150, 10}, // 150-159 (gap)
		}

		for i, tw := range testWrites {
			s.write(tw.off, tw.length)

			// After each write, verify invariants are maintained
			// This will panic if invariants are violated
			validateAreasInvariants(s.areas)

			t.Logf("After write %d (off=%d, len=%d): areas=%v", i, tw.off, tw.length, s.areas)
		}
	})
}
