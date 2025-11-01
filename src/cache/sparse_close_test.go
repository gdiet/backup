package cache

import (
	"reflect"
	"testing"
)

const expectedAreasNilMsg = "expected areas to be nil after close, got %v"

func TestSparseCloseBasic(t *testing.T) {
	t.Run("close empty sparse", func(t *testing.T) {
		s := &sparse{}
		s.close()

		if s.areas != nil {
			t.Errorf(expectedAreasNilMsg, s.areas)
		}
	})

	t.Run("close sparse with areas", func(t *testing.T) {
		s := &sparse{areas: areas{{off: 10, len: 5}, {off: 20, len: 3}}}
		s.close()

		if s.areas != nil {
			t.Errorf(expectedAreasNilMsg, s.areas)
		}
	})

	t.Run("close sparse with single area", func(t *testing.T) {
		s := &sparse{areas: areas{{off: 0, len: 100}}}
		s.close()

		if s.areas != nil {
			t.Errorf(expectedAreasNilMsg, s.areas)
		}
	})
}

func TestSparseCloseLargeAreas(t *testing.T) {
	t.Run("close sparse with large areas", func(t *testing.T) {
		// Create sparse with many areas
		s := &sparse{}
		for i := int64(0); i < 1000; i += 10 {
			s.write(i, 5) // Write areas at offsets 0, 10, 20, etc.
		}

		// Verify areas were created
		if len(s.areas) == 0 {
			t.Fatal("expected areas to be created before close")
		}

		s.close()

		if s.areas != nil {
			t.Errorf(expectedAreasNilMsg, s.areas)
		}
	})
}

func TestSparseCloseIdempotent(t *testing.T) {
	t.Run("close is idempotent", func(t *testing.T) {
		s := &sparse{areas: areas{{off: 10, len: 5}}}

		// First close
		s.close()
		if s.areas != nil {
			t.Errorf("expected areas to be nil after first close, got %v", s.areas)
		}

		// Second close should be safe
		s.close()
		if s.areas != nil {
			t.Errorf("expected areas to still be nil after second close, got %v", s.areas)
		}
	})
}

func TestSparseCloseAndReuse(t *testing.T) {
	t.Run("sparse can be used after close", func(t *testing.T) {
		s := &sparse{areas: areas{{off: 10, len: 5}}}
		s.close()

		// Should be able to write to sparse after close
		s.write(20, 3)
		want := areas{{off: 20, len: 3}}
		if !reflect.DeepEqual(s.areas, want) {
			t.Errorf("expected areas %v after write following close, got %v", want, s.areas)
		}
	})
}
