package cache

import (
	"reflect"
	"testing"
)

func TestInsertAreas(t *testing.T) {
	t.Run("insert into empty slice", func(t *testing.T) {
		prev := areas{}
		got := insert(prev, 10, 5)
		want := areas{{off: 10, len: 5}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert before all", func(t *testing.T) {
		prev := areas{{off: 20, len: 5}}
		got := insert(prev, 10, 5)
		want := areas{{off: 10, len: 5}, {off: 20, len: 5}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert after all", func(t *testing.T) {
		prev := areas{{off: 10, len: 5}}
		got := insert(prev, 20, 5)
		want := areas{{off: 10, len: 5}, {off: 20, len: 5}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert between two non-touching", func(t *testing.T) {
		prev := areas{{off: 0, len: 5}, {off: 10, len: 5}}
		got := insert(prev, 6, 3)
		want := areas{{off: 0, len: 5}, {off: 6, len: 3}, {off: 10, len: 5}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert overlaps one", func(t *testing.T) {
		prev := areas{{off: 10, len: 5}}
		got := insert(prev, 12, 5)
		want := areas{{off: 10, len: 7}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert overlaps two", func(t *testing.T) {
		prev := areas{{off: 10, len: 5}, {off: 20, len: 5}}
		got := insert(prev, 12, 15)
		want := areas{{off: 10, len: 17}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert touches left", func(t *testing.T) {
		prev := areas{{off: 10, len: 5}}
		got := insert(prev, 5, 5)
		want := areas{{off: 5, len: 10}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert touches right", func(t *testing.T) {
		prev := areas{{off: 10, len: 5}}
		got := insert(prev, 15, 5)
		want := areas{{off: 10, len: 10}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert covers all", func(t *testing.T) {
		prev := areas{{off: 10, len: 5}, {off: 20, len: 5}}
		got := insert(prev, 5, 25)
		want := areas{{off: 5, len: 25}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})

	t.Run("insert inside existing", func(t *testing.T) {
		prev := areas{{off: 10, len: 10}}
		got := insert(prev, 12, 3)
		want := areas{{off: 10, len: 10}}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %v, got %v", want, got)
		}
	})
}
