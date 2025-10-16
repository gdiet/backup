package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartialOverlapTopBeforeBottom1(t *testing.T) {
	top := dataArea{position: 0, data: bytes("hello")}
	bottom := dataArea{position: 3, data: bytes("world")}
	expected := dataAreas{dataArea{position: 0, data: bytes("hello")}, dataArea{position: 5, data: bytes("rld")}}
	result := removeOverlaps(top, bottom)
	assertDataAreasEqual(t, expected, result)
}

func TestPartialOverlapTopBeforeBottom2(t *testing.T) {
	top := dataArea{position: 0, data: bytes("1234")}
	bottom := dataArea{position: 2, data: bytes("abcdefg")}
	expected := dataAreas{dataArea{position: 0, data: bytes("1234")}, dataArea{position: 4, data: bytes("cdefg")}}
	result := removeOverlaps(top, bottom)
	assertDataAreasEqual(t, expected, result)
}

func TestTopFullyCoversBottom1(t *testing.T) {
	top := dataArea{position: 0, data: bytes("1234")}
	bottom := dataArea{position: 0, data: bytes("abcd")}
	expected := dataAreas{dataArea{position: 0, data: bytes("1234")}}
	result := removeOverlaps(top, bottom)
	assertDataAreasEqual(t, expected, result)
}

func TestTopFullyCoversBottom2(t *testing.T) {
	top := dataArea{position: 0, data: bytes("1234")}
	bottom := dataArea{position: 1, data: bytes("ab")}
	expected := dataAreas{dataArea{position: 0, data: bytes("1234")}}
	result := removeOverlaps(top, bottom)
	assertDataAreasEqual(t, expected, result)
}

func TestPartialOverlapBottomBeforeStart1(t *testing.T) {
	top := dataArea{position: 3, data: bytes("hello")}
	bottom := dataArea{position: 0, data: bytes("world")}
	expected := dataAreas{dataArea{position: 0, data: bytes("wor")}, dataArea{position: 3, data: bytes("hello")}}
	result := removeOverlaps(top, bottom)
	assertDataAreasEqual(t, expected, result)
}

func TestPartialOverlapBottomBeforeStart2(t *testing.T) {
	top := dataArea{position: 2, data: bytes("1234")}
	bottom := dataArea{position: 0, data: bytes("abcdef")}
	expected := dataAreas{dataArea{position: 0, data: bytes("ab")}, dataArea{position: 2, data: bytes("1234")}}
	result := removeOverlaps(top, bottom)
	assertDataAreasEqual(t, expected, result)
}

func TestTopIsFullyContainedInBottom(t *testing.T) {
	top := dataArea{position: 1, data: bytes("12")}
	bottom := dataArea{position: 0, data: bytes("abcd")}
	expected := dataAreas{
		dataArea{position: 0, data: bytes("a")},
		dataArea{position: 1, data: bytes("12")},
		dataArea{position: 3, data: bytes("d")}}
	result := removeOverlaps(top, bottom)
	assertDataAreasEqual(t, expected, result)
}

func TestNoOverlapPanicTopBeforeBottom(t *testing.T) {
	top := dataArea{position: 1, data: bytes("12")}
	bottom := dataArea{position: 10, data: bytes("abcd")}
	require.Panics(t, func() { removeOverlaps(top, bottom) })
}

func TestNoOverlapPanicTopAfterBottom(t *testing.T) {
	top := dataArea{position: 10, data: bytes("12")}
	bottom := dataArea{position: 1, data: bytes("abcd")}
	require.Panics(t, func() { removeOverlaps(top, bottom) })
}

func assertDataAreasEqual(t *testing.T, expected, actual dataAreas) {
	if len(expected) != len(actual) {
		t.Errorf("Expected %d areas, got %d", len(expected), len(actual))
		return
	}
	for i := range expected {
		if expected[i].position != actual[i].position {
			t.Errorf("Expected position %d, got %d", expected[i].position, actual[i].position)
		}
		if string(expected[i].data) != string(actual[i].data) {
			t.Errorf("Expected data %q, got %q", expected[i].data, actual[i].data)
		}
	}
}
