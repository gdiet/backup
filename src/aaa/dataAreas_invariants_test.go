package aaa

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDataAreasInvariants(t *testing.T) {
	require.Panics(t, func() {
		validateDataAreasInvariants(dataAreas{
			dataArea{position: 0, data: bytes{}},
		})
	}, "empty area should cause panic")

	require.Panics(t, func() {
		validateDataAreasInvariants(dataAreas{
			dataArea{position: 18, data: bytes("data")},
			dataArea{position: 10, data: bytes("1234")},
		})
	}, "areas not ordered by position should cause panic")

	require.Panics(t, func() {
		validateDataAreasInvariants(dataAreas{
			dataArea{position: 8, data: bytes("data")},
			dataArea{position: 10, data: bytes("1234")},
		})
	}, "overlapping areas should cause panic")

	require.Panics(t, func() {
		data := make(bytes, 10, 20)
		validateDataAreasInvariants(dataAreas{
			dataArea{position: 8, data: data},
		})
	}, "non-compact areas should cause panic")

	require.NotPanics(t, func() {
		validateDataAreasInvariants(dataAreas{
			dataArea{position: 8, data: bytes("data")},
			dataArea{position: 18, data: bytes("1234")},
			dataArea{position: 28, data: bytes("data")},
		})
	}, "well-formed areas should not cause panic")
}
