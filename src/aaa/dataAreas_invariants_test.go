package aaa

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateDataAreasInvariants(t *testing.T) {
	t.Run("empty area should cause panic", func(t *testing.T) {
		require.Panics(t, func() {
			validateDataAreasInvariants(dataAreas{
				dataArea{position: 0, data: bytes{}},
			})
		})
	})

	t.Run("areas not ordered by position should cause panic", func(t *testing.T) {
		require.Panics(t, func() {
			validateDataAreasInvariants(dataAreas{
				dataArea{position: 18, data: bytes("data")},
				dataArea{position: 10, data: bytes("1234")},
			})
		})
	})

	t.Run("overlapping areas should cause panic", func(t *testing.T) {
		require.Panics(t, func() {
			validateDataAreasInvariants(dataAreas{
				dataArea{position: 8, data: bytes("data")},
				dataArea{position: 10, data: bytes("1234")},
			})
		})
	})

	t.Run("non-compact areas should cause panic", func(t *testing.T) {
		require.Panics(t, func() {
			data := make(bytes, 10, 20)
			validateDataAreasInvariants(dataAreas{
				dataArea{position: 8, data: data},
			})
		})
	})

	t.Run("well-formed areas should not cause panic", func(t *testing.T) {
		require.NotPanics(t, func() {
			validateDataAreasInvariants(dataAreas{
				dataArea{position: 8, data: bytes("data")},
				dataArea{position: 18, data: bytes("1234")},
				dataArea{position: 28, data: bytes("data")},
			})
		})
	})
}
